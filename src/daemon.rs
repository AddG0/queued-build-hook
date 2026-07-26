// Ack-on-receipt: the daemon ack's the client AS SOON AS the message is
// appended to the queue, so build-side hooks never wait on worker progress.
//
// Shutdown: SIGTERM/SIGINT flips a static AtomicBool. The accept loop polls
// it (listener is non-blocking, 100 ms idle sleep). Workers check it at the
// top of each loop iteration. systemd's TimeoutStopSec is the upper bound on
// the wait for in-flight hooks to finish; KillMode=mixed (recommended) tears
// down the whole cgroup once the daemon exits.

use std::collections::VecDeque;
use std::io::BufReader;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tracing::{debug, error, info, warn};

use crate::gate::{Gate, REASON_MANUAL};
use crate::network_state::{self, NetworkState};
use crate::protocol::{
    self, CurrentBatch, NetworkStatus, RecentBatch, Request, Response, StatusReport, DRV_PATH_ENV,
    OUT_PATHS_ENV,
};
use crate::socket_activation;

/// How often `run_hook` polls its child while watching for a pause/shutdown so
/// it can cancel an in-flight upload. Bounds cancel latency.
const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Grace period between SIGTERM and SIGKILL when cancelling an upload.
const TERM_GRACE: Duration = Duration::from_secs(2);

/// Cap on rolling-window history of completed batches. Belt-and-braces
/// alongside the time-based cutoff in case a worker burns through batches
/// fast enough that the 30-min window holds more than this.
const ROLLING_MAX_ENTRIES: usize = 200;
/// Drop completed-batch entries older than this from the rolling window.
const ROLLING_MAX_AGE: Duration = Duration::from_secs(30 * 60);

pub struct DaemonOpts {
    pub hook: PathBuf,
    pub socket: PathBuf,
    /// Optional second socket carrying only control commands (pause/resume/
    /// status). Split out so it can be group-owned separately from the enqueue
    /// socket — callers who may pause need not be able to enqueue. `None`
    /// disables the control socket (control still works on the main socket).
    pub control_socket: Option<PathBuf>,
    pub concurrency: usize,
    pub retries: u32,
    pub retry_interval: Duration,
    pub pause_on_metered: bool,
}

/// Outcome of a single batch's hook run.
enum HookOutcome {
    /// Hook exited 0.
    Success,
    /// Hook failed every attempt — count it and drop the batch.
    Failed,
    /// Cancelled mid-flight (pause asserted or shutdown). Requeue, don't count.
    Cancelled,
}

struct Job {
    drv_path: Option<String>,
    out_paths: Vec<String>,
}

struct RunningBatch {
    drv_path: Option<String>,
    paths_count: usize,
    closure_bytes: Option<u64>,
    started: Instant,
}

#[derive(Clone)]
struct CompletedBatch {
    drv_path: Option<String>,
    paths_count: usize,
    closure_bytes: Option<u64>,
    duration: Duration,
    completed: Instant,
}

#[derive(Default)]
struct MetricsState {
    /// Rolling window of recent successful completions. Pruned by
    /// ROLLING_MAX_AGE / ROLLING_MAX_ENTRIES on every push.
    rolling: VecDeque<CompletedBatch>,
    current: Option<RunningBatch>,
    last_completed: Option<CompletedBatch>,
}

struct State {
    queue: Mutex<VecDeque<Job>>,
    queue_cv: Condvar,
    in_flight: AtomicUsize,
    processed_total: AtomicU64,
    failed_total: AtomicU64,
    started: Instant,
    hook: PathBuf,
    retries: u32,
    retry_interval: Duration,
    /// Unified park gate. Fed by the manual pause/resume commands and (when
    /// `--pause-on-metered` is on) the NM watcher. Workers park here before
    /// running a hook and cancel an in-flight hook when it becomes blocked.
    gate: Arc<Gate>,
    /// `Some` when --pause-on-metered is on. Status-only NM view; parking is
    /// driven by `gate`.
    network: Option<Arc<NetworkState>>,
    metrics: Mutex<MetricsState>,
}

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

fn shutting_down() -> bool {
    SHUTDOWN.load(Ordering::Acquire)
}

pub fn run(opts: DaemonOpts) -> Result<(), String> {
    install_signal_handler();

    let listeners = build_listeners(&opts)?;
    for (l, _) in &listeners {
        l.set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking: {e}"))?;
    }

    let gate = Arc::new(Gate::new());
    let network = if opts.pause_on_metered {
        Some(network_state::spawn_watcher(Arc::clone(&gate)))
    } else {
        None
    };

    let state = Arc::new(State {
        queue: Mutex::new(VecDeque::new()),
        queue_cv: Condvar::new(),
        in_flight: AtomicUsize::new(0),
        processed_total: AtomicU64::new(0),
        failed_total: AtomicU64::new(0),
        started: Instant::now(),
        hook: opts.hook,
        retries: opts.retries.max(1),
        retry_interval: opts.retry_interval,
        gate,
        network,
        metrics: Mutex::new(MetricsState::default()),
    });

    let concurrency = opts.concurrency.max(1);
    for i in 0..concurrency {
        let s = Arc::clone(&state);
        thread::Builder::new()
            .name(format!("worker-{i}"))
            .spawn(move || worker_loop(s))
            .map_err(|e| format!("spawn worker-{i}: {e}"))?;
    }

    info!(
        hook = %state.hook.display(),
        concurrency,
        listeners = listeners.len(),
        "daemon ready"
    );

    // Poll every listener each pass; only sleep when all would-block, so idle
    // CPU stays flat regardless of how many sockets we serve.
    while !shutting_down() {
        let mut served = false;
        for (listener, kind) in &listeners {
            match listener.accept() {
                Ok((stream, _)) => {
                    served = true;
                    let s = Arc::clone(&state);
                    let kind = *kind;
                    thread::spawn(move || {
                        if let Err(e) = handle_connection(stream, s, kind) {
                            warn!(error = %e, "connection failed");
                        }
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => warn!(error = %e, "accept failed"),
            }
        }
        if !served {
            thread::sleep(Duration::from_millis(100));
        }
    }

    // Wake idle/parked workers so they observe SHUTDOWN and exit.
    state.queue_cv.notify_all();
    state.gate.notify_all();
    info!(
        processed = state.processed_total.load(Ordering::Acquire),
        failed = state.failed_total.load(Ordering::Acquire),
        "shutdown"
    );
    Ok(())
}

/// What a listener is allowed to carry. The permission split between the
/// enqueue and control sockets is a *capability* boundary, not just a filesystem
/// one: the control socket is scoped to a lower-privilege principal, so the
/// daemon must also refuse `Enqueue` there — otherwise anyone who can pause
/// could also push arbitrary paths (and trigger a signed `nix copy`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum SocketKind {
    /// Enqueue socket — every request type.
    Full,
    /// Control socket — status/pause/resume only; `Enqueue` is rejected.
    ControlOnly,
}

/// Resolve the listening sockets, each tagged with its capability. Prefers
/// systemd socket activation (named `enqueue` / `control` fds, so systemd owns
/// their permissions); otherwise binds them directly from the configured paths
/// (dev / non-systemd).
fn build_listeners(opts: &DaemonOpts) -> Result<Vec<(UnixListener, SocketKind)>, String> {
    let mut activated = socket_activation::systemd_listeners();
    let mut listeners = Vec::new();

    // Claim the control fd first so a lone activated fd can't be mis-adopted as
    // the enqueue socket by the single-fd fallback below.
    let control = activated.remove("control");

    let enqueue = match activated
        .remove("enqueue")
        .or_else(|| take_single(&mut activated))
    {
        Some(l) => l,
        None => bind_unix(&opts.socket)?,
    };
    listeners.push((enqueue, SocketKind::Full));

    if let Some(l) = control {
        listeners.push((l, SocketKind::ControlOnly));
    } else if let Some(path) = &opts.control_socket {
        listeners.push((bind_unix(path)?, SocketKind::ControlOnly));
    }

    // Any activated fd we couldn't place would silently never be served (and the
    // connection that activated us would hang). Fail loud instead.
    if !activated.is_empty() {
        let mut names: Vec<&String> = activated.keys().collect();
        names.sort();
        return Err(format!("unrecognized socket-activation fds: {names:?}"));
    }

    Ok(listeners)
}

/// Bind a fresh Unix listener at `path`, clearing any stale socket file first.
fn bind_unix(path: &std::path::Path) -> Result<UnixListener, String> {
    let _ = std::fs::remove_file(path);
    UnixListener::bind(path).map_err(|e| format!("bind {}: {e}", path.display()))
}

/// Pull the one remaining activated listener when exactly one is left and it
/// wasn't matched by name — the legacy single-socket unit case.
fn take_single(map: &mut std::collections::HashMap<String, UnixListener>) -> Option<UnixListener> {
    if map.len() == 1 {
        let key = map.keys().next().cloned()?;
        map.remove(&key)
    } else {
        None
    }
}

fn handle_connection(
    stream: UnixStream,
    state: Arc<State>,
    kind: SocketKind,
) -> Result<(), String> {
    let mut reader = BufReader::new(stream.try_clone().map_err(|e| e.to_string())?);
    let mut writer = stream;
    let resp = match protocol::read_line::<_, Request>(&mut reader) {
        Ok(req) => process(req, &state, kind),
        Err(e) => Response::Error {
            message: format!("decode: {e}"),
        },
    };
    protocol::write_line(&mut writer, &resp)
}

fn process(req: Request, state: &State, kind: SocketKind) -> Response {
    match req {
        Request::Enqueue {
            drv_path,
            out_paths,
        } => {
            // The control socket is reachable by lower-privilege callers; enqueue
            // (which runs a signed `nix copy`) must stay on the enqueue socket.
            if kind == SocketKind::ControlOnly {
                return Response::Error {
                    message: "enqueue not permitted on the control socket".into(),
                };
            }
            if out_paths.is_empty() {
                return Response::Error {
                    message: "out_paths must not be empty".into(),
                };
            }
            let path_count = out_paths.len();
            let depth = {
                let mut q = state.queue.lock().unwrap();
                q.push_back(Job {
                    drv_path,
                    out_paths,
                });
                q.len()
            };
            state.queue_cv.notify_one();
            debug!(paths = path_count, queue_depth = depth, "enqueued");
            Response::Ack
        }
        Request::Status => {
            let metrics = state.metrics.lock().unwrap();
            let now = Instant::now();
            let avg_batch_seconds = if metrics.rolling.is_empty() {
                None
            } else {
                let total_secs: f64 = metrics
                    .rolling
                    .iter()
                    .map(|c| c.duration.as_secs_f64())
                    .sum();
                Some(total_secs / metrics.rolling.len() as f64)
            };
            let current_batch = metrics.current.as_ref().map(|r| CurrentBatch {
                drv_path: r.drv_path.clone(),
                paths_count: r.paths_count,
                closure_bytes: r.closure_bytes,
                started_seconds_ago: now.saturating_duration_since(r.started).as_secs(),
            });
            let last_completed = metrics.last_completed.as_ref().map(|c| RecentBatch {
                drv_path: c.drv_path.clone(),
                paths_count: c.paths_count,
                closure_bytes: c.closure_bytes,
                duration_seconds: c.duration.as_secs(),
                completed_seconds_ago: now.saturating_duration_since(c.completed).as_secs(),
            });
            drop(metrics);

            Response::Status(StatusReport {
                queue_depth: state.queue.lock().unwrap().len(),
                in_flight: state.in_flight.load(Ordering::Acquire),
                processed_total: state.processed_total.load(Ordering::Acquire),
                failed_total: state.failed_total.load(Ordering::Acquire),
                uptime_seconds: state.started.elapsed().as_secs(),
                avg_batch_seconds,
                current_batch,
                last_completed,
                network: state.network.as_ref().map(|n| NetworkStatus {
                    nm_available: n.is_available(),
                    metered: n.is_metered(),
                }),
                paused: state.gate.has(REASON_MANUAL),
            })
        }
        Request::Pause => {
            state.gate.set(REASON_MANUAL, true);
            info!("paused by control command — in-flight uploads will cancel and requeue");
            Response::Ack
        }
        Request::Resume => {
            state.gate.set(REASON_MANUAL, false);
            info!("resumed by control command");
            Response::Ack
        }
    }
}

fn worker_loop(state: Arc<State>) {
    loop {
        let job = {
            let mut q = state.queue.lock().unwrap();
            while q.is_empty() {
                if shutting_down() {
                    return;
                }
                q = state.queue_cv.wait(q).unwrap();
            }
            q.pop_front().unwrap()
        };

        // Park the pulled job while the gate is blocked (metered and/or a
        // manual pause). The job stays owned by this worker — nothing is lost —
        // and other workers keep pulling (and parking) in parallel.
        state.gate.wait_while_blocked(shutting_down);
        if shutting_down() {
            return;
        }

        // Closure size: shells out to `nix path-info`. None on failure (no
        // guess written to the status report — caller sees "unknown").
        let closure_bytes = compute_closure_bytes(&job.out_paths);
        let started = Instant::now();

        {
            let mut m = state.metrics.lock().unwrap();
            m.current = Some(RunningBatch {
                drv_path: job.drv_path.clone(),
                paths_count: job.out_paths.len(),
                closure_bytes,
                started,
            });
        }

        state.in_flight.fetch_add(1, Ordering::AcqRel);
        let outcome = run_hook(&state, &job);
        state.in_flight.fetch_sub(1, Ordering::AcqRel);
        let duration = started.elapsed();

        {
            let mut m = state.metrics.lock().unwrap();
            m.current = None;
            if let HookOutcome::Success = outcome {
                let entry = CompletedBatch {
                    drv_path: job.drv_path.clone(),
                    paths_count: job.out_paths.len(),
                    closure_bytes,
                    duration,
                    completed: Instant::now(),
                };
                m.rolling.push_back(entry.clone());
                let cutoff = Instant::now() - ROLLING_MAX_AGE;
                while m.rolling.front().is_some_and(|c| c.completed < cutoff) {
                    m.rolling.pop_front();
                }
                while m.rolling.len() > ROLLING_MAX_ENTRIES {
                    m.rolling.pop_front();
                }
                m.last_completed = Some(entry);
            }
        }

        match outcome {
            HookOutcome::Success => {
                state.processed_total.fetch_add(1, Ordering::AcqRel);
            }
            HookOutcome::Failed => {
                state.failed_total.fetch_add(1, Ordering::AcqRel);
            }
            HookOutcome::Cancelled => {
                // Paused (or shutting down) mid-upload. Don't count it either
                // way; requeue at the FRONT so it's retried first once the gate
                // clears. On shutdown the in-memory queue is discarded anyway.
                if !shutting_down() {
                    state.queue.lock().unwrap().push_front(job);
                    debug!("upload cancelled by pause — job requeued");
                }
            }
        }
    }
}

/// Sum of NAR sizes for the unique closure of `out_paths`. Shells out to
/// `nix path-info --size --recursive` and streams stdout line by line —
/// constant memory regardless of closure size, so even a system aggregator
/// with 60k paths costs a few KB. Returns `None` on any failure (binary
/// absent, path GC'd between enqueue and pull, parse error); the daemon
/// stays useful without metrics rather than producing fake numbers.
fn compute_closure_bytes(out_paths: &[String]) -> Option<u64> {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};

    let mut child = Command::new("nix")
        .arg("path-info")
        .arg("--size")
        .arg("--recursive")
        .args(out_paths)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| {
            warn!(error = %e, "nix path-info spawn failed");
        })
        .ok()?;

    // Output format is "<path>\t<narSize>\n" (or whitespace-separated).
    // Sum the size column without holding the whole transcript in memory.
    let stdout = child.stdout.take()?;
    let mut total: u64 = 0;
    for line in BufReader::new(stdout).lines().map_while(Result::ok) {
        if let Some(size) = line.split_whitespace().nth(1).and_then(|s| s.parse().ok()) {
            total = total.saturating_add(size);
        }
    }

    let status = child.wait().ok()?;
    if !status.success() {
        warn!(
            exit = status.code().unwrap_or(-1),
            "nix path-info exited non-zero"
        );
        return None;
    }
    Some(total)
}

fn run_hook(state: &State, job: &Job) -> HookOutcome {
    use std::os::unix::process::CommandExt;
    use std::process::Command;
    let out_paths = job.out_paths.join(" ");
    for attempt in 1..=state.retries {
        if should_cancel(state) {
            return HookOutcome::Cancelled;
        }
        let mut cmd = Command::new(&state.hook);
        if let Some(p) = &job.drv_path {
            cmd.env(DRV_PATH_ENV, p);
        }
        cmd.env(OUT_PATHS_ENV, &out_paths);
        // Put the hook in its own process group so cancelling can signal the
        // whole tree (`nix copy` and any helpers), not just the wrapper script.
        unsafe {
            cmd.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        match cmd.spawn() {
            // Wait for the child, polling so a pause/shutdown mid-upload can
            // cancel it promptly (latency bounded by CANCEL_POLL_INTERVAL).
            Ok(mut child) => loop {
                match child.try_wait() {
                    Ok(Some(s)) if s.success() => return HookOutcome::Success,
                    Ok(Some(s)) => {
                        warn!(
                            exit = s.code().unwrap_or(-1),
                            attempt, "hook returned non-zero"
                        );
                        break;
                    }
                    Ok(None) => {
                        if should_cancel(state) {
                            terminate(&mut child);
                            return HookOutcome::Cancelled;
                        }
                        thread::sleep(CANCEL_POLL_INTERVAL);
                    }
                    Err(e) => {
                        error!(error = %e, attempt, "hook wait failed");
                        terminate(&mut child);
                        break;
                    }
                }
            },
            Err(e) => error!(error = %e, attempt, "hook spawn failed"),
        }

        // Both a spawn failure and a non-zero / errored exit fall through here.
        if attempt < state.retries && !wait_retry(state) {
            return HookOutcome::Cancelled;
        }
    }
    error!(
        out_paths = job.out_paths.len(),
        "dropping job after retries"
    );
    HookOutcome::Failed
}

/// True when an in-flight or about-to-start hook should abort: the gate is
/// blocked (paused / metered) or the daemon is shutting down.
fn should_cancel(state: &State) -> bool {
    shutting_down() || state.gate.blocked()
}

/// Sleep the retry backoff, but bail early (returning false) if a cancel
/// condition arises so a paused daemon doesn't sit in a backoff sleep.
fn wait_retry(state: &State) -> bool {
    let deadline = Instant::now() + state.retry_interval;
    while Instant::now() < deadline {
        if should_cancel(state) {
            return false;
        }
        thread::sleep(CANCEL_POLL_INTERVAL.min(state.retry_interval));
    }
    true
}

/// Terminate a hook's process group: SIGTERM, a short grace, then SIGKILL.
/// Signals the negative pgid (set via `setpgid` in `pre_exec`) so the whole
/// tree dies, not just the wrapper. `nix copy` handles SIGTERM by aborting the
/// upload cleanly; the requeued job re-copies later.
fn terminate(child: &mut std::process::Child) {
    let pgid = child.id() as libc::pid_t;
    unsafe {
        libc::kill(-pgid, libc::SIGTERM);
    }
    let deadline = Instant::now() + TERM_GRACE;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => thread::sleep(CANCEL_POLL_INTERVAL),
            Err(_) => break,
        }
    }
    unsafe {
        libc::kill(-pgid, libc::SIGKILL);
    }
    let _ = child.wait();
}

fn install_signal_handler() {
    extern "C" fn handler(_: libc::c_int) {
        SHUTDOWN.store(true, Ordering::Release);
    }
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        // fn-item → raw pointer → integer. The direct fn-as-usize cast is
        // denied by modern clippy (function_casts_as_integer).
        sa.sa_sigaction = handler as *const () as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        sa.sa_flags = libc::SA_RESTART;
        libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());
        libc::sigaction(libc::SIGINT, &sa, std::ptr::null_mut());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state() -> Arc<State> {
        Arc::new(State {
            queue: Mutex::new(VecDeque::new()),
            queue_cv: Condvar::new(),
            in_flight: AtomicUsize::new(0),
            processed_total: AtomicU64::new(0),
            failed_total: AtomicU64::new(0),
            started: Instant::now(),
            hook: PathBuf::from("/bin/true"),
            retries: 1,
            retry_interval: Duration::from_secs(0),
            gate: Arc::new(Gate::new()),
            network: None,
            metrics: Mutex::new(MetricsState::default()),
        })
    }

    #[test]
    fn enqueue_pushes_and_acks() {
        let state = test_state();
        let resp = process(
            Request::Enqueue {
                drv_path: None,
                out_paths: vec!["/nix/store/abc".into()],
            },
            &state,
            SocketKind::Full,
        );
        assert!(matches!(resp, Response::Ack));
        assert_eq!(state.queue.lock().unwrap().len(), 1);
    }

    #[test]
    fn enqueue_is_rejected_on_the_control_socket() {
        let state = test_state();
        let resp = process(
            Request::Enqueue {
                drv_path: None,
                out_paths: vec!["/nix/store/abc".into()],
            },
            &state,
            SocketKind::ControlOnly,
        );
        assert!(matches!(resp, Response::Error { .. }));
        assert_eq!(state.queue.lock().unwrap().len(), 0);
    }

    #[test]
    fn enqueue_with_empty_paths_returns_error() {
        let state = test_state();
        let resp = process(
            Request::Enqueue {
                drv_path: None,
                out_paths: vec![],
            },
            &state,
            SocketKind::Full,
        );
        assert!(matches!(resp, Response::Error { .. }));
        assert_eq!(state.queue.lock().unwrap().len(), 0);
    }

    #[test]
    fn status_returns_zero_state_for_fresh_daemon() {
        let state = test_state();
        let resp = process(Request::Status, &state, SocketKind::Full);
        match resp {
            Response::Status(s) => {
                assert_eq!(s.queue_depth, 0);
                assert_eq!(s.in_flight, 0);
                assert_eq!(s.processed_total, 0);
                assert_eq!(s.failed_total, 0);
                // No batches yet → fields stay None rather than guess values.
                assert!(s.avg_batch_seconds.is_none());
                assert!(s.current_batch.is_none());
                assert!(s.last_completed.is_none());
                assert!(!s.paused);
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }

    #[test]
    fn pause_and_resume_toggle_manual_gate_and_status() {
        let state = test_state();
        assert!(!state.gate.blocked());

        assert!(matches!(
            process(Request::Pause, &state, SocketKind::ControlOnly),
            Response::Ack
        ));
        assert!(state.gate.has(REASON_MANUAL));
        match process(Request::Status, &state, SocketKind::ControlOnly) {
            Response::Status(s) => assert!(s.paused),
            other => panic!("expected Status, got {other:?}"),
        }

        // Idempotent: a second pause stays paused.
        assert!(matches!(
            process(Request::Pause, &state, SocketKind::ControlOnly),
            Response::Ack
        ));
        assert!(state.gate.has(REASON_MANUAL));

        assert!(matches!(
            process(Request::Resume, &state, SocketKind::ControlOnly),
            Response::Ack
        ));
        assert!(!state.gate.blocked());
        match process(Request::Status, &state, SocketKind::ControlOnly) {
            Response::Status(s) => assert!(!s.paused),
            other => panic!("expected Status, got {other:?}"),
        }
    }

    #[test]
    fn status_reflects_recent_batches_after_completions() {
        let state = test_state();
        // Seed one running and one completed batch directly.
        {
            let mut m = state.metrics.lock().unwrap();
            m.current = Some(RunningBatch {
                drv_path: Some("/nix/store/cur.drv".into()),
                paths_count: 2,
                closure_bytes: Some(4096),
                started: Instant::now(),
            });
            let done = CompletedBatch {
                drv_path: Some("/nix/store/done.drv".into()),
                paths_count: 1,
                closure_bytes: Some(2048),
                duration: Duration::from_secs(7),
                completed: Instant::now(),
            };
            m.rolling.push_back(done.clone());
            m.last_completed = Some(done);
        }
        match process(Request::Status, &state, SocketKind::Full) {
            Response::Status(s) => {
                assert_eq!(s.avg_batch_seconds, Some(7.0));
                let cur = s.current_batch.expect("current_batch present");
                assert_eq!(cur.paths_count, 2);
                assert_eq!(cur.closure_bytes, Some(4096));
                let last = s.last_completed.expect("last_completed present");
                assert_eq!(last.duration_seconds, 7);
                assert_eq!(last.closure_bytes, Some(2048));
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }
}
