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

use crate::network_state::{self, NetworkState};
use crate::protocol::{
    self, CurrentBatch, NetworkStatus, RecentBatch, Request, Response, StatusReport, DRV_PATH_ENV,
    OUT_PATHS_ENV,
};
use crate::socket_activation;

/// Cap on rolling-window history of completed batches. Belt-and-braces
/// alongside the time-based cutoff in case a worker burns through batches
/// fast enough that the 30-min window holds more than this.
const ROLLING_MAX_ENTRIES: usize = 200;
/// Drop completed-batch entries older than this from the rolling window.
const ROLLING_MAX_AGE: Duration = Duration::from_secs(30 * 60);

pub struct DaemonOpts {
    pub hook: PathBuf,
    pub socket: PathBuf,
    pub concurrency: usize,
    pub retries: u32,
    pub retry_interval: Duration,
    pub pause_on_metered: bool,
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
    /// `Some` when --pause-on-metered is on. Workers park here whenever the
    /// network is metered; populated and updated by the NM D-Bus watcher.
    network: Option<Arc<NetworkState>>,
    metrics: Mutex<MetricsState>,
}

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

fn shutting_down() -> bool {
    SHUTDOWN.load(Ordering::Acquire)
}

pub fn run(opts: DaemonOpts) -> Result<(), String> {
    install_signal_handler();

    let listener = match socket_activation::try_systemd_socket() {
        Some(l) => l,
        None => {
            let _ = std::fs::remove_file(&opts.socket);
            UnixListener::bind(&opts.socket)
                .map_err(|e| format!("bind {}: {e}", opts.socket.display()))?
        }
    };
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("set_nonblocking: {e}"))?;

    let network = if opts.pause_on_metered {
        Some(network_state::spawn_watcher())
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
        network,
        metrics: Mutex::new(MetricsState::default()),
    });

    for i in 0..opts.concurrency.max(1) {
        let s = Arc::clone(&state);
        thread::Builder::new()
            .name(format!("worker-{i}"))
            .spawn(move || worker_loop(s))
            .map_err(|e| format!("spawn worker-{i}: {e}"))?;
    }

    info!(
        hook = %state.hook.display(),
        concurrency = opts.concurrency.max(1),
        "daemon ready"
    );

    while !shutting_down() {
        match listener.accept() {
            Ok((stream, _)) => {
                let s = Arc::clone(&state);
                thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, s) {
                        warn!(error = %e, "connection failed");
                    }
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(e) => warn!(error = %e, "accept failed"),
        }
    }

    // Wake idle workers so they observe SHUTDOWN and exit.
    state.queue_cv.notify_all();
    if let Some(ref ns) = state.network {
        ns.notify_all();
    }
    info!(
        processed = state.processed_total.load(Ordering::Acquire),
        failed = state.failed_total.load(Ordering::Acquire),
        "shutdown"
    );
    Ok(())
}

fn handle_connection(stream: UnixStream, state: Arc<State>) -> Result<(), String> {
    let mut reader = BufReader::new(stream.try_clone().map_err(|e| e.to_string())?);
    let mut writer = stream;
    let resp = match protocol::read_line::<_, Request>(&mut reader) {
        Ok(req) => process(req, &state),
        Err(e) => Response::Error {
            message: format!("decode: {e}"),
        },
    };
    protocol::write_line(&mut writer, &resp)
}

fn process(req: Request, state: &State) -> Response {
    match req {
        Request::Enqueue {
            drv_path,
            out_paths,
        } => {
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
            })
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

        // Park the pulled job until the network is unmetered. The job stays
        // owned by this worker — nothing is lost — and other workers continue
        // pulling from the queue (and parking themselves) in parallel.
        if let Some(ref ns) = state.network {
            ns.wait_while_metered(shutting_down);
        }
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
        let ok = run_hook(&state, &job);
        state.in_flight.fetch_sub(1, Ordering::AcqRel);
        let duration = started.elapsed();

        {
            let mut m = state.metrics.lock().unwrap();
            m.current = None;
            if ok {
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

        let counter = if ok {
            &state.processed_total
        } else {
            &state.failed_total
        };
        counter.fetch_add(1, Ordering::AcqRel);
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

fn run_hook(state: &State, job: &Job) -> bool {
    use std::process::Command;
    for attempt in 1..=state.retries {
        if shutting_down() {
            return false;
        }
        let mut cmd = Command::new(&state.hook);
        if let Some(p) = &job.drv_path {
            cmd.env(DRV_PATH_ENV, p);
        }
        cmd.env(OUT_PATHS_ENV, job.out_paths.join(" "));

        match cmd.status() {
            Ok(s) if s.success() => return true,
            Ok(s) => warn!(
                exit = s.code().unwrap_or(-1),
                attempt, "hook returned non-zero"
            ),
            Err(e) => error!(error = %e, attempt, "hook spawn failed"),
        }
        if attempt < state.retries {
            thread::sleep(state.retry_interval);
        }
    }
    error!(
        out_paths = job.out_paths.len(),
        "dropping job after retries"
    );
    false
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
        );
        assert!(matches!(resp, Response::Ack));
        assert_eq!(state.queue.lock().unwrap().len(), 1);
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
        );
        assert!(matches!(resp, Response::Error { .. }));
        assert_eq!(state.queue.lock().unwrap().len(), 0);
    }

    #[test]
    fn status_returns_zero_state_for_fresh_daemon() {
        let state = test_state();
        let resp = process(Request::Status, &state);
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
            }
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
        match process(Request::Status, &state) {
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
