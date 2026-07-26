// End-to-end: a manual pause cancels an in-flight upload and requeues it
// (not counted as a failure), and resume re-runs it to completion.
//
// Uses a fake "upload" hook that logs START, traps SIGTERM (logging
// TERMINATED), and otherwise runs long enough to still be in flight when we
// pause. Talks to the daemon binary over its sockets exactly as a real client
// would.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread::sleep;
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_queued-build-hook");

fn tmpdir() -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("qbh-it-{}", std::process::id()));
    let _ = fs::remove_dir_all(&p);
    fs::create_dir_all(&p).unwrap();
    p
}

/// Send one JSON request line, return the response line.
fn call(sock: &Path, req: &str) -> String {
    let mut stream = UnixStream::connect(sock).expect("connect");
    stream.write_all(req.as_bytes()).unwrap();
    stream.write_all(b"\n").unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    line
}

fn status(sock: &Path) -> String {
    call(sock, r#"{"type":"status"}"#)
}

/// Block until `pred(status_json)` or the deadline. Returns the last status.
fn wait_until<F: Fn(&str) -> bool>(sock: &Path, secs: u64, pred: F) -> String {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut last = String::new();
    while Instant::now() < deadline {
        last = status(sock);
        if pred(&last) {
            return last;
        }
        sleep(Duration::from_millis(100));
    }
    last
}

struct DaemonGuard(Child);
impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// Ignored by default: spawns the daemon binary and a shell hook, so it needs a
// real environment (a shell, process spawning) that the Nix build sandbox — where
// crane runs `cargo test` as the package check — doesn't provide. Run explicitly:
//   cargo test --test pause_cancel -- --ignored
#[test]
#[ignore]
fn pause_cancels_in_flight_and_resume_completes() {
    let dir = tmpdir();
    let enqueue = dir.join("enqueue.sock");
    let control = dir.join("control.sock");
    let hook = dir.join("hook.sh");
    let mark = dir.join("upload.log");
    fs::write(&mark, "").unwrap();

    // Fake long upload that must die on SIGTERM.
    fs::write(
        &hook,
        format!(
            "#!/usr/bin/env bash\n\
             echo START >> {m}\n\
             trap 'echo TERMINATED >> {m}; exit 143' TERM\n\
             for i in $(seq 1 100); do sleep 0.1; done\n\
             echo DONE >> {m}\n",
            m = mark.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();

    let child = Command::new(BIN)
        .args([
            "daemon",
            "--hook",
            hook.to_str().unwrap(),
            "--socket",
            enqueue.to_str().unwrap(),
            "--control-socket",
            control.to_str().unwrap(),
            "--concurrency",
            "1",
            "--retries",
            "1",
            "--retry-interval-secs",
            "1",
        ])
        .spawn()
        .expect("spawn daemon");
    let _guard = DaemonGuard(child);

    let up = Instant::now();
    while !control.exists() && up.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50));
    }
    assert!(control.exists(), "control socket never appeared");

    let resp = call(
        &enqueue,
        r#"{"type":"enqueue","out_paths":["/nix/store/fake-aaa"]}"#,
    );
    assert!(resp.contains("\"ack\""), "enqueue not acked: {resp}");

    let s = wait_until(&control, 5, |s| s.contains("\"in_flight\":1"));
    assert!(s.contains("\"in_flight\":1"), "never went in flight: {s}");
    assert!(s.contains("\"paused\":false"), "unexpected paused: {s}");
    assert!(read(&mark).contains("START"), "hook never started");

    // Pause (game starts) → cancels the in-flight upload. The job isn't lost:
    // with one worker it's immediately re-pulled and parked on the gate, so it
    // becomes invisible in the queue (queue_depth back to 0, in_flight 0). Its
    // survival is proven below by resume completing it.
    let resp = call(&control, r#"{"type":"pause"}"#);
    assert!(resp.contains("\"ack\""), "pause not acked: {resp}");

    let s = wait_until(&control, 5, |s| s.contains("\"in_flight\":0"));
    assert!(
        s.contains("\"in_flight\":0"),
        "still in flight after pause: {s}"
    );
    assert!(s.contains("\"paused\":true"), "not marked paused: {s}");
    assert!(
        s.contains("\"processed_total\":0"),
        "wrongly counted processed: {s}"
    );
    assert!(
        s.contains("\"failed_total\":0"),
        "cancel wrongly counted as failure: {s}"
    );

    let log = read(&mark);
    assert!(
        log.contains("TERMINATED"),
        "hook wasn't signaled on pause: {log}"
    );
    assert!(
        !log.contains("DONE"),
        "upload finished despite pause: {log}"
    );

    // Resume (game ends) → the requeued job re-runs and completes.
    let resp = call(&control, r#"{"type":"resume"}"#);
    assert!(resp.contains("\"ack\""), "resume not acked: {resp}");

    let s = wait_until(&control, 20, |s| s.contains("\"processed_total\":1"));
    assert!(
        s.contains("\"processed_total\":1"),
        "never completed after resume: {s}"
    );
    assert!(s.contains("\"queue_depth\":0"), "queue not drained: {s}");
    assert!(
        s.contains("\"paused\":false"),
        "still paused after resume: {s}"
    );

    let log = read(&mark);
    assert_eq!(
        log.matches("START").count(),
        2,
        "expected a second run: {log}"
    );
    assert!(log.contains("DONE"), "second run didn't finish: {log}");
}

fn read(p: &Path) -> String {
    fs::read_to_string(p).unwrap_or_default()
}
