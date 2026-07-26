// Subcommand clients: enqueue / status / wait.
//
// All three open a Unix socket, write one line of JSON, read one line back,
// close. Connect retries with exponential backoff so a freshly socket-
// activated daemon has a moment to come up.

use std::io::BufReader;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use tracing::debug;

use crate::protocol::{self, Request, Response, DRV_PATH_ENV, OUT_PATHS_ENV};

pub fn enqueue(socket: &Path) -> Result<(), String> {
    let drv_path = std::env::var(DRV_PATH_ENV).ok().filter(|s| !s.is_empty());
    let out_paths: Vec<String> = std::env::var(OUT_PATHS_ENV)
        .unwrap_or_default()
        .split_whitespace()
        .map(String::from)
        .collect();
    if out_paths.is_empty() {
        // No-op success: nix-daemon sometimes fires the hook with nothing to
        // forward. Returning an error would surface as a build-time hook
        // failure for what's effectively a no-op.
        debug!("OUT_PATHS empty, nothing to enqueue");
        return Ok(());
    }

    let req = Request::Enqueue {
        drv_path,
        out_paths,
    };
    ack_call(socket, &req)
}

pub fn status(socket: &Path) -> Result<(), String> {
    match call(socket, &Request::Status)? {
        Response::Status(s) => {
            let pretty = serde_json::to_string_pretty(&s).map_err(|e| format!("encode: {e}"))?;
            println!("{pretty}");
            Ok(())
        }
        other => Err(format!("unexpected response: {other:?}")),
    }
}

pub fn pause(socket: &Path) -> Result<(), String> {
    ack_call(socket, &Request::Pause)
}

pub fn resume(socket: &Path) -> Result<(), String> {
    ack_call(socket, &Request::Resume)
}

fn ack_call(socket: &Path, req: &Request) -> Result<(), String> {
    match call(socket, req)? {
        Response::Ack => Ok(()),
        other => Err(format!("unexpected response: {other:?}")),
    }
}

/// Send one request, return the response — translating `Response::Error`
/// from the daemon into our local `Err` so callers only handle one error
/// channel.
fn call(socket: &Path, req: &Request) -> Result<Response, String> {
    let mut stream = connect_with_retry(socket)?;
    // A wedged daemon (accepting but not replying) would otherwise hang the
    // caller forever — and the always-on game-pause watcher blocks on this.
    let timeout = Some(Duration::from_secs(10));
    stream
        .set_read_timeout(timeout)
        .and_then(|_| stream.set_write_timeout(timeout))
        .map_err(|e| format!("set timeout: {e}"))?;
    protocol::write_line(&mut stream, req)?;
    let mut reader = BufReader::new(stream);
    match protocol::read_line(&mut reader)? {
        Response::Error { message } => Err(format!("daemon error: {message}")),
        other => Ok(other),
    }
}

fn connect_with_retry(socket: &Path) -> Result<UnixStream, String> {
    let mut delay = Duration::from_millis(100);
    let mut last_err = String::new();
    for attempt in 1..=10 {
        match UnixStream::connect(socket) {
            Ok(s) => return Ok(s),
            Err(e) => {
                last_err = e.to_string();
                if attempt < 10 {
                    debug!(socket = %socket.display(), attempt, error = %e, "connect retry");
                    std::thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
            }
        }
    }
    Err(format!("connect {}: {last_err}", socket.display()))
}
