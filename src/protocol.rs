// Wire protocol: one JSON object per line.
//
//   client → server   {"type":"enqueue","drv_path":...,"out_paths":[...]}
//   server → client   {"type":"ack"}
//
//   client → server   {"type":"status"}
//   server → client   {"type":"status","queue_depth":N,...}

use std::io::{BufRead, Write};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Env var nix-daemon sets on post-build-hook invocations to the .drv path.
pub const DRV_PATH_ENV: &str = "DRV_PATH";
/// Env var nix-daemon sets on post-build-hook invocations to the new outputs.
pub const OUT_PATHS_ENV: &str = "OUT_PATHS";

/// Read one newline-delimited JSON message from `r`.
pub fn read_line<R: BufRead, T: DeserializeOwned>(r: &mut R) -> Result<T, String> {
    let mut line = String::new();
    let n = r.read_line(&mut line).map_err(|e| format!("read: {e}"))?;
    if n == 0 {
        return Err("empty request".into());
    }
    serde_json::from_str(line.trim_end()).map_err(|e| format!("decode: {e}"))
}

/// Write `msg` as one newline-delimited JSON message to `w`.
pub fn write_line<W: Write, T: Serialize>(w: &mut W, msg: &T) -> Result<(), String> {
    let bytes = serde_json::to_string(msg).map_err(|e| format!("encode: {e}"))?;
    w.write_all(bytes.as_bytes())
        .and_then(|_| w.write_all(b"\n"))
        .map_err(|e| format!("write: {e}"))
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Enqueue {
        #[serde(default)]
        drv_path: Option<String>,
        out_paths: Vec<String>,
    },
    Status,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Ack,
    Error { message: String },
    Status(StatusReport),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StatusReport {
    pub queue_depth: usize,
    pub in_flight: usize,
    pub processed_total: u64,
    pub failed_total: u64,
    pub uptime_seconds: u64,
    /// Rolling average of completed-batch wall-clock durations (~last 30 min,
    /// up to 200 entries). `None` until at least one batch has completed.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub avg_batch_seconds: Option<f64>,
    /// The batch the worker is currently running, if any.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub current_batch: Option<CurrentBatch>,
    /// The most recent successful batch.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_completed: Option<RecentBatch>,
    /// `None` when --pause-on-metered is off. Otherwise reports NM availability
    /// and the live metered flag (whether the daemon is currently parked).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub network: Option<NetworkStatus>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CurrentBatch {
    /// .drv that produced this batch, if known.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub drv_path: Option<String>,
    /// Number of OUT_PATHS in the batch.
    pub paths_count: usize,
    /// Total NAR size of the unique closure of all OUT_PATHS, in bytes. Upper
    /// bound on what `nix copy` will upload — paths already in the cache are
    /// skipped, so the actual upload may be smaller. `None` if the closure
    /// query (`nix path-info --recursive`) hasn't finished yet or failed.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub closure_bytes: Option<u64>,
    pub started_seconds_ago: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RecentBatch {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub drv_path: Option<String>,
    pub paths_count: usize,
    /// See CurrentBatch.closure_bytes — same caveat (closure size, not actual
    /// bytes pushed).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub closure_bytes: Option<u64>,
    pub duration_seconds: u64,
    pub completed_seconds_ago: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NetworkStatus {
    /// True when NetworkManager is reachable on the system bus.
    pub nm_available: bool,
    /// True when NM reports a metered connection (NM_METERED_YES or GUESS_YES).
    /// While true, workers that pull a job park until the connection becomes
    /// unmetered.
    pub metered: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_request_round_trips() {
        let original = Request::Enqueue {
            drv_path: Some("/nix/store/xyz.drv".into()),
            out_paths: vec!["/nix/store/abc-foo".into(), "/nix/store/def-bar".into()],
        };
        let json = serde_json::to_string(&original).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        match parsed {
            Request::Enqueue {
                drv_path,
                out_paths,
            } => {
                assert_eq!(drv_path.as_deref(), Some("/nix/store/xyz.drv"));
                assert_eq!(out_paths.len(), 2);
            }
            _ => panic!("expected Enqueue"),
        }
    }

    #[test]
    fn enqueue_accepts_missing_drv_path_field() {
        // Older clients may omit drv_path; the #[serde(default)] keeps us
        // backward-compatible.
        let json = r#"{"type":"enqueue","out_paths":["/nix/store/foo"]}"#;
        let parsed: Request = serde_json::from_str(json).unwrap();
        match parsed {
            Request::Enqueue {
                drv_path,
                out_paths,
            } => {
                assert!(drv_path.is_none());
                assert_eq!(out_paths.len(), 1);
            }
            _ => panic!("expected Enqueue"),
        }
    }
}
