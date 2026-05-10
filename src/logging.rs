// Logging setup. Uses `tracing` everywhere; emits structured fields.
//
// Sink selection:
//   - When the systemd journal socket is reachable (running under a
//     systemd unit), send records via `tracing-journald` so PRIORITY,
//     MESSAGE, and any structured fields land as proper journal entries
//     and can be filtered with `journalctl -p err`, `--output=json`, etc.
//   - Otherwise fall back to a human-readable stderr formatter with
//     timestamps. Useful in dev / `nix run` invocations.
//
// Filtering follows the standard `RUST_LOG` env var (`RUST_LOG=debug`,
// `RUST_LOG=queued_build_hook::daemon=trace`, etc). Defaults to `info`.

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub fn init() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let registry = tracing_subscriber::registry().with(env_filter);

    match tracing_journald::layer() {
        Ok(journald) => {
            registry.with(journald).init();
        }
        Err(_) => {
            registry
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_target(false)
                        .with_writer(std::io::stderr),
                )
                .init();
        }
    }
}
