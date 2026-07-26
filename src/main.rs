mod client;
mod daemon;
mod gate;
mod logging;
mod network_state;
mod protocol;
mod socket_activation;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tracing::error;

/// Async post-build-hook queue daemon for pushing Nix store paths to a binary cache.
#[derive(Parser, Debug)]
#[command(name = "queued-build-hook", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the queue daemon (typically socket-activated by systemd).
    Daemon(DaemonArgs),

    /// Submit OUT_PATHS (and optional DRV_PATH) from env vars to the daemon.
    Enqueue(SocketArgs),

    /// Print the daemon's queue snapshot as pretty JSON.
    Status(SocketArgs),

    /// Park all uploads until `resume`. Cancels any in-flight upload (requeued,
    /// not failed). Point --socket at the control socket if one is configured.
    Pause(SocketArgs),

    /// Lift a manual `pause`. Does not override a metered pause.
    Resume(SocketArgs),
}

#[derive(Parser, Debug)]
struct DaemonArgs {
    /// Path to the real post-build-hook to invoke.
    /// Receives DRV_PATH and OUT_PATHS in its environment.
    #[arg(long)]
    hook: PathBuf,

    /// Unix socket path. Ignored when started with systemd LISTEN_FDS.
    #[arg(long, default_value = "./pusher.sock")]
    socket: PathBuf,

    /// Optional control-only socket path (pause/resume/status). Bound directly
    /// only when not socket-activated; under systemd it arrives as the
    /// `control`-named LISTEN_FD. Omit to disable the separate control socket.
    #[arg(long)]
    control_socket: Option<PathBuf>,

    /// Number of worker threads.
    #[arg(long, default_value_t = 1)]
    concurrency: usize,

    /// Max attempts per job before incrementing failed_total and dropping.
    #[arg(long, default_value_t = 3)]
    retries: u32,

    /// Backoff between retries, in seconds.
    #[arg(long, default_value_t = 30)]
    retry_interval_secs: u64,

    /// Pause uploads while NetworkManager reports a metered connection.
    /// Subscribes to NM's D-Bus signals — instant transitions, no polling.
    /// No-op if NM isn't on the system bus.
    #[arg(long)]
    pause_on_metered: bool,
}

#[derive(Parser, Debug)]
struct SocketArgs {
    /// Path of the daemon's Unix socket.
    #[arg(long)]
    socket: PathBuf,
}

impl From<DaemonArgs> for daemon::DaemonOpts {
    fn from(a: DaemonArgs) -> Self {
        daemon::DaemonOpts {
            hook: a.hook,
            socket: a.socket,
            control_socket: a.control_socket,
            concurrency: a.concurrency,
            // Clamped to >=1 in daemon::run, where they're consumed.
            retries: a.retries,
            retry_interval: Duration::from_secs(a.retry_interval_secs),
            pause_on_metered: a.pause_on_metered,
        }
    }
}

fn main() -> ExitCode {
    logging::init();

    let cli = Cli::parse();

    let result = match cli.command {
        Command::Daemon(a) => daemon::run(a.into()),
        Command::Enqueue(a) => client::enqueue(&a.socket),
        Command::Status(a) => client::status(&a.socket),
        Command::Pause(a) => client::pause(&a.socket),
        Command::Resume(a) => client::resume(&a.socket),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %e, "command failed");
            ExitCode::FAILURE
        }
    }
}
