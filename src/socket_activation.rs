// systemd LISTEN_FDS handling.
//
// Reference: man sd_listen_fds(3).

use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::net::UnixListener;

use tracing::warn;

/// Per sd_listen_fds(3), inherited fds start at 3 (after stdin/stdout/stderr).
const SD_LISTEN_FDS_START: RawFd = 3;

pub fn try_systemd_socket() -> Option<UnixListener> {
    let listen_pid: Option<i32> = std::env::var("LISTEN_PID")
        .ok()
        .and_then(|s| s.parse().ok());
    let listen_fds: Option<i32> = std::env::var("LISTEN_FDS")
        .ok()
        .and_then(|s| s.parse().ok());

    // sd_listen_fds(3) recommends unsetting these unconditionally so any child
    // we exec doesn't think it inherited socket activation.
    std::env::remove_var("LISTEN_PID");
    std::env::remove_var("LISTEN_FDS");
    std::env::remove_var("LISTEN_FDNAMES");

    let listen_pid = listen_pid?;
    let listen_fds = listen_fds?;

    if listen_pid != unsafe { libc::getpid() } {
        return None;
    }
    if listen_fds < 1 {
        return None;
    }
    if listen_fds > 1 {
        warn!(
            listen_fds,
            "ignoring extra socket-activation fds; using only fd 3"
        );
    }

    // Belt-and-suspenders CLOEXEC. systemd >= 240 sets it; older releases
    // and non-systemd activators may not. Without this, every child we spawn
    // (the post-build-hook) inherits the listening socket — leak + the child
    // could accept() on it.
    unsafe {
        let flags = libc::fcntl(SD_LISTEN_FDS_START, libc::F_GETFD);
        if flags >= 0 {
            libc::fcntl(SD_LISTEN_FDS_START, libc::F_SETFD, flags | libc::FD_CLOEXEC);
        }
    }

    // SAFETY: systemd guarantees the fd is a valid AF_UNIX listener.
    Some(unsafe { UnixListener::from_raw_fd(SD_LISTEN_FDS_START) })
}
