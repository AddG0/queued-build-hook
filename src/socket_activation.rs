// systemd LISTEN_FDS handling.
//
// Reference: man sd_listen_fds(3), sd_listen_fds_with_names(3).

use std::collections::HashMap;
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::net::UnixListener;

/// Per sd_listen_fds(3), inherited fds start at 3 (after stdin/stdout/stderr).
const SD_LISTEN_FDS_START: RawFd = 3;

/// Return the socket-activation listeners keyed by their `FileDescriptorName`
/// (from `LISTEN_FDNAMES`). When names are absent, keys are synthesized as
/// `fd<N>`. Empty when the process wasn't socket-activated.
pub fn systemd_listeners() -> HashMap<String, UnixListener> {
    let mut map = HashMap::new();

    let listen_pid: Option<i32> = std::env::var("LISTEN_PID")
        .ok()
        .and_then(|s| s.parse().ok());
    let listen_fds: Option<i32> = std::env::var("LISTEN_FDS")
        .ok()
        .and_then(|s| s.parse().ok());
    let names: Vec<String> = std::env::var("LISTEN_FDNAMES")
        .ok()
        .map(|s| s.split(':').map(String::from).collect())
        .unwrap_or_default();

    // sd_listen_fds(3) recommends unsetting these unconditionally so any child
    // we exec doesn't think it inherited socket activation.
    std::env::remove_var("LISTEN_PID");
    std::env::remove_var("LISTEN_FDS");
    std::env::remove_var("LISTEN_FDNAMES");

    let (listen_pid, listen_fds) = match (listen_pid, listen_fds) {
        (Some(p), Some(n)) => (p, n),
        _ => return map,
    };
    if listen_pid != unsafe { libc::getpid() } || listen_fds < 1 {
        return map;
    }

    for i in 0..listen_fds {
        let fd = SD_LISTEN_FDS_START + i;

        // Belt-and-suspenders CLOEXEC. systemd >= 240 sets it; older releases
        // and non-systemd activators may not. Without this, every child we
        // spawn (the post-build-hook) inherits the listening socket.
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags >= 0 {
                libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
            }
        }

        let name = names
            .get(i as usize)
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_else(|| format!("fd{fd}"));

        // SAFETY: systemd guarantees each fd is a valid AF_UNIX listener.
        map.insert(name, unsafe { UnixListener::from_raw_fd(fd) });
    }

    map
}
