// Unified pause gate.
//
// Workers park here while any pause *reason* is active. Two independent
// sources feed it: the NetworkManager metered watcher (REASON_METERED) and
// manual pause/resume commands over the control socket (REASON_MANUAL). A
// reason is a bit; the gate is "blocked" whenever any bit is set. Both the
// worker's pre-hook park and the mid-hook cancel check consult the same
// `blocked()`, so a game starting mid-upload cancels the in-flight `nix copy`
// *and* keeps the next pull parked, with one shared wakeup.
//
// Event-driven: `set` flips a bit and wakes parked workers; `wait_while_blocked`
// re-checks on wakeup. The 1 s wait timeout is a belt-and-braces bound on any
// missed wakeup and lets a parked worker observe shutdown without a dedicated
// signal.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// NetworkManager reports a metered connection (`--pause-on-metered`).
pub const REASON_METERED: u32 = 1 << 0;
/// A `pause` control command is in effect until a matching `resume`.
pub const REASON_MANUAL: u32 = 1 << 1;

pub struct Gate {
    reasons: AtomicU32,
    cv: Condvar,
    m: Mutex<()>,
}

impl Gate {
    pub fn new() -> Self {
        Self {
            reasons: AtomicU32::new(0),
            cv: Condvar::new(),
            m: Mutex::new(()),
        }
    }

    /// Set or clear one reason bit. Wakes parked workers only on an actual
    /// transition, so repeated identical updates (a metered signal that didn't
    /// change, a redundant pause) are cheap no-ops.
    pub fn set(&self, reason: u32, active: bool) {
        // The whole read-modify-write is under the lock: two producers touching
        // different bits (nm-watcher METERED vs a control-socket MANUAL) would
        // otherwise race on a bare load/store and lose one bit. Holding the lock
        // for the store also closes the wakeup gap against a worker between its
        // `blocked()` check and its wait in `wait_while_blocked`.
        let _g = self.m.lock().unwrap();
        let prev = self.reasons.load(Ordering::Acquire);
        let next = if active {
            prev | reason
        } else {
            prev & !reason
        };
        if prev != next {
            self.reasons.store(next, Ordering::Release);
            self.cv.notify_all();
        }
    }

    fn reasons(&self) -> u32 {
        self.reasons.load(Ordering::Acquire)
    }

    pub fn blocked(&self) -> bool {
        self.reasons() != 0
    }

    pub fn has(&self, reason: u32) -> bool {
        self.reasons() & reason != 0
    }

    /// Block while any reason is active. Returns when the gate clears or
    /// `should_stop` returns true (shutdown).
    pub fn wait_while_blocked<F: Fn() -> bool>(&self, should_stop: F) {
        let mut g = self.m.lock().unwrap();
        while self.blocked() && !should_stop() {
            g = self.cv.wait_timeout(g, Duration::from_secs(1)).unwrap().0;
        }
    }

    /// Wake every parked worker (e.g. on shutdown) regardless of reason state.
    pub fn notify_all(&self) {
        let _g = self.m.lock().unwrap();
        self.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_tracks_any_reason() {
        let g = Gate::new();
        assert!(!g.blocked());
        g.set(REASON_METERED, true);
        assert!(g.blocked());
        assert!(g.has(REASON_METERED));
        assert!(!g.has(REASON_MANUAL));
        // A second reason keeps it blocked until BOTH clear.
        g.set(REASON_MANUAL, true);
        g.set(REASON_METERED, false);
        assert!(g.blocked());
        g.set(REASON_MANUAL, false);
        assert!(!g.blocked());
    }

    #[test]
    fn wait_returns_immediately_when_unblocked() {
        let g = Gate::new();
        g.wait_while_blocked(|| false);
    }

    #[test]
    fn wait_returns_when_should_stop() {
        let g = Gate::new();
        g.set(REASON_MANUAL, true);
        // Blocked, but should_stop short-circuits the park.
        g.wait_while_blocked(|| true);
    }
}
