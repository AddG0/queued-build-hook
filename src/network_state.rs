// NetworkManager metered-state watcher.
//
// Subscribes to PropertiesChanged on the NM root object via D-Bus and
// updates a shared atomic so the worker loop can block on unmetered. Pure
// event-driven — no polling. Falls back to "never metered" gracefully if
// NM isn't on the bus (systemd-networkd hosts, headless servers, NM not
// yet started, etc.) so the daemon still does useful work.
//
// NM_METERED enum values from NetworkManager's API:
//   0 UNKNOWN, 1 YES, 2 NO, 3 GUESS_YES, 4 GUESS_NO
// Treat YES (1) and GUESS_YES (3) as metered.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use tracing::{info, warn};
use zbus::blocking::fdo::PropertiesProxy;
use zbus::blocking::{Connection, Proxy};
use zbus::names::InterfaceName;

const NM_SERVICE: &str = "org.freedesktop.NetworkManager";
const NM_PATH: &str = "/org/freedesktop/NetworkManager";
const NM_IFACE: &str = "org.freedesktop.NetworkManager";

fn is_metered_value(v: u32) -> bool {
    matches!(v, 1 | 3)
}

pub struct NetworkState {
    available: AtomicBool,
    metered: AtomicBool,
    cv: Condvar,
    cv_mutex: Mutex<()>,
}

impl NetworkState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            available: AtomicBool::new(false),
            metered: AtomicBool::new(false),
            cv: Condvar::new(),
            cv_mutex: Mutex::new(()),
        })
    }

    pub fn is_metered(&self) -> bool {
        self.metered.load(Ordering::Acquire)
    }

    pub fn is_available(&self) -> bool {
        self.available.load(Ordering::Acquire)
    }

    /// Block while metered. Returns when unmetered or `should_stop` returns true.
    /// Times out periodically so shutdown is observed without a separate signal.
    pub fn wait_while_metered<F: Fn() -> bool>(&self, should_stop: F) {
        let mut guard = self.cv_mutex.lock().unwrap();
        while self.is_metered() && !should_stop() {
            guard = self
                .cv
                .wait_timeout(guard, Duration::from_secs(1))
                .unwrap()
                .0;
        }
    }

    /// Wake any thread blocked in `wait_while_metered` (e.g. on shutdown).
    pub fn notify_all(&self) {
        let _g = self.cv_mutex.lock().unwrap();
        self.cv.notify_all();
    }

    fn set_metered(&self, m: bool) {
        let prev = self.metered.swap(m, Ordering::AcqRel);
        if prev != m {
            info!(metered = m, "NetworkManager metered changed");
            self.notify_all();
        }
    }
}

/// Spawn the NM watcher thread. Returns immediately with a state handle whose
/// values populate as soon as the watcher reaches NM. If NM isn't reachable,
/// `is_available()` stays false and `is_metered()` stays false — daemon
/// proceeds as if there were no metered awareness.
pub fn spawn_watcher() -> Arc<NetworkState> {
    let state = NetworkState::new();
    let s = Arc::clone(&state);
    thread::Builder::new()
        .name("nm-watcher".into())
        .spawn(move || {
            if let Err(e) = run(&s) {
                warn!(
                    error = %e,
                    "NetworkManager not reachable — pause-on-metered will be a no-op"
                );
                s.available.store(false, Ordering::Release);
                s.metered.store(false, Ordering::Release);
                s.notify_all();
            }
        })
        .expect("spawn nm-watcher thread");
    state
}

fn run(state: &NetworkState) -> Result<(), String> {
    let conn = Connection::system().map_err(|e| format!("system bus: {e}"))?;

    // Initial state via Properties.Get.
    let proxy =
        Proxy::new(&conn, NM_SERVICE, NM_PATH, NM_IFACE).map_err(|e| format!("proxy: {e}"))?;
    let initial: u32 = proxy
        .get_property("Metered")
        .map_err(|e| format!("get Metered: {e}"))?;
    state
        .metered
        .store(is_metered_value(initial), Ordering::Release);
    state.available.store(true, Ordering::Release);
    state.notify_all();
    info!(
        nm_metered = initial,
        metered = is_metered_value(initial),
        "NetworkManager available"
    );

    // Subscribe to PropertiesChanged on the NM root and react to Metered changes.
    let props = PropertiesProxy::builder(&conn)
        .destination(NM_SERVICE)
        .map_err(|e| format!("dest: {e}"))?
        .path(NM_PATH)
        .map_err(|e| format!("path: {e}"))?
        .build()
        .map_err(|e| format!("build proxy: {e}"))?;
    let signals = props
        .receive_properties_changed()
        .map_err(|e| format!("subscribe: {e}"))?;

    let nm_iface = InterfaceName::try_from(NM_IFACE).map_err(|e| format!("iface: {e}"))?;
    for change in signals {
        let args = change.args().map_err(|e| format!("args: {e}"))?;
        if args.interface_name != nm_iface {
            continue;
        }
        if let Some(v) = args.changed_properties.get("Metered") {
            if let Ok(n) = u32::try_from(v) {
                state.set_metered(is_metered_value(n));
            }
        }
    }
    Ok(())
}
