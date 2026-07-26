// NetworkManager metered-state watcher.
//
// Subscribes to PropertiesChanged on the NM root object via D-Bus and drives
// the shared pause `Gate` (REASON_METERED) so workers park while metered.
// Pure event-driven — no polling. Falls back to "never metered" gracefully if
// NM isn't on the bus (systemd-networkd hosts, headless servers, NM not yet
// started, etc.) so the daemon still does useful work.
//
// The watcher also keeps its own `available`/`metered` atomics purely for the
// status report; the gate is the source of truth for *parking*.
//
// NM_METERED enum values from NetworkManager's API:
//   0 UNKNOWN, 1 YES, 2 NO, 3 GUESS_YES, 4 GUESS_NO
// Treat YES (1) and GUESS_YES (3) as metered.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tracing::{debug, info, warn};
use zbus::blocking::fdo::PropertiesProxy;
use zbus::blocking::{Connection, Proxy};
use zbus::names::InterfaceName;

use crate::gate::{Gate, REASON_METERED};

const NM_SERVICE: &str = "org.freedesktop.NetworkManager";
const NM_PATH: &str = "/org/freedesktop/NetworkManager";
const NM_IFACE: &str = "org.freedesktop.NetworkManager";

fn is_metered_value(v: u32) -> bool {
    matches!(v, 1 | 3)
}

/// Status-only view of the NM watcher. Parking is driven by the shared `Gate`,
/// which is also the single source of truth for the metered flag; only NM
/// availability — not a park reason — lives here.
pub struct NetworkState {
    available: AtomicBool,
    gate: Arc<Gate>,
}

impl NetworkState {
    fn new(gate: Arc<Gate>) -> Arc<Self> {
        Arc::new(Self {
            available: AtomicBool::new(false),
            gate,
        })
    }

    pub fn is_metered(&self) -> bool {
        self.gate.has(REASON_METERED)
    }

    pub fn is_available(&self) -> bool {
        self.available.load(Ordering::Acquire)
    }

    fn set_metered(&self, m: bool) {
        if self.is_metered() != m {
            info!(metered = m, "NetworkManager metered changed");
        }
        self.gate.set(REASON_METERED, m);
    }
}

/// Spawn the NM watcher thread. Returns immediately with a state handle whose
/// values populate as soon as the watcher reaches NM. If NM isn't reachable,
/// `is_available()` stays false, the metered gate bit stays clear, and the
/// daemon proceeds as if there were no metered awareness.
pub fn spawn_watcher(gate: Arc<Gate>) -> Arc<NetworkState> {
    let state = NetworkState::new(gate);
    let s = Arc::clone(&state);
    thread::Builder::new()
        .name("nm-watcher".into())
        .spawn(move || {
            // NM restarts end the signal stream, so re-subscribe in a loop.
            // Must clear the metered bit on every exit: leaving REASON_METERED
            // set with no producer to clear it parks uploads forever, and a
            // manual resume can't lift a metered pause. Fail open, not stuck.
            let mut backoff = Duration::from_secs(1);
            let mut announced = false;
            loop {
                match run(&s) {
                    Ok(()) => {
                        info!("NetworkManager signal stream ended — reconnecting");
                        backoff = Duration::from_secs(1);
                    }
                    Err(e) if !announced => {
                        warn!(error = %e, "NetworkManager not reachable — pause-on-metered inactive until it appears");
                        announced = true;
                    }
                    Err(e) => debug!(error = %e, "NetworkManager still unreachable — retrying"),
                }
                s.available.store(false, Ordering::Release);
                s.set_metered(false);
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(30));
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
    state.available.store(true, Ordering::Release);
    state.set_metered(is_metered_value(initial));
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
