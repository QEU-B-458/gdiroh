//! The Tokio runtime that carries every network task.
//!
//! Godot objects are not `Send`, so nothing spawned here may touch a `Gd<T>`.
//! Tasks reach the main thread through channels, and the main thread drains
//! them. Crossing that line in the other direction is undefined behaviour.
//!
//! The runtime is started by the first endpoint that needs it and stopped when
//! the last one goes, so a project merely carrying the addon runs no network
//! threads at all. [`Lease`] is what counts holders.

use std::future::Future;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use tokio::runtime::{Builder, Handle, Runtime};
use tokio::task::JoinHandle;

use crate::error::Report;

/// Networking does not need a thread per core, and a game cannot spare them.
/// Revisit once blob transfers give us something real to measure.
const WORKER_THREADS: usize = 2;

/// How long shutdown waits on in-flight tasks before abandoning them.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

struct Shared {
    runtime: Option<Runtime>,
    /// Live [`Lease`]s. The runtime stops when this reaches zero.
    holders: usize,
}

static SHARED: Mutex<Shared> = Mutex::new(Shared {
    runtime: None,
    holders: 0,
});

/// Keeps the runtime alive for as long as it is held.
///
/// The first lease starts the runtime; dropping the last one stops it. Hold one
/// per endpoint and let it fall with the endpoint rather than releasing by hand.
pub(crate) struct Lease(());

impl Lease {
    /// Takes a lease, starting the runtime if this is the first.
    ///
    /// `None` means the runtime could not be built, and the caller cannot do
    /// any networking.
    pub(crate) fn acquire() -> Option<Self> {
        let mut shared = lock();

        if shared.runtime.is_none() {
            let built = Builder::new_multi_thread()
                .worker_threads(WORKER_THREADS)
                .thread_name("gdiroh")
                .enable_all()
                .build()
                .report("could not start the network runtime");

            built.as_ref()?;
            crate::log::info!("network runtime started on {WORKER_THREADS} threads");
            shared.runtime = built;
        }

        shared.holders += 1;
        Some(Self(()))
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut shared = lock();
        shared.holders = shared.holders.saturating_sub(1);

        if shared.holders > 0 {
            return;
        }

        let Some(runtime) = shared.runtime.take() else {
            return;
        };
        drop(shared);

        // `shutdown_timeout` blocks, and this runs on the main thread when a
        // game closes its last endpoint mid-play. Waiting out the grace period
        // here would stall the frame, so hand it to a thread that can afford to.
        std::thread::spawn(move || runtime.shutdown_timeout(SHUTDOWN_GRACE));
        crate::log::info!("network runtime stopped");
    }
}

/// Stops the runtime regardless of who still holds a lease.
///
/// The backstop for extension unload, where the process is going away and an
/// endpoint a game never closed must not keep threads running. Blocks, which is
/// right here and wrong anywhere else.
pub(crate) fn shutdown() {
    // The guard is dropped at the end of this statement, so the blocking wait
    // below happens without the lock held.
    let taken = {
        let mut shared = lock();
        shared.holders = 0;
        shared.runtime.take()
    };

    let Some(runtime) = taken else {
        return;
    };

    runtime.shutdown_timeout(SHUTDOWN_GRACE);
    crate::log::info!("network runtime stopped");
}

/// Spawns a task, or reports `None` if the runtime is not running.
pub(crate) fn spawn<F>(future: F) -> Option<JoinHandle<F::Output>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    lock().runtime.as_ref().map(|runtime| runtime.spawn(future))
}

/// Spawns a task nobody waits on, reporting whether it started.
///
/// Prefers the ambient runtime when there is one — true for every task started
/// from inside another network task — and falls back to the shared runtime.
/// Tests supply their own runtime and take the first branch.
pub(crate) fn detach<F>(future: F) -> bool
where
    F: Future<Output = ()> + Send + 'static,
{
    detach_handle(future).is_some()
}

/// A handle to whichever runtime is in play, for code that has to be built
/// inside a runtime context rather than spawned into one.
///
/// Some constructors register timers or spawn tasks as they are built, and panic
/// outright when no reactor is in scope — which is the normal state on Godot's
/// main thread. Enter the returned handle around such a call.
pub(crate) fn handle() -> Option<Handle> {
    if let Ok(handle) = Handle::try_current() {
        return Some(handle);
    }

    lock().runtime.as_ref().map(Runtime::handle).cloned()
}

/// Like [`detach`], but hands back the handle so the caller can abort the task.
pub(crate) fn detach_handle<F>(future: F) -> Option<JoinHandle<()>>
where
    F: Future<Output = ()> + Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => Some(handle.spawn(future)),
        Err(_) => spawn(future),
    }
}

fn lock() -> MutexGuard<'static, Shared> {
    // Poisoning only tells us some other caller panicked while holding the
    // lock; the slot itself is still sound.
    SHARED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running() -> bool {
        lock().runtime.is_some()
    }

    /// One test rather than four, because they share a process-wide count and
    /// separate tests would race each other through it.
    #[test]
    fn the_runtime_lives_exactly_as_long_as_its_leases() {
        assert!(!running(), "nothing should have started the runtime yet");

        let first = Lease::acquire().expect("the first lease starts the runtime");
        assert!(running());

        let second = Lease::acquire().expect("a second lease shares the same runtime");
        assert!(running());

        drop(first);
        assert!(running(), "a lease still held must keep the runtime alive");

        drop(second);
        assert!(!running(), "the last lease going stops the runtime");
    }
}
