use godot::init::InitStage;
use godot::prelude::*;

mod blobs;
mod connection;
mod dispatch;
mod docs;
mod document;
mod endpoint;
mod error;
mod gossip;
mod log;
mod peer;
mod puppy;
mod raw;
mod runtime;
mod session;
mod stats;
mod stream;
#[cfg(test)]
mod testing;
mod topic;
mod transfer;

struct GdIroh;

#[gdextension]
unsafe impl ExtensionLibrary for GdIroh {
    // Nothing is started at load on purpose. Endpoints are ordinary objects a
    // game constructs, and the network runtime begins at the first bind and
    // ends with the last endpoint — a project that merely ships the addon runs
    // no threads of ours.

    fn on_stage_deinit(stage: InitStage) {
        if stage == InitStage::Scene {
            // Endpoints normally stop the runtime themselves as their leases
            // fall; this is the backstop for one a game never released.
            runtime::shutdown();
        }
    }
}
