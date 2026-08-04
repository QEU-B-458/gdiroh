//! `IrohTransfer` — one blob operation in flight, as a Godot object.
//!
//! ```gdscript
//! var send := endpoint.add_file("user://level.dat", "level")
//! send.completed.connect(func(hash, _data):
//!     var ticket := endpoint.blob_ticket(hash)
//!     share_with_players(ticket))
//! ```
//!
//! Keep the returned object in a variable until it reports back. It is
//! reference counted, and dropping the last reference gives up on the transfer.

use godot::classes::RefCounted;
use godot::prelude::*;

use crate::blobs::{Event, Transfer};
use crate::endpoint::scene_tree;

/// One blob operation in flight — an add, fetch, export or read.
///
/// These come from the blob methods on [IrohEndpoint]. Watch
/// [code]progress[/code] to follow along; at the end exactly one of
/// [code]completed[/code] or [code]failed[/code] fires.
///
/// Keep the transfer in a variable until it reports back. It is reference
/// counted, and dropping the last reference gives up on it.
#[derive(GodotClass)]
// `no_init` because transfers come from an endpoint, never from `new()`.
#[class(no_init, base=RefCounted)]
pub struct IrohTransfer {
    /// Present until the transfer finishes one way or the other.
    transfer: Option<Transfer>,
    /// Total bytes, or `0` while unknown. Not every operation reports one.
    total: u64,
    /// Filled in when the transfer completes; empty before that, for every
    /// kind of operation.
    hash: String,
    done: u64,
    ticking: bool,
    base: Base<RefCounted>,
}

impl IrohTransfer {
    pub(crate) fn wrap(transfer: Transfer) -> Gd<Self> {
        let mut object = Gd::from_init_fn(|base| Self {
            transfer: Some(transfer),
            total: 0,
            hash: String::new(),
            done: 0,
            ticking: false,
            base,
        });
        object.bind_mut().start_ticking();
        object
    }

    fn start_ticking(&mut self) {
        if self.ticking {
            return;
        }

        let Some(mut tree) = scene_tree() else {
            crate::log::error!("no scene tree to poll on; this transfer cannot report anything");
            return;
        };

        let callable = Callable::from_object_method(&self.to_gd(), "_drain");
        tree.connect("process_frame", &callable);
        self.ticking = true;
    }

    fn stop_ticking(&mut self) {
        if !self.ticking {
            return;
        }

        if let Some(mut tree) = scene_tree() {
            let callable = Callable::from_object_method(&self.to_gd(), "_drain");
            tree.disconnect("process_frame", &callable);
        }

        self.ticking = false;
    }

    /// Emits after the current call unwinds. Emitting inline would re-enter this
    /// object while it is still mutably borrowed, which panics.
    fn emit_later(&mut self, signal: &str, args: &[Variant]) {
        let mut call = Vec::with_capacity(args.len() + 1);
        call.push(signal.to_variant());
        call.extend_from_slice(args);
        self.base_mut().call_deferred("emit_signal", &call);
    }

    fn handle(&mut self, event: Event) {
        match event {
            Event::Size(size) => {
                self.total = size;
                let done = self.done as i64;
                self.emit_later("progress", &[done.to_variant(), (size as i64).to_variant()]);
            }
            Event::Progress(done) => {
                self.done = done;
                let total = self.total as i64;
                self.emit_later(
                    "progress",
                    &[(done as i64).to_variant(), total.to_variant()],
                );
            }
            Event::Done { hash, data } => {
                self.hash = hash.to_string();
                self.finished();
                self.emit_later(
                    "completed",
                    &[
                        GString::from(&hash.to_string()).to_variant(),
                        PackedByteArray::from(&data[..]).to_variant(),
                    ],
                );
            }
            Event::Failed(reason) => {
                self.finished();
                self.emit_later("failed", &[GString::from(&reason).to_variant()]);
            }
        }
    }

    fn finished(&mut self) {
        self.transfer = None;
        self.stop_ticking();
    }
}

#[godot_api]
impl IrohTransfer {
    /// Emitted as the transfer moves along.
    ///
    /// `total` is `0` until the size is known, and stays `0` for operations that
    /// never learn one. Progress is best effort — a transfer that finishes in
    /// one step may report nothing before completing, so do not wait on it.
    #[signal]
    fn progress(done: i64, total: i64);

    /// Emitted once the transfer succeeds.
    ///
    /// `hash` names the blob. `data` carries the contents, but only for
    /// [method IrohEndpoint.read_blob]; every other operation leaves it empty rather
    /// than pulling a whole file into memory you did not ask for.
    #[signal]
    fn completed(hash: GString, data: PackedByteArray);

    /// Emitted if the transfer fails. Nothing follows it.
    #[signal]
    fn failed(reason: GString);

    /// Drains work finished on the runtime. Connected to `SceneTree`'s
    /// `process_frame`, so it always runs on the main thread.
    #[func]
    fn _drain(&mut self) {
        let Some(transfer) = self.transfer.as_mut() else {
            return;
        };

        // Collected first because handling an event borrows `self` again.
        let mut pending = Vec::new();
        while let Some(event) = transfer.try_recv() {
            pending.push(event);
        }

        for event in pending {
            self.handle(event);
        }
    }

    /// Bytes moved so far.
    #[func]
    fn get_done(&self) -> i64 {
        self.done as i64
    }

    /// Total bytes, or `0` while that is still unknown.
    #[func]
    fn get_total(&self) -> i64 {
        self.total as i64
    }

    /// The blob this transfer is about, or empty until it completes.
    #[func]
    fn get_hash(&self) -> GString {
        GString::from(&self.hash)
    }

    /// Whether the transfer is still going.
    #[func]
    fn is_running(&self) -> bool {
        self.transfer.is_some()
    }

    /// Gives up. No further signals follow.
    ///
    /// A partly fetched blob stays in the store, so asking for it again resumes
    /// rather than starting over.
    #[func]
    fn cancel(&mut self) {
        self.finished();
    }
}
