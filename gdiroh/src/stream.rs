//! `IrohStream` — one bidirectional QUIC stream, as a Godot [`StreamPeer`].
//!
//! Being a `StreamPeer` means script gets `get_data`, `put_data`,
//! `get_available_bytes` and the whole family of `get_u8` / `get_string` /
//! `get_var` helpers without gdiroh reimplementing any of them. That includes
//! the contract underneath them: `get_data` waits for what it was asked for,
//! because the length-then-payload helpers mis-frame the stream on a short
//! read. `get_partial_data` is the non-waiting path.
//!
//! ```gdscript
//! var stream := connection.open_stream()
//! stream.put_var({"spawn": "puppy", "at": Vector3.ZERO})
//! stream.finish()
//! ```
//!
//! [`StreamPeer`]: https://docs.godotengine.org/en/stable/classes/class_streampeer.html

use std::cell::{RefCell, RefMut};
use std::collections::VecDeque;

use bytes::{Buf, Bytes};
use godot::classes::{IStreamPeerExtension, StreamPeerExtension};
use godot::global::Error;
use godot::prelude::*;
// Not reachable through the `godot` facade; see the note in Cargo.toml.
use godot_core::meta::RawPtr;

use crate::raw::{Incoming, Stream};

/// One bidirectional stream on an [IrohConnection], as a Godot [StreamPeer].
///
/// Comes from [method IrohConnection.open_stream] or the connection's
/// [code]stream_opened[/code] signal. Both ends can write, whichever opened
/// it. A stream is usable the moment it is opened — but the other peer only
/// learns it exists when its first bytes arrive.
///
/// Reading works like Godot's TCP stream: [code]get_data[/code] and the
/// helpers built on it — [code]get_string[/code], [code]get_var[/code], the
/// [code]get_u8[/code] family — wait for the bytes they ask for, with no
/// timeout. A peer that never sends looks like a freeze, not an error. If
/// the stream ends before enough bytes exist, [code]get_data[/code] returns
/// [code]ERR_UNAVAILABLE[/code] with nothing consumed, and
/// [code]get_partial_data[/code] can still collect what did arrive. Code
/// that must never wait checks [code]get_available_bytes[/code] before
/// reading.
///
/// Writing never waits and is never partial: every put queues the bytes and
/// returns, and the queue has no limit. Only [code]put_data[/code] reports a
/// finished or dead stream — the typed puts return nothing, so their
/// failures are silent. OK means queued, not delivered.
///
/// A stream has no message boundaries of its own. Either open a stream per
/// message and let [method finish] mark the end, or keep one stream and
/// frame every message — [code]put_utf8_string[/code] and
/// [code]get_utf8_string[/code] are a matched pair for exactly that. Both
/// peers have to pick the same shape.
#[derive(GodotClass)]
// `no_init` because streams come from a connection, never from `new()`.
// `tool` for the same reason `IrohPeer` needs it: Godot instantiates extension
// classes in the editor too.
#[class(no_init, tool, base=StreamPeerExtension)]
pub struct IrohStream {
    /// Behind a `RefCell` because `get_available_bytes` is a `&self` method on
    /// `StreamPeer` and still has to pull from the network before answering.
    /// Reporting a stale count strands the last bytes of every stream.
    state: RefCell<State>,
    base: Base<StreamPeerExtension>,
}

struct State {
    stream: Option<Stream>,
    /// Read off the network, not yet handed to script.
    buffered: VecDeque<Bytes>,
    available: usize,
    /// Set once the remote stops sending. `None` inside means a clean finish.
    ended: Option<Option<String>>,
}

impl IrohStream {
    /// Wraps a stream from [`crate::raw`].
    pub(crate) fn wrap(stream: Stream) -> Gd<Self> {
        Gd::from_init_fn(|base| Self {
            state: RefCell::new(State {
                stream: Some(stream),
                buffered: VecDeque::new(),
                available: 0,
                ended: None,
            }),
            base,
        })
    }

    /// Pulls in whatever the reader task has produced, then hands back the state.
    ///
    /// Every entry point goes through here, so script always sees the newest
    /// picture without a per-frame tick of its own — `StreamPeer` is a polled
    /// interface anyway.
    fn drained(&self) -> RefMut<'_, State> {
        let mut state = self.state.borrow_mut();
        state.drain();
        state
    }
}

impl State {
    /// Moves whatever the reader task has produced into the buffer.
    fn drain(&mut self) {
        // Destructured so the reader and the buffer are borrowed separately.
        let Self {
            stream,
            buffered,
            available,
            ended,
        } = self;

        let Some(stream) = stream.as_mut() else {
            return;
        };

        while let Some(incoming) = stream.try_recv() {
            match incoming {
                Incoming::Data(chunk) => {
                    *available += chunk.len();
                    buffered.push_back(chunk);
                }
                Incoming::Ended(reason) => {
                    *ended = Some(reason);
                    break;
                }
            }
        }
    }

    /// Blocks until `wanted` bytes are buffered, the stream ends, or the
    /// stream is already gone.
    ///
    /// The reader task keeps delivering from the runtime while this thread
    /// waits, so the wait ends as soon as the network catches up — it cannot
    /// deadlock on itself.
    fn fill(&mut self, wanted: usize) {
        while self.available < wanted && self.ended.is_none() {
            let Some(stream) = self.stream.as_mut() else {
                return;
            };
            match stream.recv_blocking() {
                Some(Incoming::Data(chunk)) => {
                    self.available += chunk.len();
                    self.buffered.push_back(chunk);
                }
                Some(Incoming::Ended(reason)) => self.ended = Some(reason),
                // The carry task is gone without saying why; nothing more is
                // coming.
                None => self.ended = Some(Some("the stream's task ended".into())),
            }
        }
    }

    /// Fills `buffer` from the front of the queue, returning how much it took.
    fn take(&mut self, buffer: &mut [u8]) -> usize {
        let mut filled = 0;

        while filled < buffer.len() {
            let Some(chunk) = self.buffered.front_mut() else {
                break;
            };

            let take = chunk.len().min(buffer.len() - filled);
            buffer[filled..filled + take].copy_from_slice(&chunk[..take]);
            chunk.advance(take);
            if chunk.is_empty() {
                self.buffered.pop_front();
            }
            filled += take;
        }

        self.available -= filled;
        filled
    }
}

#[godot_api]
impl IStreamPeerExtension for IrohStream {
    /// Reads exactly `r_bytes`, waiting for them if they are still on their
    /// way.
    ///
    /// Waiting is `StreamPeer`'s contract, and every helper built on this —
    /// `get_string`, `get_var`, the `get_u8` family — leans on it: they read
    /// a length first and the bytes second, and a short read in between
    /// would wreck the stream's framing for good. Waiting here is safe
    /// because the reader task fills the buffer from the runtime, not from
    /// this thread. Use `get_partial_data` when waiting is not an option.
    unsafe fn get_data_rawptr(
        &mut self,
        r_buffer: RawPtr<*mut u8>,
        r_bytes: i32,
        r_received: RawPtr<*mut i32>,
    ) -> Error {
        let mut state = self.drained();

        let wanted = r_bytes.max(0) as usize;
        // SAFETY: Godot passes a valid out-pointer.
        unsafe { *r_received.ptr() = 0 };

        if wanted == 0 {
            return Error::OK;
        }
        state.fill(wanted);
        if state.available < wanted {
            // The stream ended before enough arrived; nothing was consumed.
            return Error::ERR_UNAVAILABLE;
        }

        // SAFETY: Godot guarantees `r_buffer` is writable for `r_bytes` bytes
        // for the duration of this call.
        let buffer = unsafe { std::slice::from_raw_parts_mut(r_buffer.ptr(), wanted) };
        let filled = state.take(buffer);

        // SAFETY: as above.
        unsafe { *r_received.ptr() = filled as i32 };
        Error::OK
    }

    /// Reads up to `r_bytes`, however few are ready.
    unsafe fn get_partial_data_rawptr(
        &mut self,
        r_buffer: RawPtr<*mut u8>,
        r_bytes: i32,
        r_received: RawPtr<*mut i32>,
    ) -> Error {
        let mut state = self.drained();

        let wanted = (r_bytes.max(0) as usize).min(state.available);
        // SAFETY: Godot passes a valid out-pointer.
        unsafe { *r_received.ptr() = 0 };

        if wanted == 0 {
            return Error::OK;
        }

        // SAFETY: `wanted` is at most `r_bytes`, which Godot guarantees is
        // writable for the duration of this call.
        let buffer = unsafe { std::slice::from_raw_parts_mut(r_buffer.ptr(), wanted) };
        let filled = state.take(buffer);

        // SAFETY: as above.
        unsafe { *r_received.ptr() = filled as i32 };
        Error::OK
    }

    /// Queues bytes for the remote. Never partial, so `r_sent` is all of them.
    unsafe fn put_data_rawptr(
        &mut self,
        p_data: RawPtr<*const u8>,
        p_bytes: i32,
        r_sent: RawPtr<*mut i32>,
    ) -> Error {
        // SAFETY: Godot passes a valid out-pointer.
        unsafe { *r_sent.ptr() = 0 };

        if p_bytes < 0 {
            return Error::ERR_INVALID_PARAMETER;
        }
        if p_bytes == 0 {
            return Error::OK;
        }

        // SAFETY: Godot guarantees `p_data` is readable for `p_bytes` bytes for
        // the duration of this call.
        let data = unsafe { std::slice::from_raw_parts(p_data.ptr(), p_bytes as usize) };

        let state = self.state.borrow();
        let Some(stream) = state.stream.as_ref() else {
            return Error::ERR_FILE_EOF;
        };
        if !stream.write(Bytes::copy_from_slice(data)) {
            return Error::ERR_CONNECTION_ERROR;
        }

        // SAFETY: as above.
        unsafe { *r_sent.ptr() = p_bytes };
        Error::OK
    }

    /// Same as [`Self::put_data_rawptr`]: the queue always takes everything.
    unsafe fn put_partial_data_rawptr(
        &mut self,
        p_data: RawPtr<*const u8>,
        p_bytes: i32,
        r_sent: RawPtr<*mut i32>,
    ) -> Error {
        // SAFETY: the caller's guarantees are identical.
        unsafe { self.put_data_rawptr(p_data, p_bytes, r_sent) }
    }

    fn get_available_bytes(&self) -> i32 {
        self.drained().available.min(i32::MAX as usize) as i32
    }
}

#[godot_api]
impl IrohStream {
    /// Whether the other peer might still send more.
    ///
    /// Goes false once they finish, the stream fails, or [method close] is
    /// called here. The end of a stream often lands together with its last
    /// bytes, and those stay readable — check [code]get_available_bytes[/code]
    /// before treating this as end of data.
    ///
    /// It never waits; it reports what has already arrived.
    #[func]
    fn is_open(&self) -> bool {
        let state = self.drained();
        state.ended.is_none() && state.stream.is_some()
    }

    /// Why the stream ended, or an empty string.
    ///
    /// Empty also just means "not ended yet", so pair it with
    /// [method is_open]. A clean [method finish] from the other peer stays
    /// empty; a failure or an abort leaves its reason here. Our own
    /// [method close] leaves it empty too — closing is not an error.
    #[func]
    fn get_error(&self) -> GString {
        match &self.drained().ended {
            Some(Some(reason)) => GString::from(reason),
            _ => GString::new(),
        }
    }

    /// Says we are done sending. Everything already written still goes out
    /// first.
    ///
    /// The other peer reads a clean end of stream: their [method is_open] goes
    /// false and their [method get_error] stays empty. Reading here carries on,
    /// so this is how you say "that is the whole request" and then wait for
    /// the reply.
    ///
    /// After [method close] it does nothing. Anything written after a finish
    /// never reaches the other peer.
    ///
    /// It never blocks, and it cannot be taken back.
    #[func]
    fn finish(&self) {
        if let Some(stream) = self.state.borrow().stream.as_ref() {
            stream.finish();
        }
    }

    /// Ends the stream in both directions at once and tells the other peer
    /// why.
    ///
    /// Where [method close] just drops the stream and leaves the other peer
    /// to notice, this carries `code` across, so a cancel done on purpose
    /// reads differently from a failure. What each code means is yours to
    /// define, and both peers have to agree on it.
    ///
    /// Returns false when the stream is already gone, and nothing is sent.
    #[func]
    fn abort(&self, code: i64) -> bool {
        let code = code.clamp(0, u32::MAX as i64) as u32;
        match self.state.borrow().stream.as_ref() {
            Some(stream) => stream.abort(code),
            None => false,
        }
    }

    /// Drops the stream and stops reading it.
    ///
    /// Bytes already written still go out, and the other peer reads the same
    /// clean end of stream that [method finish] gives them. What close
    /// abandons is our reading half: buffered bytes are discarded, and
    /// anything they send afterwards goes nowhere, with no error on their
    /// side.
    ///
    /// After it, reads return [code]ERR_UNAVAILABLE[/code] at once and puts
    /// report [code]ERR_FILE_EOF[/code]. It never tells the other peer why —
    /// [method abort] is the one that carries a reason.
    #[func]
    fn close(&self) {
        let mut state = self.state.borrow_mut();
        state.stream = None;
        state.buffered.clear();
        state.available = 0;
    }
}
