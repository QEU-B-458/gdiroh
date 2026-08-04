# gdiroh

Peer-to-peer networking for Godot 4, built on [iroh](https://www.iroh.computer/).

Godot's high-level multiplayer — RPCs, `MultiplayerSpawner`,
`MultiplayerSynchronizer` — runs directly between players over QUIC, with hole
punching and relay fallback handled for you. No game server, no STUN/TURN to
operate.

Beyond multiplayer, the same endpoint carries protocols of your own, a gossip
layer for presence and lobbies, content-addressed file transfer, and a
multi-writer key-value store. All of it reachable from GDScript.

---

## Status

Working and tested, but **not yet released**: CI has never run, and no platform
other than Linux x86_64 has been built or verified. See
[Known limitations](#known-limitations) before depending on it.

---

## Requirements

| | |
|---|---|
| Godot | 4.2+ declared; developed against **4.6**, currently exercised on **4.7.1-stable** |
| Rust | 1.96+ (edition 2024) |
| Platforms | everything Godot targets except web — iroh needs UDP sockets browsers do not expose |

---

## Install

Copy `addons/gdiroh/` into your project. The layout is:

```
addons/gdiroh/
    gdiroh.gdextension
    linux/x86_64/libgdiroh.so
    windows/x86_64/gdiroh.dll
    macos/libgdiroh.dylib
    android/arm64/libgdiroh.so
```

Restart Godot. Eight classes appear — `IrohEndpoint`, `IrohPeer`, the objects
their methods hand back, and `IrohPuppy`, the mascot, whose construction is
the cheapest proof the native library loaded. Nothing autoloads, nothing runs
at startup, and no global names are taken: gdiroh costs nothing until you
construct an endpoint and bind it.

> `.gdextension` files are parsed by Godot's `ConfigFile`, which accepts `;`
> comments and **not** `#`. A `#` comment silently breaks the file and Godot
> reports "No GDExtension library found for current OS and architecture", which
> points nowhere near the real cause.

---

## Quickstart

Everything starts with an endpoint — your identity and your reachable address.
Construct one and **keep it in a member variable**: an endpoint is reference
counted and closes when its last reference goes, so one held only in a local
dies when the function returns. gdiroh never writes your identity to disk —
generate a key, store it wherever suits your game, hand it back.

```gdscript
var endpoint: IrohEndpoint

func _ready() -> void:
    endpoint = IrohEndpoint.new()
    endpoint.bound.connect(_on_bound)
    endpoint.set_secret_key(_load_or_create_key())
    endpoint.bind()

func _on_bound(endpoint_id: String) -> void:
    print("listening as ", endpoint_id)
```

A game can hold several endpoints — separate identities, or separate stores.
Every other object below comes from an endpoint's methods and belongs to that
endpoint.

### Multiplayer

```gdscript
# Host
var peer := IrohPeer.new()
peer.host(endpoint)
multiplayer.multiplayer_peer = peer

# Join, by endpoint id or by ticket
var peer := IrohPeer.new()
peer.join(endpoint, host_endpoint_id)        # needs a lookup service
peer.join_ticket(endpoint, host_ticket)      # carries addresses, works on a closed network
multiplayer.multiplayer_peer = peer
```

RPCs and the rest of Godot's multiplayer work unchanged from here.

`IrohPeer` is a `MultiplayerPeer`, which is itself a `PacketPeer` — so it also
works bare, never assigned to `multiplayer.multiplayer_peer`: address peers
with `set_target_peer` (`0` is everyone, a negative id is everyone but that
one), send with `put_packet`, and compose typed payloads in a
`StreamPeerBuffer`, where the `put_u8`/`put_float` family lives. Going bare
means `poll()` is yours to call every frame, and the peer must not also be
driven by a `MultiplayerAPI` — that API consumes every incoming packet as its
own protocol. `samples/packets.gd` walks the whole bare cycle.

### A protocol of your own

For anything `MultiplayerAPI` is the wrong shape for — file transfer, a voice
channel, a lobby query.

```gdscript
# Accept
endpoint.listen("mygame/chat/1")
endpoint.connection_received.connect(func(alpn, conn):
    conn.stream_opened.connect(_on_stream))

# Dial
var conn := endpoint.connect_to(peer_id, "mygame/chat/1")
conn.opened.connect(func():
    var stream := conn.open_stream()
    stream.put_utf8_string("hello")
    stream.finish())
```

`IrohStream` is a Godot `StreamPeer`, so `get_data`, `put_var`, `get_u8` and the
rest work as they do anywhere else.

A stream is a pipe of bytes with no message boundaries of its own, so pick one
of two shapes. A fresh stream per exchange — the snippet above — lets the
stream's end mark the message's end, with no framing at all; this is how the
web does it, and streams cost almost nothing. Or keep one long-lived stream
and frame each message with `put_utf8_string`'s length header, reading them
back whole with `get_utf8_string`. The example's protocol tab shows the
long-lived shape, and `samples/stream.gd` walks it end to end.

### Datagrams

A datagram is a single packet on a connection: unreliable, unordered, never
split up, and dropped outright when larger than `max_datagram_size()`. That
makes it wrong for chat and right for positions — a lost position is replaced
by the next one a tick later, and nobody wants a stale one retransmitted.
This is the path a game's movement data belongs on.

```gdscript
var link := endpoint.connect_to_ticket(their_ticket, "mygame/move/1")

func _on_tick() -> void:                     # your send rate, not the frame rate
    var packet := PackedByteArray()
    packet.resize(8)
    packet.encode_float(0, position.x)
    packet.encode_float(4, position.y)
    link.send_datagram(packet)

link.datagram_received.connect(func(data):
    them.position = Vector2(data.decode_float(0), data.decode_float(4)))
```

Datagrams travel on any open connection, whichever side dialled. Use a stream
on the same connection for anything that must arrive.

### Gossip

Every peer on a topic receives every message, relayed peer to peer.

```gdscript
var lobby := endpoint.subscribe("mygame/lobby", [known_peer_id])
lobby.message.connect(func(data, from, _direct):
    print(from, ": ", data.get_string_from_utf8()))
lobby.broadcast("anyone there?".to_utf8_buffer())
```

### Blobs

Content-addressed transfer. A blob is named by the hash of its bytes, so a peer
that already has it skips the transfer and an interrupted one resumes.

```gdscript
# Publish
var add := endpoint.add_file("user://level.dat", "my-level")
add.completed.connect(func(hash, _data):
    share(endpoint.blob_ticket(hash)))

# Fetch
var get := endpoint.fetch_blob_ticket(ticket, "my-level")
get.progress.connect(func(done, total): bar.value = float(done) / total)
get.completed.connect(func(hash, _data):
    endpoint.export_blob(hash, "user://downloaded.dat"))
```

The second argument is a **tag**. See [Blob storage](#blob-storage) — it decides
whether the blob is kept.

### Documents

A key-value store several peers can write to at once.

```gdscript
var world := endpoint.create_document()
world.opened.connect(func(_id): world.share(true))
world.shared.connect(func(ticket): tell_players(ticket))
world.entry.connect(func(key, _author, _hash, _len, from):
    if not from.is_empty():
        world.read(key))

world.set("spawn", "north gate".to_utf8_buffer())
```

Conflicting writes resolve by timestamp — last write wins. Good for world edits,
inventories and settings; not a transaction.

---

## Building from source

```sh
# Build (documentation for the Godot editor is on by default)
cargo build --manifest-path gdiroh/Cargo.toml --release

# Stage into the example project. The rename matters: overwriting a .so that
# a running Godot has mapped crashes it with a bus error, while a renamed
# file leaves the old inode alive until that copy exits.
cp gdiroh/target/release/libgdiroh.so \
   godot/gdiroh-example/addons/gdiroh/linux/x86_64/libgdiroh.so.next
mv godot/gdiroh-example/addons/gdiroh/linux/x86_64/libgdiroh.so.next \
   godot/gdiroh-example/addons/gdiroh/linux/x86_64/libgdiroh.so
```

### Feature flags

| Feature | Default | Effect |
|---|---|---|
| `editor-docs` | on | Registers doc comments into Godot's built-in help |

Build shipped games with `--no-default-features`. Godot only reads the docs at
editor init, so an exported build carries them for nothing — and gdext logs a
startup warning saying so.

```sh
cargo build --manifest-path gdiroh/Cargo.toml --release --no-default-features
```

### Android

```sh
cargo install cargo-ndk
cargo ndk -t aarch64-linux-android -p 21 build --release
```

---

## Testing

```sh
cargo test  --manifest-path gdiroh/Cargo.toml     # 46 transport tests, no Godot needed
cargo clippy --manifest-path gdiroh/Cargo.toml --all-targets -- -D warnings
cargo fmt   --manifest-path gdiroh/Cargo.toml --all -- --check
```

The transport tests run two endpoints in one process and cover the handshake,
framing, ordering, gossip delivery, blob transfer and document convergence. They
need no editor, so they run in CI on every target.

**Anything touching the Godot layer needs the example**, because unit tests
cannot reach it:

```sh
cd godot/gdiroh-example

./two-peers.sh              # two windows; the scripted run drives the tabs
./two-peers.sh --manual     # two idle windows, drive them yourself
./two-peers.sh --headless   # no windows, run 36 checks, exit non-zero on failure
./two-peers.sh --build      # rebuild and stage the library first
./two-peers.sh --keep       # keep the logs and print where

GODOT=/path/to/godot ./two-peers.sh
```

The scripted run works by conducting: each copy watches a cue file, and the
script appends a line when it is time for that peer's next step. A cue calls
the same functions the tab buttons call, so what gets tested is the code a
person clicking around runs — tickets travel the way a person would take
them, read from one peer's output and pasted into the other's cue.

Output from both copies is interleaved and prefixed. Two things the script
handles that catch people out by hand: Godot block-buffers stdout when it is
not a terminal (so a captured run loses everything), and both copies share
`user://`, so without `--profile` they load the same keys and come up as the
same peers.

### The example

One app whose tabs are use cases — **session** (Godot's multiplayer),
**movement** (positions over datagrams), **chat** (gossip), **assets**
(blobs), **world** (documents), **protocol** (an ALPN of your own) and
**diagnose** (the endpoint's own reachability readouts). Every
tab owns an endpoint of its own: Start builds and binds it, Stop drops the
reference, and dropping the reference is what closes it. Run two tabs at once
and you are running two independent endpoints in one process; stop one and
the others do not notice. The network runtime itself stops when the last
endpoint goes and comes back for the next one — the headless run checks both.

Each tab's script under `tabs/` is one use case written to be lifted into a
game. The endpoint construction itself — `IrohEndpoint.new()`, the identity
key, `bind()` — lives once, in `common/use_case_tab.gd`, because it is the
same for every tab; each tab's doc comment says so, and the
[Quickstart](#quickstart) above shows the same lines standalone. Ids, tickets
and hashes all sit in fields with a **Copy** button, so joining from a second
machine is paste-driven.

Every tab also has a **Random id** toggle beside Start, for copies launched
without `--profile`. The editor's multi-instance run starts copies exactly
like that, and without one of the two, every copy loads the same saved keys
and comes up as the same peer.

Arguments go after a bare `--`, which separates Godot's options from the
game's.

| Argument | Effect |
|---|---|
| `--profile <name>` | identity slot, so two copies on one machine stay apart |
| `--random` | throwaway identities instead of saved ones |
| `--local` | find peers on this network over mDNS |
| `--no-dns` | do not resolve ids through n0's DNS |
| `--no-relay` | refuse relays, direct paths only |
| `--demo` | follow cues from `two-peers.sh` (needs `--cues`) |
| `--cues <file>` | the cue file the script appends to |

`--local --no-dns --no-relay` together is how you prove mDNS alone can resolve
a bare endpoint id.

```sh
godot --path . -- --profile alice
godot --path . -- --profile bob
```

The command line handling can be switched off in the inspector
(`allow_command_line`), so a game built on the example can force its own flow.

### The samples

The tabs show use cases wired to a UI; `samples/` shows the same cycles with
nothing else in the way. Each sample is one plain script that plays both peers
in one process — endpoint to teardown, nothing to paste, no second window:

| Sample | What it walks through |
|---|---|
| `stream.gd` | one long-lived stream carrying a framed conversation |
| `datagrams.gd` | positions ticking across as datagrams |
| `gossip.gd` | subscribe, bootstrap, broadcast, receive |
| `session.gd` | Godot's multiplayer and an RPC over an `IrohPeer` |
| `packets.gd` | bare `put_packet`/`get_packet` on an `IrohPeer`, no MultiplayerAPI |

Attach one to any node and press play, or run it headless:

```sh
cd godot/gdiroh-example
godot --headless --path . -s samples/_run.gd -- stream.gd
```

The headless smoke run executes every one of them, so they stay true.

---

## Blob storage

Blobs live in memory by default. To keep them between sessions:

```gdscript
endpoint.set_blob_store_path("user://blobs")   # before the first blob call
```

The store belongs to the endpoint. If you run several endpoints, give each its
own path — an on-disk store holds an exclusive database lock, and two endpoints
pointed at one directory will fight over it.

**Tags decide what survives.** `iroh-blobs` deletes blobs only through garbage
collection, and collection treats named tags as the roots — everything unnamed is
swept. Collection is off in this build, so nothing is deleted today, but that
cuts both ways:

- **Untagged blobs** are protected only while being imported. Turn collection on
  and they go.
- **Nothing can be freed** without tags either, since removing the last tag is
  what makes a blob collectable. An on-disk store otherwise grows forever.

So pass a tag for anything worth keeping, and `untag_blob()` when it is not.

```gdscript
endpoint.tag_blob(hash, "level-3")
endpoint.untag_blob("level-3")
endpoint.request_tag_list()          # answered by the tag_list signal
```

---

## Design notes

**Endpoints are objects, not a service.** An endpoint is a `RefCounted` you
construct, hold and release like anything else in Godot; closing is what happens
when the last reference goes. gdiroh takes no autoload, no engine singleton and
no `multiplayer` slot — it assumes nothing about being the only networking in
the project.

**One endpoint, many protocols.** `Endpoint::accept()` can only be consumed
once, so each endpoint owns a single accept loop and routes inbound connections
by ALPN. Multiplayer is one protocol among several — a session, ALPNs of your
own, gossip, blobs and documents can all share one endpoint.

**Everything is polled onto the main thread.** Network work happens on a Tokio
runtime and reaches Godot through channels drained on `SceneTree`'s
`process_frame`. Godot objects are never touched from a runtime thread.

**Nothing starts until asked.** The runtime itself begins at the first `bind()`
and stops when the last endpoint closes — a project that ships the addon but
never networks runs zero gdiroh threads. Within an endpoint the same rule
repeats: a game that never gossips pays nothing for gossip, and likewise blobs
and documents. Documents build on the blob store and the gossip swarm rather
than opening their own.

**Asking is a signal, not a return value.** Anything that needs the network
answers through a signal — `request_blob_status` → `blob_status`,
`IrohDocument.read` → `value`. Nothing blocks the main thread, with one
deliberate exception: `IrohStream.get_data` waits for the bytes it was asked
for, because being a `StreamPeer` means honouring the contract Godot's
`get_string` and `get_var` helpers are built on. Code that must never wait
polls `get_available_bytes` and reads with `get_partial_data`, the way the
example does.

---

## Known limitations

- **CI has never run.** No platform other than Linux x86_64 has been built or
  verified. Android, Windows, macOS and Linux arm64 are configured but unproven.
- **iOS is not supported.** It needs a `.framework` or static library rather than
  the plain dylib layout, which is its own piece of work.
- **Some of the GDScript surface is unexercised.** The transport underneath is
  tested, and the example drives 111 of the 134 registered items in a real
  engine; the remainder is mostly reopening stored documents across sessions
  and by-hash variants of paths the ticket calls already cover. Unproven, not
  known-broken.
- **`set_custom_relays` is verified only to URL-parsing depth.** Nothing has
  routed through a custom relay.
- **Reopening an on-disk blob store within one process may fail** while the
  previous one still holds its database lock. The same lock is why two endpoints
  must not share a store path. A game opens one store once, so this has not been
  worth solving.

---

## Licence

MIT. See `LICENSE`.
