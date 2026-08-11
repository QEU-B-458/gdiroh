#!/usr/bin/env bash
#
# Runs two copies of the example against each other, side by side, so you can
# click around in both.
#
#     ./two-peers.sh              two windows, the scripted run drives the tabs
#     ./two-peers.sh --manual     two windows, nothing automatic — you drive
#     ./two-peers.sh --headless   no windows, run the checks, exit non-zero on
#                                 failure (this is the smoke test)
#
#     ./two-peers.sh --build      rebuild the library and stage it first
#     ./two-peers.sh --keep       leave the logs behind and print where
#     ./two-peers.sh --size 800x900
#
#     GODOT=/path/to/godot4.6 ./two-peers.sh
#
# The scripted run works by conducting: each copy watches a cue file, and this
# script appends one line to it when it is time for that peer's next step. A
# cue calls the same functions the tab buttons call, so what gets tested is
# the code a person clicking around runs. Tickets travel the way a person
# would take them — read from one peer's output, pasted into the other's cue.
#
# Two things this handles that catch people out by hand:
#
#   * Godot's stdout is block-buffered when it is not a terminal, so a run whose
#     output is captured loses it entirely. Both copies run under `script`,
#     which gives them a pty.
#   * Both copies share `user://`, so without `--profile` they load the same
#     saved keys and come up as the same peers.

set -euo pipefail

example_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$example_dir/../.." && pwd)

godot=${GODOT:-}
mode=auto
build=0
keep=0
size=${GDIROH_WINDOW_SIZE:-700x820}

while (($#)); do
	case $1 in
	--manual) mode=manual ;;
	--headless) mode=headless ;;
	--build) build=1 ;;
	--keep) keep=1 ;;
	--size)
		size=${2:?--size needs a value like 700x820}
		shift
		;;
	-h | --help)
		sed -n '3,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
		exit 0
		;;
	*)
		echo "unknown option: $1" >&2
		exit 2
		;;
	esac
	shift
done

width=${size%x*}
height=${size#*x}

# --- prerequisites ------------------------------------------------------------

if [[ -z $godot ]]; then
	for candidate in godot4.6 godot4 godot "$HOME/.local/bin/godot4.6"; do
		if command -v "$candidate" >/dev/null 2>&1; then
			godot=$candidate
			break
		fi
	done
fi

if [[ -z $godot ]] || ! command -v "$godot" >/dev/null 2>&1; then
	echo "no Godot binary found; set GODOT=/path/to/godot" >&2
	exit 1
fi

library=$example_dir/addons/gdiroh/linux/x86_64/libgdiroh.so

if ((build)); then
	echo "building..."
	cargo build --manifest-path "$repo_root/gdiroh/Cargo.toml" --release
	# Staged with a rename, not an overwrite: a copy of the example already
	# running has the .so mapped, and truncating a mapped file is a bus error
	# waiting for a frame. A rename leaves the old inode alive until it exits.
	cp "$repo_root/gdiroh/target/release/libgdiroh.so" "$library.next"
	mv -f "$library.next" "$library"
fi

if [[ ! -f $library ]]; then
	echo "no staged library at $library; run with --build" >&2
	exit 1
fi

logs=$(mktemp -d)
alice_log=$logs/alice.log
bob_log=$logs/bob.log
alice_cues=$logs/alice.cues
bob_cues=$logs/bob.cues
pids=()

cleanup() {
	for pid in ${pids+"${pids[@]}"}; do
		kill "$pid" 2>/dev/null || true
	done
	# `script` can spin after its child exits, shrugging off the TERM above,
	# which would hang the wait below forever.
	sleep 0.3
	for pid in ${pids+"${pids[@]}"}; do
		kill -9 "$pid" 2>/dev/null || true
	done
	wait 2>/dev/null || true
	if ((keep)); then
		echo
		echo "logs kept in $logs"
	else
		rm -rf "$logs"
	fi
}
trap cleanup EXIT

if [[ -t 1 ]]; then
	alice_colour=$'\033[36m' bob_colour=$'\033[35m' plain=$'\033[0m'
else
	alice_colour= bob_colour= plain=
fi

# --- helpers ------------------------------------------------------------------

# Starts one copy under a pty, logging to a file and echoing here with a prefix.
#
# `$!` is the pty process, so killing it takes Godot with it — which would not
# be true of a plain pipeline, where `$!` is the last stage instead.
spawn() {
	local logfile=$1 prefix=$2 x=$3
	shift 3

	local -a options=(--headless)
	if [[ $mode != headless ]]; then
		# Wayland ignores an app positioning itself, so under it the two windows
		# land wherever the compositor decides. X11 honours this.
		options=(--windowed --resolution "$size" --position "$x,80")
	fi

	local command
	command=$(printf '%q ' "$godot" "${options[@]}" --path "$example_dir" -- "$@")

	script -qec "$command" /dev/null \
		> >(tee "$logfile" | sed -u "s/^/$prefix/") 2>&1 &
	pids+=($!)
}

# Appends one cue line to a peer's cue file. The app performs it on its next
# poll by calling the same function the matching button calls.
cue() {
	local file=$1
	shift
	printf '%s\n' "$*" >>"$file"
}

# Waits up to `$2` seconds for a line matching `$3` to appear in `$1`.
await() {
	local logfile=$1 seconds=$2 pattern=$3
	for _ in $(seq $((seconds * 4))); do
		if grep -qE -- "$pattern" "$logfile" 2>/dev/null; then
			return 0
		fi
		sleep 0.25
	done
	return 1
}

# Last match of a pattern in a log, printed whole.
pluck() {
	grep -oE -- "$2" "$1" 2>/dev/null | tail -1
}

# Waits for a pattern and prints the last field of the match — how tickets get
# from one peer's log into the other peer's cues.
harvest() {
	local logfile=$1 seconds=$2 pattern=$3
	await "$logfile" "$seconds" "$pattern" || return 1
	pluck "$logfile" "$pattern" | awk '{print $NF}'
}

failures=0

expect() {
	local label=$1 logfile=$2 pattern=$3
	if grep -qE -- "$pattern" "$logfile" 2>/dev/null; then
		echo "  ok    $label"
	else
		echo "  FAIL  $label"
		failures=$((failures + 1))
	fi
}

verdict() {
	local label=$1 outcome=$2
	if ((outcome)); then
		echo "  ok    $label"
	else
		echo "  FAIL  $label"
		failures=$((failures + 1))
	fi
}

# Runs one single-file sample. A sample plays both peers inside one process,
# so it needs no window and no partner — the log is read by the checks below.
run_sample() {
	local name=$1
	echo "— sample $name"
	timeout 90 "$godot" --headless --path "$example_dir" -s samples/_run.gd -- "$name.gd" \
		>"$logs/sample_$name.log" 2>&1 || true
}

# --- the conducted sequence ---------------------------------------------------

# Starts a tab on both peers and waits for both endpoints to bind.
open_tab() {
	local slug=$1
	cue "$alice_cues" "$slug" start
	cue "$bob_cues" "$slug" start
	await "$alice_log" 45 "\[$slug\] listening as" || true
	await "$bob_log" 45 "\[$slug\] listening as" || true
}

close_tab() {
	local slug=$1
	cue "$alice_cues" "$slug" stop
	cue "$bob_cues" "$slug" stop
	await "$alice_log" 15 "\[$slug\] endpoint released" || true
	await "$bob_log" 15 "\[$slug\] endpoint released" || true
}

conduct() {
	local ticket own

	# Session: Godot's multiplayer over gdiroh, then torn down again.
	echo "— session"
	open_tab session
	cue "$alice_cues" session host
	# The host has to be hosting before anyone dials in — a dial to a protocol
	# nobody has claimed yet is refused, not queued.
	await "$alice_log" 15 '\[session\] hosting as peer 1' || true
	ticket=$(harvest "$alice_log" 15 '\[session\] ticket \S+' || true)
	# The classic two-windows paste mistake, made on purpose: bob offers his
	# own ticket first, and the tab must say so rather than dial.
	own=$(harvest "$bob_log" 15 '\[session\] ticket \S+' || true)
	cue "$bob_cues" session join "$own"
	await "$bob_log" 15 "window's own ticket" || true
	cue "$bob_cues" session join "$ticket"
	await "$alice_log" 90 '\[session\] peer 2 connected' || true
	cue "$alice_cues" session say hi from alice
	await "$bob_log" 30 '\[session\] peer 1: hi from alice' || true
	# the same rpc as a datagram: reconfigured to unreliable, said three
	# times because unreliable means exactly that
	cue "$alice_cues" session mode unreliable
	cue "$alice_cues" session say fast hi
	cue "$alice_cues" session say fast hi
	cue "$alice_cues" session say fast hi
	await "$bob_log" 30 '\[session\] peer 1: fast hi' || true
	close_tab session

	# With the last endpoint gone the runtime stops; the next tab restarts it.
	await "$alice_log" 30 'network runtime stopped' || true

	# Movement: positions as datagrams, flowing both ways over one link.
	echo "— movement"
	open_tab move
	ticket=$(harvest "$alice_log" 15 '\[move\] ticket \S+' || true)
	cue "$bob_cues" move join "$ticket"
	await "$alice_log" 60 '\[move\] positions flowing' || true
	await "$bob_log" 60 '\[move\] positions flowing' || true
	close_tab move

	# Chat: a gossip room, deliberately kept open through the next tab.
	echo "— chat"
	open_tab chat
	cue "$alice_cues" chat host
	# Same reason as the session: alice must be subscribed before bob
	# bootstraps off her.
	await "$alice_log" 15 "\[chat\] subscribed" || true
	ticket=$(harvest "$alice_log" 15 '\[chat\] ticket \S+' || true)
	cue "$bob_cues" chat join "$ticket"
	await "$alice_log" 60 '\[chat\] neighbour up' || true
	cue "$alice_cues" chat say hi from alice
	await "$bob_log" 30 '\[chat\] .*: hi from alice' || true

	# Assets, while chat is still up: two endpoints per process right now.
	echo "— assets (chat stays up)"
	open_tab assets
	cue "$alice_cues" assets publish
	ticket=$(harvest "$alice_log" 30 '\[assets\] blob_ticket \S+' || true)
	cue "$bob_cues" assets fetch "$ticket"
	await "$bob_log" 90 '\[assets\] status .* complete=true' || true
	# the store operations, on alice's side: a real file in, the blob back
	# out, then the listings and the tag lifecycle
	cue "$alice_cues" assets file
	await "$alice_log" 30 '\[assets\] imported' || true
	cue "$alice_cues" assets export
	await "$alice_log" 30 '\[assets\] exported' || true
	cue "$alice_cues" assets list
	cue "$alice_cues" assets tags
	await "$alice_log" 15 '\[assets\] tags \(' || true
	cue "$alice_cues" assets untag
	await "$alice_log" 15 '\[assets\] removed tag' || true
	close_tab assets

	# Chat must have survived assets closing — its endpoint was its own.
	cue "$alice_cues" chat say still here
	await "$bob_log" 30 '\[chat\] .*: still here' || true
	# neighbours-only reaches bob too: he is directly attached
	cue "$alice_cues" chat saynear direct hello
	await "$bob_log" 30 '\[chat\] .*: direct hello' || true
	close_tab chat

	# World: a shared document, written before the joiner arrives.
	echo "— world"
	open_tab world
	cue "$alice_cues" world create
	ticket=$(harvest "$alice_log" 30 '\[world\] doc_ticket \S+' || true)
	cue "$alice_cues" world set spawn north gate
	cue "$bob_cues" world join "$ticket"
	await "$bob_log" 90 "\[world\] 'spawn' = 'north gate'" || true
	# the queries, then a deletion — prefix-wide on purpose
	cue "$alice_cues" world status
	await "$alice_log" 15 '\[world\] syncing:' || true
	cue "$alice_cues" world authors
	await "$alice_log" 15 '\[world\] authors \(' || true
	cue "$alice_cues" world del spawn
	await "$alice_log" 15 "\[world\] deleted everything under 'spawn'" || true
	close_tab world

	# Protocol: bob shouts down alice's custom ALPN and reads the replies —
	# two shouts through the one long-lived conversation stream.
	echo "— protocol"
	open_tab protocol
	ticket=$(harvest "$alice_log" 15 '\[protocol\] ticket \S+' || true)
	cue "$bob_cues" protocol join "$ticket"
	await "$bob_log" 60 '\[protocol\] linked with' || true
	cue "$bob_cues" protocol say hello from bob
	await "$bob_log" 45 '\[protocol\] reply "HELLO FROM BOB"' || true
	cue "$bob_cues" protocol say still the same stream
	await "$bob_log" 45 '\[protocol\] reply "STILL THE SAME STREAM"' || true
	close_tab protocol

	# Diagnose: one peer reading its own reachability back.
	echo "— diagnose"
	cue "$alice_cues" diagnose start
	await "$alice_log" 45 '\[diagnose\] listening as' || true
	cue "$alice_cues" diagnose refresh
	await "$alice_log" 15 '\[diagnose\] addresses:' || true
	cue "$alice_cues" diagnose metrics
	await "$alice_log" 15 '\[diagnose\] .*counters\)' || true
	cue "$alice_cues" diagnose stop
	await "$alice_log" 15 '\[diagnose\] endpoint released' || true
}

# --- run ----------------------------------------------------------------------

echo "godot:   $godot"
echo "library: $(date -r "$library" '+%Y-%m-%d %H:%M') ($(du -h "$library" | cut -f1))"
echo "mode:    $mode"
echo

alice_prefix="${alice_colour}[alice]${plain} "
bob_prefix="${bob_colour}[bob]${plain}   "
bob_x=$((60 + width + 20))

if [[ $mode == manual ]]; then
	# No arguments at all, so both come up idle with their buttons live. Copy
	# tickets from one window's tabs into the other's.
	spawn "$alice_log" "$alice_prefix" 60 --profile alice
	spawn "$bob_log" "$bob_prefix" "$bob_x" --profile bob

	echo "close them to finish."
	wait
	exit 0
fi

: >"$alice_cues"
: >"$bob_cues"

spawn "$alice_log" "$alice_prefix" 60 --demo --cues "$alice_cues" --profile alice
spawn "$bob_log" "$bob_prefix" "$bob_x" --demo --cues "$bob_cues" --profile bob

if ! await "$alice_log" 45 'mascot happy: true'; then
	echo "alice never came up." >&2
	exit 1
fi
if ! await "$bob_log" 45 'mascot happy: true'; then
	echo "bob never came up." >&2
	exit 1
fi

conduct

if [[ $mode != headless ]]; then
	echo
	echo "the scripted run is done — both windows are yours. close them to finish."
	wait
	exit 0
fi

cue "$alice_cues" quit
cue "$bob_cues" quit
sleep 2

# The single-file samples, each a complete cycle on its own.
run_sample stream
run_sample datagrams
run_sample gossip
run_sample session
run_sample packets

echo
echo "checks:"
expect "mascot barked"             "$alice_log" 'mascot happy: true'
expect "own ticket refused"        "$bob_log" "\[session\] that is this window's own ticket"
expect "session connected"         "$alice_log" '\[session\] peer 2 connected, endpoint [0-9a-f]{12}'
expect "rpc crossed"               "$bob_log" '\[session\] peer 1: hi from alice'
expect "unreliable rpc arrived"    "$bob_log" '\[session\] peer 1: fast hi'
expect "runtime stopped when idle" "$alice_log" 'network runtime stopped'
expect "positions flowed to bob"   "$bob_log" '\[move\] positions flowing'
expect "positions flowed to alice" "$alice_log" '\[move\] positions flowing'
expect "chat neighbour found"      "$alice_log" '\[chat\] neighbour up'
expect "chat delivered"            "$bob_log" '\[chat\] [0-9a-f]{12}.*: hi from alice'
expect "asset published"           "$alice_log" '\[assets\] published [0-9a-f]{12}'
expect "asset fetched whole"       "$bob_log" '\[assets\] status .* complete=true size=262144'
expect "file imported"             "$alice_log" '\[assets\] imported [0-9a-f]{12}'
expect "blob exported"             "$alice_log" '\[assets\] exported [0-9a-f]{12}'
expect "store listed"              "$alice_log" '\[assets\] store holds [0-9]+ blob'
expect "tags listed"               "$alice_log" '\[assets\] tags \([0-9]+\)'
expect "tag removed"               "$alice_log" '\[assets\] removed tag'
expect "chat outlived assets"      "$bob_log" '\[chat\] [0-9a-f]{12}.*: still here'
expect "neighbours-only delivered" "$bob_log" '\[chat\] [0-9a-f]{12}.*: direct hello'
expect "world value synced"        "$bob_log" "\[world\] 'spawn' = 'north gate'"
expect "world status answered"     "$alice_log" '\[world\] syncing: '
expect "authors answered"          "$alice_log" '\[world\] authors \([0-9]+\)'
expect "world key deleted"         "$alice_log" "\[world\] deleted everything under 'spawn'"
expect "protocol served"           "$alice_log" '\[protocol\] shouted back "HELLO FROM BOB"'
expect "protocol reply intact"     "$bob_log" '\[protocol\] reply "HELLO FROM BOB"'
expect "protocol stream persisted" "$bob_log" '\[protocol\] reply "STILL THE SAME STREAM"'
expect "addresses read back"       "$alice_log" '\[diagnose\] addresses: '
expect "metrics read back"         "$alice_log" '\[diagnose\] .* \([0-9]+ counters\)'
expect "endpoints released"        "$alice_log" '\[diagnose\] endpoint released'
expect "sample stream conversed"   "$logs/sample_stream.log" 'reply: SECOND SHOUT, SAME STREAM'
expect "sample datagrams landed"   "$logs/sample_datagrams.log" 'alice caught [1-9][0-9]* of 30'
expect "sample gossip heard"       "$logs/sample_gossip.log" 'bob heard: the door creaks open'
expect "sample session rpc"        "$logs/sample_session.log" 'alice heard over rpc: hello over rpc'
expect "sample packets delivered"  "$logs/sample_packets.log" 'alice got, from peer 2: packed by hand'

# Every tab binds an identity of its own, so two of alice's tabs must never
# report the same id.
session_id=$(pluck "$alice_log" '\[session\] listening as [0-9a-f]+' | awk '{print $NF}')
chat_id=$(pluck "$alice_log" '\[chat\] listening as [0-9a-f]+' | awk '{print $NF}')
verdict "tabs have identities of their own" \
	"$([[ -n $session_id && -n $chat_id && $session_id != "$chat_id" ]] && echo 1 || echo 0)"

# The runtime stops with the last endpoint and comes back for the next tab,
# so a full run starts it more than once.
verdict "runtime restarted for later tabs" \
	"$([[ $(grep -cE 'network runtime started' "$alice_log") -ge 2 ]] && echo 1 || echo 0)"

echo
if ((failures)); then
	echo "$failures check(s) failed"
	exit 1
fi
echo "all checks passed"
