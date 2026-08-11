#!/usr/bin/env bash
#
# runs bench.gd as two real processes per transport (stream, stream_blocking,
# datagram, correctness_stream, correctness_datagram) and appends every
# RESULT/CASE line to results.log with a timestamp and the gdiroh commit, so
# a run before a fix and a run after it can be diffed.
#
#   ./run_bench.sh [iterations] [payload_size_bytes]

set -euo pipefail

bench_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
example_dir=$(cd -- "$bench_dir/.." && pwd)
repo_root=$(cd -- "$example_dir/../.." && pwd)
godot=${GODOT:-godot}
iterations=${1:-300}
size=${2:-64}
log_file="$bench_dir/results.log"

logs=$(mktemp -d)
pids=()

cleanup() {
	for pid in ${pids+"${pids[@]}"}; do
		kill "$pid" 2>/dev/null || true
	done
	sleep 0.2
	for pid in ${pids+"${pids[@]}"}; do
		kill -9 "$pid" 2>/dev/null || true
	done
	wait 2>/dev/null || true
	rm -rf "$logs"
}
trap cleanup EXIT

# godot's stdout is block-buffered when it isn't a terminal, so a run whose
# output is only captured to a file loses it until exit — a pty via `script`
# is what keeps it flushed while we're still trying to scrape the ticket out.
spawn() {
	local logfile=$1
	shift
	local command
	command=$(printf '%q ' "$godot" --headless --path "$example_dir" -s tmp_latency_bench/bench.gd -- "$@")
	script -qec "$command" /dev/null >"$logfile" 2>&1 &
	pids+=($!)
}

await_line() {
	local logfile=$1 seconds=$2 pattern=$3
	for _ in $(seq $((seconds * 10))); do
		grep -q -- "$pattern" "$logfile" 2>/dev/null && return 0
		sleep 0.1
	done
	return 1
}

run_one() {
	local transport=$1 n=$2 sz=$3
	local host_log="$logs/host_$transport.log"
	local join_log="$logs/join_$transport.log"

	spawn "$host_log" host "$transport" "" "$n" "$sz"

	if ! await_line "$host_log" 15 '^TICKET '; then
		echo "$transport: host never printed a ticket" >&2
		cat "$host_log" >&2
		return 1
	fi
	local ticket
	ticket=$(sed -n 's/^TICKET //p' "$host_log" | head -1)

	spawn "$join_log" join "$transport" "$ticket" "$n" "$sz"

	if ! await_line "$join_log" 90 '^RESULT '; then
		echo "$transport: join never printed a result" >&2
		cat "$join_log" >&2
		return 1
	fi
	grep -E '^(RESULT|CASE) ' "$join_log"
}

commit=$(git -C "$repo_root" rev-parse --short HEAD 2>/dev/null || echo unknown)
dirty=$(git -C "$repo_root" diff --quiet 2>/dev/null && echo clean || echo dirty)
stamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

{
	echo "=== $stamp commit=$commit ($dirty) iterations=$iterations size=${size}b ==="
	for transport in stream stream_blocking datagram datagram_blocking correctness_stream correctness_datagram; do
		run_one "$transport" "$iterations" "$size"
	done
} | tee -a "$log_file"

echo
echo "appended to $log_file"
