extends SceneTree

## throwaway round-trip bench: latency (stream frame-polled, stream real
## blocking get_data, datagram) and correctness (arbitrary byte content,
## including edge cases) — each run as two separate headless processes, so a
## blocking call on one side can never freeze the other.
##
## usage:
##   godot --headless -s tmp_latency_bench/bench.gd -- host <transport> "" <n> <size>
##   godot --headless -s tmp_latency_bench/bench.gd -- join <transport> <ticket> <n> <size>
##
## transport is one of:
##   stream            - poll get_available_bytes() every frame
##   stream_blocking   - real blocking get_data()
##   datagram          - only option gdiroh offers for datagrams
##   correctness_stream, correctness_datagram
##     - a fixed list of edge-case payloads (empty, all-zero, all-0xff,
##       embedded null, a size right at the datagram cap, one byte over it,
##       and — stream only — a full megabyte) instead of n random ones.
##       every reply is compared byte-for-byte against what was sent.
##
## the host prints "TICKET <ticket>" once ready; the join side prints one or
## more "RESULT ..." lines when done, then both quit.

const ALPN := "gdiroh-bench/1"

var _endpoint: IrohEndpoint
var _connection: IrohConnection
var _arrived_at: Dictionary = {}
var _arrived_data: Dictionary = {}


func _initialize() -> void:
	_run()


func _run() -> void:
	var args := OS.get_cmdline_user_args()
	if args.size() < 2:
		push_error("usage: (host|join) (stream|stream_blocking|datagram|correctness_stream|correctness_datagram) [ticket] [n] [size]")
		quit(1)
		return

	var role := args[0]
	var transport := args[1]
	var ticket := args[2] if args.size() > 2 else ""
	var iterations := int(args[3]) if args.size() > 3 else 300
	var size := int(args[4]) if args.size() > 4 else 64

	_endpoint = IrohEndpoint.new()
	_endpoint.set_secret_key(IrohEndpoint.generate_secret_key())
	_endpoint.bind()
	await _endpoint.bound

	if role == "host":
		await _serve(transport, size)
	else:
		await _measure(transport, ticket, iterations, size)
	quit(0)


# --- host side: echoes back whatever it gets, as fast as its own transport allows -----


func _serve(transport: String, size: int) -> void:
	_endpoint.listen(ALPN)
	print("TICKET ", _endpoint.ticket())

	var received: Array = await _endpoint.connection_received
	_connection = received[1]

	match transport:
		"stream", "stream_blocking":
			var stream: IrohStream = await _connection.stream_opened
			var frame := 8 + size
			var blocking := transport == "stream_blocking"
			while true:
				var data: PackedByteArray
				if blocking:
					var result: Array = stream.get_data(frame)
					if result[0] != OK:
						break
					data = result[1]
				else:
					while stream.get_available_bytes() < frame and stream.is_open():
						await process_frame
					if stream.get_available_bytes() < frame:
						break
					data = (stream.get_data(frame) as Array)[1]
				stream.put_data(data)
		"stream_sync":
			# host is the poller here: it has to be the side that *receives*
			# a stream via stream_opened, which only fires once the opener
			# has actually written something — so the opener must be the
			# sender, not the poller (see _measure_stream_sync for the other
			# half of this)
			await _poll_stream_sync(await _connection.stream_opened)
		"datagram":
			_connection.datagram_received.connect(func(data: PackedByteArray) -> void:
				_connection.send_datagram(data))
			await _connection.closed
		"datagram_blocking":
			while _connection.is_open():
				var data := _connection.get_datagram()
				if _connection.is_open():
					_connection.send_datagram(data)
		"datagram_checked":
			# the actually-recommended shape: never wait, just ask once a
			# frame whether anything showed up
			while _connection.is_open():
				if _connection.get_available_datagram_count() > 0:
					_connection.send_datagram(_connection.get_datagram())
				else:
					await process_frame
		"datagram_blocking_paced":
			# an unprompted sender, at its own pace — like another player's
			# movement ticks, not a reply to anything we asked for
			for i in size:
				await create_timer(0.15).timeout
				_connection.send_datagram(_make_fixed_payload(i, 32))
			await create_timer(0.5).timeout
		"datagram_overhead":
			# fires the whole batch immediately, no waiting for replies — the
			# point is to have them all sitting queued, unread, before the
			# join side starts timing how long draining them costs
			for i in size:
				_connection.send_datagram(_make_fixed_payload(i, 64))
			await _connection.closed
		"correctness_stream":
			# variable-length messages, so a 4-byte length header comes first
			var stream: IrohStream = await _connection.stream_opened
			while true:
				while stream.get_available_bytes() < 4 and stream.is_open():
					await process_frame
				if stream.get_available_bytes() < 4:
					break
				var header: Array = stream.get_data(4)
				if header[0] != OK:
					break
				var length: int = (header[1] as PackedByteArray).decode_u32(0)
				while stream.get_available_bytes() < length and stream.is_open():
					await process_frame
				if stream.get_available_bytes() < length:
					break
				var body: PackedByteArray = (stream.get_data(length) as Array)[1]
				var reply := PackedByteArray()
				reply.resize(4)
				reply.encode_u32(0, length)
				reply.append_array(body)
				stream.put_data(reply)
		"correctness_datagram":
			_connection.datagram_received.connect(func(data: PackedByteArray) -> void:
				_connection.send_datagram(data))
			await _connection.closed


# --- join side: sends the payloads, times each one, verifies content -----------------


func _measure(transport: String, ticket: String, iterations: int, size: int) -> void:
	_connection = _endpoint.connect_to_ticket(ticket, ALPN)
	await _connection.opened

	match transport:
		"stream", "stream_blocking":
			await _measure_stream(transport == "stream_blocking", iterations, size)
		"stream_sync":
			await _send_stream_sync()
		"datagram":
			await _measure_datagram(iterations, size)
		"datagram_blocking":
			await _measure_datagram_blocking(iterations, size)
		"datagram_checked":
			await _measure_datagram_checked(iterations, size)
		"datagram_blocking_paced":
			await _measure_datagram_blocking_paced(iterations)
		"datagram_overhead":
			await _measure_datagram_overhead(size)
		"correctness_stream":
			await _correctness_stream()
		"correctness_datagram":
			await _correctness_datagram()

	_connection.close("bench done")


func _measure_stream(blocking: bool, iterations: int, size: int) -> void:
	var stream: IrohStream = _connection.open_stream()
	var frame := 8 + size
	var samples_us: Array = []
	var corrupt := 0
	for i in iterations:
		# fixed size here, not _make_payload's variable one: this transport's
		# wire format is "always exactly `frame` bytes", agreed with the host
		# up front — content is still genuinely random per iteration
		var payload := _make_fixed_payload(i, size)
		var t0 := Time.get_ticks_usec()
		stream.put_data(payload)
		var data: PackedByteArray
		if blocking:
			data = (stream.get_data(frame) as Array)[1]
		else:
			while stream.get_available_bytes() < frame:
				await process_frame
			data = (stream.get_data(frame) as Array)[1]
		samples_us.append(Time.get_ticks_usec() - t0)
		if data != payload:
			corrupt += 1
	stream.finish()
	_report_latency("stream_blocking" if blocking else "stream", size, samples_us, 0, corrupt)


## opens the stream (so it must also write first — a stream isn't visible
## to the other side until it does) and sends a sequential byte pattern in
## random-sized chunks with random real gaps between them
func _send_stream_sync() -> void:
	var stream: IrohStream = _connection.open_stream()
	var total := 20000
	var sent := 0
	while sent < total:
		var chunk_size: int = min(randi_range(1, 500), total - sent)
		var chunk := PackedByteArray()
		chunk.resize(chunk_size)
		for b in chunk_size:
			chunk[b] = (sent + b) % 256
		stream.put_data(chunk)
		sent += chunk_size
		await create_timer(randf_range(0.01, 0.08)).timeout
	stream.finish()


## polls get_available_bytes() as fast as this process can possibly loop —
## no await, no waiting for a frame — and checks every single claim against
## what get_data() actually hands back, both in count and in content. this
## is deliberately harder than any real game would ever poll, on purpose
func _poll_stream_sync(stream: IrohStream) -> void:
	var total_received := 0
	var polls_with_data := 0
	var polls_empty := 0
	var anomalies := 0
	var start_ms := Time.get_ticks_msec()
	while (stream.is_open() or stream.get_available_bytes() > 0) and Time.get_ticks_msec() - start_ms < 30000:
		var available := stream.get_available_bytes()
		if available > 0:
			var result: Array = stream.get_data(available)
			var ok: bool = result[0] == OK
			var data: PackedByteArray = result[1]
			var size_ok := ok and data.size() == available
			var content_ok := size_ok
			if size_ok:
				for b in data.size():
					if data[b] != (total_received + b) % 256:
						content_ok = false
						break
			if not size_ok or not content_ok:
				anomalies += 1
				print(
					"ANOMALY total_received=%d claimed_available=%d ok=%s got_size=%d content_ok=%s"
					% [total_received, available, ok, data.size(), content_ok]
				)
			total_received += data.size()
			polls_with_data += 1
		else:
			polls_empty += 1
	print(
		"RESULT transport=stream_sync total_received=%d polls_with_data=%d polls_empty=%d anomalies=%d"
		% [total_received, polls_with_data, polls_empty, anomalies]
	)


func _measure_datagram(iterations: int, size: int) -> void:
	_connection.datagram_received.connect(_on_datagram)
	var samples_us: Array = []
	var drops := 0
	var corrupt := 0
	for i in iterations:
		var payload := _make_payload(i, size)
		_arrived_at.erase(i)
		_arrived_data.erase(i)
		var t0 := Time.get_ticks_usec()
		_connection.send_datagram(payload)
		var waited := 0
		while not _arrived_at.has(i) and waited < 300:
			await process_frame
			waited += 1
		if _arrived_at.has(i):
			samples_us.append(_arrived_at[i] - t0)
			if _arrived_data[i] != payload:
				corrupt += 1
			_arrived_at.erase(i)
			_arrived_data.erase(i)
		else:
			drops += 1
	_report_latency("datagram", size, samples_us, drops, corrupt)


func _measure_datagram_blocking(iterations: int, size: int) -> void:
	var samples_us: Array = []
	var corrupt := 0
	for i in iterations:
		var payload := _make_payload(i, size)
		var t0 := Time.get_ticks_usec()
		_connection.send_datagram(payload)
		var data := _connection.get_datagram()
		samples_us.append(Time.get_ticks_usec() - t0)
		if data != payload:
			corrupt += 1
	_report_latency("datagram_blocking", size, samples_us, 0, corrupt)


func _measure_datagram_checked(iterations: int, size: int) -> void:
	var samples_us: Array = []
	var corrupt := 0
	for i in iterations:
		var payload := _make_payload(i, size)
		var t0 := Time.get_ticks_usec()
		_connection.send_datagram(payload)
		while _connection.get_available_datagram_count() == 0:
			await process_frame
		var data := _connection.get_datagram()
		samples_us.append(Time.get_ticks_usec() - t0)
		if data != payload:
			corrupt += 1
	_report_latency("datagram_checked", size, samples_us, 0, corrupt)


## calls get_datagram() directly, no count check first — the mistake this
## whole thread of questions is about. measures how long each individual
## call actually sat waiting, with an unprompted sender pacing itself on its
## own schedule instead of replying to us
func _measure_datagram_blocking_paced(iterations: int) -> void:
	var waits_us: Array = []
	for i in iterations:
		var t0 := Time.get_ticks_usec()
		_connection.get_datagram()
		waits_us.append(Time.get_ticks_usec() - t0)
	var total := 0
	for w in waits_us:
		total += w
	var avg := (total / float(waits_us.size())) if not waits_us.is_empty() else 0.0
	waits_us.sort()
	print(
		"RESULT transport=datagram_blocking_paced n=%d avg_wait_us=%.1f min_wait_us=%d max_wait_us=%d"
		% [waits_us.size(), avg, waits_us[0], waits_us[-1]]
	)


## isolates the main-thread cost of draining datagrams that already arrived
## — no network wait in this number, since by the time we start the clock
## the whole batch is already sitting in the queue, unread
func _measure_datagram_overhead(batch: int) -> void:
	# give the batch time to actually cross the loopback link and queue up
	# untouched — we deliberately do not call anything that would drain it
	# early
	await create_timer(1.0).timeout

	var t0 := Time.get_ticks_usec()
	var count := _connection.get_available_datagram_count()
	var drain_us := Time.get_ticks_usec() - t0

	var per_call_us: Array = []
	var got := 0
	while _connection.get_available_datagram_count() > 0:
		var call_t0 := Time.get_ticks_usec()
		_connection.get_datagram()
		per_call_us.append(Time.get_ticks_usec() - call_t0)
		got += 1

	var total := 0
	for us in per_call_us:
		total += us
	var avg_per_call := (total / float(per_call_us.size())) if not per_call_us.is_empty() else 0.0
	print(
		"RESULT transport=datagram_overhead batch=%d arrived=%d first_count_call_us=%d total_drain_us=%d avg_per_get_datagram_us=%.2f"
		% [batch, got, count, drain_us + total, avg_per_call]
	)


func _on_datagram(data: PackedByteArray) -> void:
	var i := data.decode_u64(0)
	_arrived_at[i] = Time.get_ticks_usec()
	_arrived_data[i] = data


## builds a payload with a genuinely random size (1..=max_size, not a fixed
## one) and genuinely random content — "arbitrary data" means both, not just
## random bytes at a size chosen by the test. used wherever the wire format
## does not require a size agreed in advance (datagrams: self-delimiting)
func _make_payload(i: int, max_size: int) -> PackedByteArray:
	return _make_fixed_payload(i, randi_range(1, max(1, max_size)))


## same random content, but exactly `size` bytes — for the fixed-frame
## stream transports, which read back exactly as many bytes as they wrote
func _make_fixed_payload(i: int, size: int) -> PackedByteArray:
	var payload := PackedByteArray()
	payload.resize(8 + size)
	payload.encode_u64(0, i)
	for b in size:
		payload[8 + b] = randi() % 256
	return payload


func _report_latency(transport: String, size: int, samples_us: Array, drops: int, corrupt: int) -> void:
	if samples_us.is_empty():
		print("RESULT transport=%s size=%d NO_SAMPLES drops=%d corrupt=%d" % [transport, size, drops, corrupt])
		return
	samples_us.sort()
	var total := 0
	for s in samples_us:
		total += s
	var n: int = samples_us.size()
	var avg := total / float(n)
	var p50: int = samples_us[int(n * 0.5)]
	var p95: int = samples_us[min(int(n * 0.95), n - 1)]
	print(
		"RESULT transport=%s n=%d size=%d min_us=%d avg_us=%.1f p50_us=%d p95_us=%d max_us=%d drops=%d corrupt=%d"
		% [transport, n, size, samples_us[0], avg, p50, p95, samples_us[-1], drops, corrupt]
	)


# --- correctness: a fixed set of edge cases, not random samples ----------------------


## label, size, fill byte pattern. "z" = all zero, "f" = all 0xff,
## "r" = random, "n" = mostly ascii with one embedded null in the middle
func _edge_cases() -> Array:
	return [
		{"label": "empty", "size": 0, "fill": "z"},
		{"label": "one_byte_zero", "size": 1, "fill": "z"},
		{"label": "one_byte_ff", "size": 1, "fill": "f"},
		{"label": "hundred_zero", "size": 100, "fill": "z"},
		{"label": "hundred_ff", "size": 100, "fill": "f"},
		{"label": "hundred_random", "size": 100, "fill": "r"},
		{"label": "embedded_null", "size": 101, "fill": "n"},
	]


func _fill_payload(size: int, fill: String) -> PackedByteArray:
	var payload := PackedByteArray()
	payload.resize(size)
	match fill:
		"z":
			pass # PackedByteArray.resize() zero-fills
		"f":
			for b in size:
				payload[b] = 0xFF
		"r":
			for b in size:
				payload[b] = randi() % 256
		"n":
			for b in size:
				payload[b] = 0x41 + (b % 20)
			if size > 0:
				payload[size / 2] = 0
	return payload


func _correctness_stream() -> void:
	var stream: IrohStream = _connection.open_stream()
	var cases := _edge_cases()
	cases.append({"label": "one_megabyte_random", "size": 1048576, "fill": "r"})
	var passed := 0
	var failed := 0
	for c in cases:
		var payload: PackedByteArray = _fill_payload(c.size, c.fill)
		var t0 := Time.get_ticks_usec()
		var framed := PackedByteArray()
		framed.resize(4)
		framed.encode_u32(0, payload.size())
		framed.append_array(payload)
		stream.put_data(framed)
		while stream.get_available_bytes() < 4:
			await process_frame
		var header: Array = stream.get_data(4)
		var length: int = (header[1] as PackedByteArray).decode_u32(0)
		while stream.get_available_bytes() < length:
			await process_frame
		var echoed: PackedByteArray = (stream.get_data(length) as Array)[1]
		var dt := Time.get_ticks_usec() - t0
		var ok := echoed == payload
		if ok:
			passed += 1
		else:
			failed += 1
		print("CASE transport=stream label=%s size=%d ok=%s us=%d" % [c.label, c.size, ok, dt])
	stream.finish()
	print("RESULT transport=correctness_stream passed=%d failed=%d" % [passed, failed])


func _correctness_datagram() -> void:
	_connection.datagram_received.connect(_on_datagram)
	var cases := _edge_cases()
	# cap is looked up fresh per case rather than once up front — it can move
	# during a connection (path mtu discovery). delta is against the *total*
	# bytes handed to send_datagram, index included — max_datagram_size()
	# describes that whole call's argument, not just the payload portion
	for delta in [0, 1]:
		cases.append({"label": "cap%+d" % delta, "delta": delta, "fill": "r", "expect_arrive": delta <= 0})
	var passed := 0
	var failed := 0
	for i in cases.size():
		var c = cases[i]
		var size: int = c.size if c.has("size") else _connection.max_datagram_size() + c.delta - 8
		var payload: PackedByteArray = _fill_payload(size, c.fill)
		var expect_arrive: bool = c.get("expect_arrive", true)
		_arrived_at.erase(i)
		_arrived_data.erase(i)
		var indexed := PackedByteArray()
		indexed.resize(8)
		indexed.encode_u64(0, i)
		indexed.append_array(payload)
		var t0 := Time.get_ticks_usec()
		var sent := _connection.send_datagram(indexed)
		var arrived := false
		if sent:
			var waited := 0
			while not _arrived_at.has(i) and waited < 300:
				await process_frame
				waited += 1
			arrived = _arrived_at.has(i)
		var dt := Time.get_ticks_usec() - t0
		var ok: bool
		if expect_arrive:
			ok = arrived and (_arrived_data[i] as PackedByteArray).slice(8) == payload
		else:
			ok = not arrived
		if ok:
			passed += 1
		else:
			failed += 1
		print(
			"CASE transport=datagram label=%s size=%d sent_ok=%s arrived=%s expect_arrive=%s ok=%s us=%d"
			% [c.label, indexed.size(), sent, arrived, expect_arrive, ok, dt]
		)
		_arrived_at.erase(i)
		_arrived_data.erase(i)
	print("RESULT transport=correctness_datagram passed=%d failed=%d" % [passed, failed])
