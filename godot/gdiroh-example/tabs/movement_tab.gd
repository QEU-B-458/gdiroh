extends UseCaseTab

## use case: stream player positions over datagrams.
##
## a datagram is a single packet — unreliable, unordered, never split up, and
## dropped outright when too big. that makes it wrong for chat and right for
## positions: a lost position is replaced by the next one a tick later, and
## nobody wants a stale one retransmitted. this is the path a game's movement
## data belongs on.
##
## both dots wander on their own; the other peer's dot moves because its
## positions are arriving here as datagrams, at whatever rate the spinner
## says.
##
## the `endpoint` everything below talks through is built by use_case_tab.gd
## when start is pressed — IrohEndpoint.new(), set_secret_key, bind — the
## same for every tab, so it lives there once instead of in each.

const ALPN := "gdiroh-example/move/1"

## open connections we exchange positions with. holding them here is what
## keeps them open
var _links: Array[IrohConnection] = []
## where our dot is on its wander, as an angle around the field
var _angle := 0.0
## seconds until the next position goes out
var _send_cooldown := 0.0
## datagrams that arrived in the current second, for the readout
var _received := 0
## datagrams that arrived in total, so we can say once that the flow is real
var _total := 0
## seconds left in the current readout window
var _window := 1.0

@onready var _target: LineEdit = $Dial/Target
@onready var _dial_button: Button = $Dial/Connect
## positions sent per second — a setting, because picking this number is the
## real design decision when a game moves to datagrams
@onready var _rate: SpinBox = $Dial/Rate
@onready var _field: Panel = $Field
@onready var _me: ColorRect = $Field/Me
@onready var _them: ColorRect = $Field/Them
@onready var _readout: Label = $Readout


func _setup() -> void:
	_dial_button.pressed.connect(func() -> void: dial(_target.text.strip_edges()))
	gate([_dial_button])


func _on_started() -> void:
	# answer anyone who dials our position protocol. the other peer needs no
	# dial from us — every connection carries datagrams both ways
	endpoint.connection_received.connect(_on_connection_received)
	endpoint.listen(ALPN)
	log_panel.note("dial the other peer's ticket, or wait to be dialled")


## connects to the other peer's movement endpoint
func dial(target: String) -> void:
	if target.is_empty():
		log_panel.fail("paste the other peer's id or ticket first")
		return
	if is_own(target):
		log_panel.fail("that is this window's own ticket — paste the other peer's")
		return

	var link := (
		endpoint.connect_to_ticket(target, ALPN) if target.begins_with("endpoint")
		else endpoint.connect_to(target, ALPN)
	)
	if link == null:
		log_panel.fail("could not start the connection")
		return

	link.opened.connect(func() -> void:
		log_panel.good("linked with %s" % short(link.remote_id(), 12)))
	link.failed.connect(func(reason: String) -> void:
		log_panel.fail("dial failed: %s" % reason))
	_adopt(link)


func _on_connection_received(alpn: String, connection: IrohConnection) -> void:
	if alpn != ALPN:
		return
	log_panel.good("linked with %s" % short(connection.remote_id(), 12))
	_adopt(connection)


## both sides treat a link the same once it exists: positions out, positions in
func _adopt(link: IrohConnection) -> void:
	_links.append(link)
	link.datagram_received.connect(_on_datagram)
	link.closed.connect(func(_reason: String) -> void:
		_links.erase(link)
		log_panel.write("link closed"))


func _process(delta: float) -> void:
	if endpoint == null:
		return

	# our dot wanders in a circle so there is always movement to send
	_angle += delta
	var center := _field.size * 0.5
	var mine := center + Vector2(cos(_angle), sin(_angle)) * center * 0.7
	_me.position = mine - _me.size * 0.5

	# positions go out on a fixed tick, not every frame — the rate spinner is
	# the whole point of the exercise
	_send_cooldown -= delta
	if _send_cooldown <= 0.0 and not _links.is_empty():
		_send_cooldown = 1.0 / _rate.value
		# eight bytes: x and y as fractions of the field, so the two windows
		# do not need to be the same size
		var packet := PackedByteArray()
		packet.resize(8)
		packet.encode_float(0, mine.x / _field.size.x)
		packet.encode_float(4, mine.y / _field.size.y)
		for link in _links:
			if link.is_open():
				link.send_datagram(packet)

	# once a second, say how fast positions are arriving and what kind of
	# path is carrying them
	_window -= delta
	if _window <= 0.0:
		_window = 1.0
		if _received > 0 and not _links.is_empty():
			var link := _links[0]
			var stats: Dictionary = link.get_stats()
			_readout.text = "receiving %d positions/s · %s · %.0f ms · max %d B · %d B in" % [
				_received,
				_path_name(link.get_path_type()),
				link.get_latency_ms(),
				link.max_datagram_size(),
				stats.get("received_bytes", 0),
			]
		_received = 0


func _path_name(kind: int) -> String:
	match kind:
		IrohConnection.PATH_DIRECT:
			return "direct"
		IrohConnection.PATH_RELAY:
			return "relayed"
	return "connecting"


func _on_datagram(data: PackedByteArray) -> void:
	if data.size() != 8:
		return
	var fraction := Vector2(data.decode_float(0), data.decode_float(4))
	_them.position = fraction * _field.size - _them.size * 0.5
	_received += 1
	_total += 1
	# one plain line once the flow is clearly real, for the scripted run
	if _total == 20:
		log_panel.good("positions flowing")


func _teardown() -> void:
	_links.clear()
	_total = 0
	_received = 0
	_readout.text = "not linked"


func _cue(verb: String, arg: String) -> void:
	match verb:
		"join":
			dial(arg)
		_:
			super(verb, arg)
