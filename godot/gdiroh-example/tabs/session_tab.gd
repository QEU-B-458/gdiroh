extends UseCaseTab

## use case: run godot's own high level multiplayer over gdiroh.
##
## an [IrohPeer] plugs into `multiplayer.multiplayer_peer` the same way an
## ENet peer does, so RPCs work unchanged — the chat below is an ordinary
## @rpc function. what gdiroh adds is the readout: the endpoint id behind
## each godot peer id, whether its path is direct or relayed, and the
## latency.
##
## the `endpoint` everything below talks through is built by use_case_tab.gd
## when start is pressed — IrohEndpoint.new(), set_secret_key, bind — the
## same for every tab, so it lives there once instead of in each.

## the multiplayer peer for this session. held here so the session lives
## exactly as long as this tab wants it to
var _peer: IrohPeer
## seconds until the next refresh of the peer readout
var _cooldown := 0.0

@onready var _host_button: Button = $Actions/Host
@onready var _target: LineEdit = $Actions/Target
@onready var _join_button: Button = $Actions/Join
@onready var _message: LineEdit = $Say/Message
@onready var _mode: OptionButton = $Say/Mode
@onready var _send: Button = $Say/Send
@onready var _readout: Label = $Readout


func _setup() -> void:
	_host_button.pressed.connect(host)
	_join_button.pressed.connect(func() -> void: join(_target.text.strip_edges()))
	_send.pressed.connect(_say)
	_message.text_submitted.connect(func(_text: String) -> void: _say())

	# the three transfer modes gdiroh maps onto QUIC: a stream, a datagram,
	# and a datagram that drops anything arriving out of order
	_mode.add_item("reliable", MultiplayerPeer.TRANSFER_MODE_RELIABLE)
	_mode.add_item("unreliable", MultiplayerPeer.TRANSFER_MODE_UNRELIABLE)
	_mode.add_item("unreliable ordered", MultiplayerPeer.TRANSFER_MODE_UNRELIABLE_ORDERED)
	_mode.item_selected.connect(func(_index: int) -> void: _apply_mode())

	gate([_host_button, _join_button])
	_send.disabled = true


func _on_started() -> void:
	log_panel.note("host here, or paste the host's ticket and join")


## starts a session with us as the host, which godot numbers peer 1
func host() -> void:
	if _peer != null:
		return
	_peer = IrohPeer.new()
	if not _peer.host(endpoint):
		log_panel.fail("could not host — is the endpoint bound?")
		_peer = null
		return
	_adopt()
	log_panel.good("hosting as peer %d" % multiplayer.get_unique_id())


## joins the session that a ticket or endpoint id points at
func join(target: String) -> void:
	if _peer != null:
		return
	if target.is_empty():
		log_panel.fail("paste the host's id or ticket first")
		return
	if is_own(target):
		log_panel.fail("that is this window's own ticket — paste the other player's")
		return

	_peer = IrohPeer.new()
	# a ticket starts with "endpoint" and carries the host's addresses; a bare
	# id needs a lookup service to resolve
	var joined := (
		_peer.join_ticket(endpoint, target) if target.begins_with("endpoint")
		else _peer.join(endpoint, target)
	)
	if not joined:
		log_panel.fail("could not join — is that an id or a ticket?")
		_peer = null
		return
	_adopt()
	log_panel.write("joining %s…" % short(target, 12))


## hands the peer to godot's multiplayer and wires the session signals.
## `multiplayer` is shared by the whole scene, so the connections are guarded —
## this tab can be started and stopped many times
func _adopt() -> void:
	multiplayer.multiplayer_peer = _peer
	if not multiplayer.peer_connected.is_connected(_on_peer_connected):
		multiplayer.peer_connected.connect(_on_peer_connected)
	if not multiplayer.peer_disconnected.is_connected(_on_peer_disconnected):
		multiplayer.peer_disconnected.connect(_on_peer_disconnected)
	if not multiplayer.connected_to_server.is_connected(_on_connected):
		multiplayer.connected_to_server.connect(_on_connected)
	if not multiplayer.connection_failed.is_connected(_on_connection_failed):
		multiplayer.connection_failed.connect(_on_connection_failed)
	if not multiplayer.server_disconnected.is_connected(_on_server_disconnected):
		multiplayer.server_disconnected.connect(_on_server_disconnected)
	# the transport warns rather than failing when a packet cannot go out as
	# asked; worth seeing while tuning what a game sends
	_peer.warning.connect(func(text: String) -> void: log_panel.fail("transport: %s" % text))
	_send.disabled = false


func _on_connected() -> void:
	log_panel.good("connected as peer %d" % multiplayer.get_unique_id())


func _on_peer_connected(id: int) -> void:
	log_panel.good("peer %d connected, endpoint %s" % [id, short(_endpoint_of(id), 12)])


func _on_peer_disconnected(id: int) -> void:
	log_panel.write("peer %d left" % id)


## joining a host who is not hosting yet is refused, and this is how godot
## tells us. the endpoint is fine — only the session attempt is over
func _on_connection_failed() -> void:
	log_panel.fail("could not join — is the other side hosting?")
	_leave_session()


func _on_server_disconnected() -> void:
	log_panel.write("the host went away")
	_leave_session()


# --- chat over rpc ------------------------------------------------------------


func _say() -> void:
	var text := _message.text.strip_edges()
	if text.is_empty() or _peer == null:
		return
	_message.text = ""
	log_panel.write("me: %s" % text)
	receive_message.rpc(text)


## applies the dropdown's transfer mode to the chat rpc. godot sends an rpc
## with the mode in its rpc config — not the peer's current transfer mode —
## so reconfiguring the method is the change that actually takes
func _apply_mode() -> void:
	rpc_config("receive_message", {
		"rpc_mode": MultiplayerAPI.RPC_MODE_ANY_PEER,
		"call_local": false,
		"transfer_mode": _mode.get_item_id(_mode.selected),
		"channel": 0,
	})


@rpc("any_peer", "call_remote", "reliable")
func receive_message(text: String) -> void:
	log_panel.write("peer %d: %s" % [multiplayer.get_remote_sender_id(), text])


# --- readout ------------------------------------------------------------------


## refreshed on a timer because a path can be upgraded from relayed to direct
## at any moment
func _process(delta: float) -> void:
	_cooldown -= delta
	if _peer == null or _cooldown > 0.0:
		return
	_cooldown = 1.0

	var lines := PackedStringArray()
	for id in multiplayer.get_peers():
		var stats: Dictionary = _peer.get_peer_stats(id)
		lines.append("peer %d · %s · %s · %.0f ms · %d B in" % [
			id,
			short(_endpoint_of(id), 12),
			_path_name(_peer.get_peer_path_type(id)),
			_peer.get_peer_latency_ms(id),
			stats.get("received_bytes", 0),
		])
	_readout.text = "\n".join(lines) if lines.size() > 0 else "no peers yet"


func _path_name(kind: int) -> String:
	match kind:
		IrohPeer.PATH_DIRECT:
			return "direct"
		IrohPeer.PATH_RELAY:
			return "relayed"
	return "connecting"


func _endpoint_of(id: int) -> String:
	return _peer.get_peer_endpoint_id(id) if _peer != null else ""


# --- lifecycle and cues -------------------------------------------------------


func _teardown() -> void:
	_leave_session()


## ends the session but keeps the tab's endpoint, so hosting or joining again
## needs no restart
func _leave_session() -> void:
	if _peer != null:
		# taking the peer away from godot's multiplayer ends the session; the
		# peer itself goes when its last reference does, right below
		multiplayer.multiplayer_peer = null
		_peer = null
	_send.disabled = true
	_readout.text = "no peers yet"


func _cue(verb: String, arg: String) -> void:
	match verb:
		"host":
			host()
		"join":
			join(arg)
		"say":
			_message.text = arg
			_say()
		"mode":
			for index in _mode.item_count:
				if _mode.get_item_text(index) == arg:
					_mode.selected = index
					_apply_mode()
					return
			log_panel.fail("no transfer mode called '%s'" % arg)
		_:
			super(verb, arg)
