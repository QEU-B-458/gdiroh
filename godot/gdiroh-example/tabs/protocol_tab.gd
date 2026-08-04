extends UseCaseTab

## use case: a protocol of your own, over one long-lived stream.
##
## both ends agree on a protocol name (an ALPN) and everything after that is
## yours — gdiroh carries the bytes and stays out of the way. the little
## protocol here is "shout": one stream stays open for the whole conversation,
## every shout goes down it, and the far side answers each one in capitals on
## the same stream.
##
## a stream is a pipe of bytes with no message boundaries of its own, so both
## ends must agree where one shout stops and the next begins. that is what
## put_utf8_string and get_utf8_string are for: the writer sends a small
## length header before the text, the reader reads the header and then exactly
## that many bytes, and out comes one whole shout every time. the other valid
## shape is a fresh stream per request, the way the web does it — streams cost
## almost nothing, and the stream's end marks the message's end with no
## framing at all. this tab shows the long-lived shape.
##
## this is the escape hatch for everything the other tabs do not cover:
## voice, custom sync, anything with its own rules.
##
## the `endpoint` everything below talks through is built by use_case_tab.gd
## when start is pressed — IrohEndpoint.new(), set_secret_key, bind — the
## same for every tab, so it lives there once instead of in each.

const DEFAULT_ALPN := "gdiroh-example/shout/1"

## the protocol name this tab is actually using, taken from the field when
## the endpoint starts. both ends must use the same one
var _alpn := DEFAULT_ALPN

## the connection we dialled out, if any. held here to stay open
var _outbound: IrohConnection
## the stream our whole conversation travels down; null until we are linked,
## and again after a hang up
var _stream: IrohStream

## connections others opened to us, held so they stay open
var _inbound: Array[IrohConnection] = []
## the conversations we are answering, one long-lived stream each
var _serving: Array[IrohStream] = []

@onready var _alpn_field: LineEdit = $Dial/Alpn
@onready var _target: LineEdit = $Dial/Target
@onready var _dial_button: Button = $Dial/Connect
@onready var _text: LineEdit = $Say/Text
@onready var _send: Button = $Say/Send
@onready var _hangup_button: Button = $Say/Abort


func _setup() -> void:
	_dial_button.pressed.connect(func() -> void: dial(_target.text.strip_edges()))
	_send.pressed.connect(_say)
	_hangup_button.pressed.connect(_hang_up)
	_text.text_submitted.connect(func(_t: String) -> void: _say())
	gate([_dial_button])
	_send.disabled = true
	_hangup_button.disabled = true


func _on_started() -> void:
	# the protocol name is a setting decided before starting; an emptied
	# field falls back to the default. locked while running, because a live
	# endpoint keeps listening on what it claimed
	_alpn = _alpn_field.text.strip_edges()
	if _alpn.is_empty():
		_alpn = DEFAULT_ALPN
		_alpn_field.text = _alpn
	_alpn_field.editable = false

	# claim our protocol name. anyone dialling it lands in
	# connection_received below
	endpoint.connection_received.connect(_on_connection_received)
	endpoint.listen(_alpn)
	log_panel.note("dial the other peer's ticket and shout at them")


## connects to the other peer's shout protocol
func dial(target: String) -> void:
	if target.is_empty():
		log_panel.fail("paste the other peer's id or ticket first")
		return
	if is_own(target):
		log_panel.fail("that is this window's own ticket — paste the other peer's")
		return

	_outbound = (
		endpoint.connect_to_ticket(target, _alpn) if target.begins_with("endpoint")
		else endpoint.connect_to(target, _alpn)
	)
	if _outbound == null:
		log_panel.fail("could not start the connection")
		return

	_outbound.opened.connect(func() -> void:
		_converse()
		log_panel.good("linked with %s" % short(_outbound.remote_id(), 12)))
	_outbound.failed.connect(func(reason: String) -> void:
		log_panel.fail("dial failed: %s" % reason))
	_outbound.closed.connect(func(reason: String) -> void:
		_stream = null
		_send.disabled = true
		_hangup_button.disabled = true
		log_panel.write("connection closed: %s" % reason))


## opens the stream the whole conversation runs on. one stream, many shouts —
## the length headers are what keep them apart
func _converse() -> void:
	_stream = _outbound.open_stream()
	_send.disabled = false
	_hangup_button.disabled = false


## sends one shout down the conversation stream
func _say() -> void:
	if _outbound == null or not _outbound.is_open():
		log_panel.fail("not linked yet")
		return
	if _stream == null:
		# the last conversation was hung up. streams cost almost nothing, so a
		# new one on the same connection simply starts the next conversation
		_converse()
	var message := _text.text.strip_edges()
	if message.is_empty():
		message = "hello over our own protocol"
	# one framed shout: a length header, then the text. the reply comes back
	# down this same stream, framed the same way
	_stream.put_utf8_string(message)
	log_panel.write("shouted: %s" % message)


## hangs up the conversation and tells the far side why, with a code both
## ends agree on — different from just dropping the stream and leaving them
## to notice
func _hang_up() -> void:
	if _stream == null:
		log_panel.note("no conversation to hang up")
		return
	_stream.abort(7)
	_stream = null
	_hangup_button.disabled = true
	log_panel.write("hung up with code 7")


# --- serving the other side ---------------------------------------------------


func _on_connection_received(alpn: String, connection: IrohConnection) -> void:
	if alpn != _alpn:
		return
	# the connection knows what was negotiated, so log that rather than what
	# we assume
	log_panel.good("answered %s on %s" % [short(connection.remote_id(), 12), connection.alpn()])
	_inbound.append(connection)
	# every stream they open is one conversation, answered in _process below
	connection.stream_opened.connect(func(stream: IrohStream) -> void:
		_serving.append(stream))


## streams are polled like any other StreamPeer, so both directions of the
## shout protocol live here in _process
func _process(_delta: float) -> void:
	# conversations aimed at us: answer every whole shout that has arrived
	for stream: IrohStream in _serving.duplicate():
		# is_open first — it also pulls from the network, so bytes that landed
		# together with the stream's ending are already counted below
		var open := stream.is_open()
		while stream.get_available_bytes() >= 4:
			# a whole length header is here, and the text behind it travels in
			# the same packet in practice, so get_utf8_string has nothing to
			# wait for
			var shout := stream.get_utf8_string()
			stream.put_utf8_string(shout.to_upper())
			log_panel.write("shouted back \"%s\"" % shout.to_upper())
		if not open:
			_serving.erase(stream)
			var reason := stream.get_error()
			if reason.is_empty():
				log_panel.write("they hung up")
			else:
				log_panel.write("they hung up: %s" % reason)

	# replies to our own shouts, framed the same way we sent them
	if _stream != null:
		var open := _stream.is_open()
		while _stream.get_available_bytes() >= 4:
			log_panel.good("reply \"%s\"" % _stream.get_utf8_string())
		if not open:
			var reason := _stream.get_error()
			if reason.is_empty():
				log_panel.write("the conversation ended")
			else:
				log_panel.fail("the conversation ended badly: %s" % reason)
			_stream = null
			_hangup_button.disabled = true


func _teardown() -> void:
	if _stream != null:
		# finishing is the polite goodbye — it says "no more from me", and the
		# far side reads a clean ending instead of a vanished peer
		_stream.finish()
		_stream = null
	_outbound = null
	_inbound.clear()
	_serving.clear()
	_send.disabled = true
	_hangup_button.disabled = true
	_alpn_field.editable = true


func _cue(verb: String, arg: String) -> void:
	match verb:
		"join":
			dial(arg)
		"say":
			_text.text = arg
			_say()
		"abort":
			_hang_up()
		_:
			super(verb, arg)
