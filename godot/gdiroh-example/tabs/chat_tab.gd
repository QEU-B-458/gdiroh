extends UseCaseTab

## use case: a room chat over gossip.
##
## everyone subscribed to a topic receives what anyone broadcasts on it.
## messages are passed peer to peer with nobody holding a member list, which
## is what lets a topic scale. delivery is best effort — a peer still joining
## can miss a message — so this fits presence and chatter, not anything that
## has to arrive.
##
## the first peer subscribes with no bootstrap; everyone after that
## bootstraps off a peer already in the topic. the room is whatever name is in
## the topic field — both sides type the same name, or they are simply in
## different rooms.
##
## the `endpoint` everything below talks through is built by use_case_tab.gd
## when start is pressed — IrohEndpoint.new(), set_secret_key, bind — the
## same for every tab, so it lives there once instead of in each.

const DEFAULT_TOPIC := "gdiroh-example/lobby"

## holder for the topic subscription; dropping it is leaving, so we keep it
## until the tab stops or the user leaves on purpose
var _topic: IrohTopic
## seconds until the next refresh of the neighbours line
var _cooldown := 0.0

@onready var _topic_field: LineEdit = $Subscribe/Topic
@onready var _bootstrap: LineEdit = $Subscribe/Bootstrap
@onready var _subscribe_button: Button = $Subscribe/Join
@onready var _message: LineEdit = $Say/Message
@onready var _near_only: CheckBox = $Say/NearOnly
@onready var _send: Button = $Say/Send
@onready var _neighbours: Label = $Neighbours


func _setup() -> void:
	_subscribe_button.pressed.connect(func() -> void: subscribe(_bootstrap.text.strip_edges()))
	_send.pressed.connect(_say)
	_message.text_submitted.connect(func(_text: String) -> void: _say())
	gate([_subscribe_button])
	_send.disabled = true


func _on_started() -> void:
	log_panel.note("subscribe with a peer's ticket to join them, or with nothing to be first")


## joins the room. `seed` is empty for the first peer, or an id or ticket of
## a peer already in the topic
func subscribe(seed: String) -> void:
	if _topic != null:
		return

	# the topic name is a setting, decided before subscribing. an emptied
	# field falls back to the default so both sides can find each other
	var topic := _topic_field.text.strip_edges()
	if topic.is_empty():
		topic = DEFAULT_TOPIC
		_topic_field.text = topic

	if not seed.is_empty() and is_own(seed):
		log_panel.fail("that is this window's own ticket — use someone else's, or none")
		return

	# bootstrapping needs a resolvable peer. a bare id resolves through a
	# lookup service; a ticket carries the addresses and is taught to the
	# endpoint first
	var peers := PackedStringArray()
	if seed.begins_with("endpoint"):
		var learned := endpoint.remember_peer(seed)
		if learned.is_empty():
			log_panel.fail("that ticket did not parse")
			return
		peers.append(learned)
	elif not seed.is_empty():
		peers.append(seed)

	_topic = endpoint.subscribe(topic, peers)
	if _topic == null:
		log_panel.fail("could not subscribe")
		return

	_topic.message.connect(_on_message)
	_topic.neighbor_up.connect(func(peer: String) -> void:
		log_panel.good("neighbour up: %s" % short(peer, 12)))
	_topic.neighbor_down.connect(func(peer: String) -> void:
		log_panel.write("neighbour down: %s" % short(peer, 12)))
	_topic.closed.connect(func(reason: String) -> void:
		log_panel.fail("topic closed: %s" % reason))
	# falling behind is recovered from by subscribing again, which is why it
	# is its own signal and not a kind of closed
	_topic.lagged.connect(func() -> void:
		log_panel.fail("fell behind — subscribe again to rejoin"))

	_send.disabled = false
	_subscribe_button.disabled = true
	# the name only picks the room while subscribing, so lock it until the
	# next subscribe rather than let edits look like they do something
	_topic_field.editable = false
	log_panel.good("subscribed to '%s'" % topic)
	# names are hashed to ids on the wire; when two peers cannot find each
	# other, comparing these is the first thing to check
	log_panel.note("topic id %s" % _topic.get_topic_id())


func _say() -> void:
	var text := _message.text.strip_edges()
	if text.is_empty() or _topic == null:
		return
	_message.text = ""

	# neighbours only skips the onward relaying — the message reaches the
	# peers we are directly attached to and stops there
	var sent := (
		_topic.broadcast_neighbors(text.to_utf8_buffer()) if _near_only.button_pressed
		else _topic.broadcast(text.to_utf8_buffer())
	)
	if sent:
		log_panel.write("me%s: %s" % [" (to neighbours)" if _near_only.button_pressed else "", text])
	else:
		log_panel.fail("not sent — has the topic ended?")


## `from` is whoever passed the message on, not necessarily who wrote it —
## gossip relays. put the author inside the payload when that matters
func _on_message(data: PackedByteArray, from: String, direct: bool) -> void:
	log_panel.write("%s%s: %s" % [
		short(from, 12), "" if direct else " (relayed)", data.get_string_from_utf8()
	])


func _process(delta: float) -> void:
	_cooldown -= delta
	if _topic == null or _cooldown > 0.0:
		return
	_cooldown = 1.0

	# neighbours are the handful of peers we are attached to directly, not
	# the room's membership — messages reach peers further away too. joined
	# means at least one of them exists
	var near := _topic.neighbors()
	var names := PackedStringArray()
	for peer in near:
		names.append(short(peer))
	var listed := ", ".join(names) if near.size() > 0 else "none"
	_neighbours.text = "neighbours (%d, joined: %s): %s" % [
		near.size(), _topic.is_joined(), listed
	]


func _teardown() -> void:
	if _topic != null:
		_topic.leave()
		_topic = null
	_send.disabled = true
	_subscribe_button.disabled = false
	_topic_field.editable = true
	_neighbours.text = "neighbours: none"


func _cue(verb: String, arg: String) -> void:
	match verb:
		"host":
			subscribe("")
		"join":
			subscribe(arg)
		"say":
			_near_only.button_pressed = false
			_message.text = arg
			_say()
		"saynear":
			_near_only.button_pressed = true
			_message.text = arg
			_say()
		_:
			super(verb, arg)
