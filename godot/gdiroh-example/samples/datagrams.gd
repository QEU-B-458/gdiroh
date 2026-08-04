extends Node

## positions tick across as datagrams — the whole cycle in one file.
##
## run it headless:
##   godot --headless --path . -s samples/_run.gd -- datagrams.gd
## or attach it to any node in a scene and press play.
##
## a datagram is a single packet: unreliable, unordered, never split up, and
## dropped outright when too big. a lost position is replaced by the next
## tick, and nobody wants a stale one retransmitted — which is why movement
## belongs here and chat does not.

const ALPN := "gdiroh-sample/datagrams/1"
const TICKS := 30

## how many of bob's packets actually landed, counted on alice's side
var _caught := 0
## the connection that reached alice, held here so it stays open
var _answered: IrohConnection


func _ready() -> void:
	var alice: IrohEndpoint = await _start_peer()
	var bob: IrohEndpoint = await _start_peer()

	# alice answers the dial and counts every position that reaches her.
	# datagrams ride the connection itself — no stream to open
	alice.listen(ALPN)
	alice.connection_received.connect(func(_alpn: String, connection: IrohConnection) -> void:
		_answered = connection
		connection.datagram_received.connect(_on_position))

	var link: IrohConnection = bob.connect_to_ticket(alice.ticket(), ALPN)
	await link.opened
	print("linked; a datagram here may carry up to ", link.max_datagram_size(), " bytes")

	# bob ticks positions out the way a game would: a small fixed packet at a
	# steady rate, each one replacing the last
	for tick in TICKS:
		var packet := PackedByteArray()
		packet.resize(8)
		packet.encode_float(0, cos(tick * 0.2))
		packet.encode_float(4, sin(tick * 0.2))
		link.send_datagram(packet)
		await get_tree().create_timer(0.03).timeout

	# a moment for the last packets to land — datagrams have no "done" signal,
	# because not arriving is a thing they are allowed to do
	await get_tree().create_timer(0.5).timeout
	print(
		"alice caught ", _caught, " of ", TICKS,
		" — a few missing would be normal, that is the deal"
	)
	print("sample done: datagrams")
	get_tree().quit()


## makes and binds one peer. a real game does this once and keeps the endpoint
func _start_peer() -> IrohEndpoint:
	var peer := IrohEndpoint.new()
	peer.set_secret_key(IrohEndpoint.generate_secret_key())
	peer.bind()
	await peer.bound
	return peer


## one position landed on alice's side
func _on_position(data: PackedByteArray) -> void:
	if data.size() != 8:
		return
	_caught += 1
	# decode the first one back, just to show the numbers crossed intact
	if _caught == 1:
		print("first position: (%.2f, %.2f)" % [data.decode_float(0), data.decode_float(4)])
