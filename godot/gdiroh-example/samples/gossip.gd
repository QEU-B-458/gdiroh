extends Node

## a room chat over gossip — the whole cycle in one file.
##
## run it headless:
##   godot --headless --path . -s samples/_run.gd -- gossip.gd
## or attach it to any node in a scene and press play.
##
## everyone subscribed to a topic receives what anyone broadcasts on it,
## passed peer to peer with nobody keeping a member list. delivery is best
## effort — that fits presence and chatter, not anything that has to arrive.

const TOPIC := "gdiroh-sample/lobby"


func _ready() -> void:
	var alice: IrohEndpoint = await _start_peer()
	var bob: IrohEndpoint = await _start_peer()

	# the first peer subscribes with no bootstrap — someone has to be first
	var room_a: IrohTopic = alice.subscribe(TOPIC, PackedStringArray())

	# everyone after that bootstraps off a peer already in the topic. bob
	# learns alice's addresses from her ticket, then points his subscribe at
	# her id
	var alice_id := bob.remember_peer(alice.ticket())
	var room_b: IrohTopic = bob.subscribe(TOPIC, PackedStringArray([alice_id]))

	# a neighbour is a peer we are attached to directly. alice having one
	# means the room is really joined up and a broadcast has somewhere to go
	await room_a.neighbor_up
	print("alice sees a neighbour in the room")

	room_a.broadcast("the door creaks open".to_utf8_buffer())

	# message carries the bytes, whoever passed them on, and whether they came
	# direct or relayed. awaiting a signal with several arguments hands back
	# an array of them
	var heard: Array = await room_b.message
	print("bob heard: ", (heard[0] as PackedByteArray).get_string_from_utf8())

	# leaving is explicit here; dropping the last reference would also do it
	room_a.leave()
	room_b.leave()
	print("sample done: gossip")
	get_tree().quit()


## makes and binds one peer. a real game does this once and keeps the endpoint
func _start_peer() -> IrohEndpoint:
	var peer := IrohEndpoint.new()
	peer.set_secret_key(IrohEndpoint.generate_secret_key())
	peer.bind()
	await peer.bound
	return peer
