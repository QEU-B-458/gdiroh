extends Node

## godot's high level multiplayer over gdiroh — the whole cycle in one file.
##
## run it headless:
##   godot --headless --path . -s samples/_run.gd -- session.gd
## or attach it to any node in a scene and press play.
##
## an IrohPeer plugs into multiplayer.multiplayer_peer the same way an ENet
## peer does, so plain @rpc functions cross it unchanged. to fit a host and a
## joiner into one process, each side gets a branch of the scene tree with a
## MultiplayerAPI of its own — the same trick splitscreen tests use.


func _ready() -> void:
	var alice: IrohEndpoint = await _start_peer()
	var bob: IrohEndpoint = await _start_peer()

	# two branches, each with a multiplayer world of its own
	var host_side: Chatter = _make_side("HostSide")
	var join_side: Chatter = _make_side("JoinSide")

	# alice hosts — godot numbers the host peer 1
	var host_peer := IrohPeer.new()
	if not host_peer.host(alice):
		push_error("could not host")
		get_tree().quit(1)
		return
	host_side.multiplayer.multiplayer_peer = host_peer

	# bob joins with alice's ticket
	var join_peer := IrohPeer.new()
	if not join_peer.join_ticket(bob, alice.ticket()):
		push_error("could not join")
		get_tree().quit(1)
		return
	join_side.multiplayer.multiplayer_peer = join_peer
	await join_side.multiplayer.connected_to_server
	print("bob joined as peer ", join_side.multiplayer.get_unique_id())

	# an ordinary rpc, crossing the wire from bob's branch to alice's
	join_side.shout.rpc("hello over rpc")
	var text: String = await host_side.heard
	print("alice heard over rpc: ", text)

	# taking the peer away from a multiplayer is how a session ends
	join_side.multiplayer.multiplayer_peer = null
	host_side.multiplayer.multiplayer_peer = null
	print("sample done: session")
	get_tree().quit()


## makes and binds one peer. a real game does this once and keeps the endpoint
func _start_peer() -> IrohEndpoint:
	var peer := IrohEndpoint.new()
	peer.set_secret_key(IrohEndpoint.generate_secret_key())
	peer.bind()
	await peer.bound
	return peer


## builds one branch: a root that owns the multiplayer, with a chatter inside.
## the chatter's path relative to its root is the same on both sides, which is
## what lets the rpc find its twin
func _make_side(side_name: String) -> Chatter:
	var side_root := Node.new()
	side_root.name = side_name
	add_child(side_root)
	get_tree().set_multiplayer(MultiplayerAPI.create_default_interface(), side_root.get_path())
	var chatter := Chatter.new()
	chatter.name = "Chatter"
	side_root.add_child(chatter)
	return chatter


## a tiny node with one rpc; one copy lives on each side
class Chatter:
	extends Node

	## fired when the rpc lands, so the story above can await it
	signal heard(text: String)

	@rpc("any_peer", "call_remote", "reliable")
	func shout(text: String) -> void:
		heard.emit(text)
