extends Node

## raw packets between peers, no MultiplayerAPI in sight — the whole cycle in
## one file.
##
## run it headless:
##   godot --headless --path . -s samples/_run.gd -- packets.gd
## or attach it to any node in a scene and press play.
##
## an IrohPeer is a MultiplayerPeer, and a MultiplayerPeer is a PacketPeer —
## it can be used bare, with put_packet and get_packet addressed by peer id,
## never assigned to multiplayer.multiplayer_peer and with no @rpc in sight.
## going bare comes with one rule: poll() is yours to call every frame — with
## no MultiplayerAPI over the peer, nobody else will.


func _ready() -> void:
	var alice: IrohEndpoint = await _start_peer()
	var bob: IrohEndpoint = await _start_peer()

	# alice hosts a session; bob joins it with her ticket. the host relays,
	# so any number of clients could join and still address one another
	var host_peer := IrohPeer.new()
	if not host_peer.host(alice):
		push_error("could not host")
		get_tree().quit(1)
		return
	var join_peer := IrohPeer.new()
	if not join_peer.join_ticket(bob, alice.ticket()):
		push_error("could not join")
		get_tree().quit(1)
		return

	while join_peer.get_connection_status() != MultiplayerPeer.CONNECTION_CONNECTED:
		await _tick(host_peer, join_peer)
	print("bob is in, as peer ", join_peer.get_unique_id())

	# bob sends one packet straight to alice — no rpc, no stream, just bytes
	# addressed by peer id, the way ordinary multiplayer traffic is
	join_peer.set_target_peer(0)
	join_peer.put_packet("packed by hand".to_utf8_buffer())

	# alice reads it back. who sent it is read before get_packet, because it
	# describes the packet about to be returned
	while host_peer.get_available_packet_count() == 0:
		await _tick(host_peer, join_peer)
	var sender := host_peer.get_packet_peer()
	print("alice got, from peer %d: %s" % [sender, host_peer.get_packet().get_string_from_utf8()])

	# the answer goes to that one peer only, unreliable — a datagram under
	# the hood, the mode a game's movement traffic rides
	host_peer.set_target_peer(sender)
	host_peer.set_transfer_mode(MultiplayerPeer.TRANSFER_MODE_UNRELIABLE)
	host_peer.put_packet("heard you".to_utf8_buffer())

	while join_peer.get_available_packet_count() == 0:
		await _tick(host_peer, join_peer)
	var as_datagram := join_peer.get_packet_mode() == MultiplayerPeer.TRANSFER_MODE_UNRELIABLE
	print("bob got back: ", join_peer.get_packet().get_string_from_utf8(), " — carried as a datagram: ", as_datagram)

	host_peer.close()
	join_peer.close()
	print("sample done: packets")
	get_tree().quit()


## makes and binds one peer. a real game does this once and keeps the endpoint
func _start_peer() -> IrohEndpoint:
	var peer := IrohEndpoint.new()
	peer.set_secret_key(IrohEndpoint.generate_secret_key())
	peer.bind()
	await peer.bound
	return peer


## one frame for both bare peers — polling them is this file's job
func _tick(host_peer: IrohPeer, join_peer: IrohPeer) -> void:
	host_peer.poll()
	join_peer.poll()
	await get_tree().process_frame
