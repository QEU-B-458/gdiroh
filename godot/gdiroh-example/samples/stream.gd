extends Node

## two peers hold one conversation over a single stream — the whole cycle in
## one file.
##
## run it headless:
##   godot --headless --path . -s samples/_run.gd -- stream.gd
## or attach it to any node in a scene and press play. it makes both peers
## itself, so there is nothing to paste and no second window to start.
##
## a stream is a pipe of bytes with no message boundaries of its own, so both
## ends must agree where one message stops and the next begins. here that is
## put_utf8_string and get_utf8_string: a small length header before the
## text, read back as exactly one whole message every time.

## the protocol's name. both ends must use the same one — it is how an
## endpoint knows which of its protocols a caller wants
const ALPN := "gdiroh-sample/stream/1"

## the connection that reached alice, held here because holding it is what
## keeps it open
var _answered: IrohConnection


func _ready() -> void:
	# two peers in one process, so the whole exchange fits in this file. a
	# real game would be one of these, with the other across the network
	var alice: IrohEndpoint = await _start_peer()
	var bob: IrohEndpoint = await _start_peer()
	print("alice is ", alice.endpoint_id())
	print("bob   is ", bob.endpoint_id())

	# alice claims the protocol name and answers whoever dials it
	alice.listen(ALPN)
	alice.connection_received.connect(func(_alpn: String, connection: IrohConnection) -> void:
		_answered = connection
		connection.stream_opened.connect(_answer))

	# a ticket is how one peer tells another where it is — id plus addresses.
	# over the network you would paste it; here it just crosses the file
	var link: IrohConnection = bob.connect_to_ticket(alice.ticket(), ALPN)
	await link.opened
	print("linked")

	# one stream for the whole conversation, and every shout goes down it
	var stream: IrohStream = link.open_stream()
	stream.put_utf8_string("first shout")
	stream.put_utf8_string("second shout, same stream")

	# replies come back down the same stream, framed the same way. we wait for
	# a header before reading, so the read never has to sit and block
	for _i in 2:
		while stream.get_available_bytes() < 4:
			await get_tree().process_frame
		print("reply: ", stream.get_utf8_string())

	# finishing says "no more from me" — the polite way to end a stream
	stream.finish()
	print("sample done: stream")
	get_tree().quit()


## makes and binds one peer. a real game does this once and keeps the endpoint
func _start_peer() -> IrohEndpoint:
	var peer := IrohEndpoint.new()
	# the key is the identity. gdiroh never stores it — hold it however your
	# game holds secrets. a fresh one each run means a fresh peer id
	peer.set_secret_key(IrohEndpoint.generate_secret_key())
	peer.bind()
	await peer.bound
	return peer


## alice's side: read each framed shout as it lands and answer it in capitals,
## on the same stream it came in on
func _answer(stream: IrohStream) -> void:
	while stream.is_open() or stream.get_available_bytes() >= 4:
		if stream.get_available_bytes() < 4:
			await get_tree().process_frame
			continue
		var shout := stream.get_utf8_string()
		print("alice heard: ", shout)
		stream.put_utf8_string(shout.to_upper())
