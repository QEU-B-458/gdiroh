extends Node

## two real windows, one shared stream, chat over your own protocol —
## the shape a manual test looks like before it becomes a use-case tab.
##
## unlike the other files in samples/, this one cannot run through _run.gd:
## it needs a human, two separate running copies of the game, and a scene
## with two buttons already in it (unique-named %copy_endpoint_to_clipboard
## and %join_from_clipboard). run two instances of the project side by side,
## press copy in one, press join in the other after pasting, then watch the
## console in both.
##
## for a version of this same flow with a real UI already built and covered
## by the test suite, see tabs/protocol_tab.gd — run it with
## ./two-peers.sh --manual and open the "protocol" tab.
##
## the bug this fixes: a connection that reaches you through
## connection_received is already open, so its `opened` signal never fires —
## see [method IrohConnection.opened]. wiring both sides to `opened` means
## the accepting side's handler never runs, so it never opens a stream and
## never listens for one either. fix is for the accepting side to listen for
## stream_opened directly, and only the dialling side to call open_stream().

@onready var copy_endpoint_to_clipboard: Button = %copy_endpoint_to_clipboard
@onready var join_from_clipboard: Button = %join_from_clipboard

var connection: IrohConnection
var endpoint := IrohEndpoint.new()
var stream: IrohStream


func _ready() -> void:
	endpoint.bind()
	endpoint.listen("awwooo")
	copy_endpoint_to_clipboard.pressed.connect(copy_endpoint_to_clipboard_pressed)
	join_from_clipboard.pressed.connect(join_from_clipboard_pressed)
	endpoint.connection_received.connect(incoming_connection)


## the other window dialled us. this connection is already open — opened
## never fires for it — so we go straight to waiting for their stream
func incoming_connection(incoming_alpn: String, incoming_connection: IrohConnection) -> void:
	connection = incoming_connection
	connection.stream_opened.connect(func(their_stream: IrohStream) -> void:
		stream = their_stream)


func _process(_delta: float) -> void:
	if is_instance_valid(stream) and stream:
		stream.put_string("woof")
		# a length header might land before the text behind it does, so this
		# is a possible-not-guaranteed check, not a wait
		if stream.get_available_bytes() > 0:
			print("new message!!\n", stream.get_string())


func copy_endpoint_to_clipboard_pressed() -> void:
	DisplayServer.clipboard_set(endpoint.endpoint_id())


## we are the one dialling, so opened will fire for us, and we are the side
## that opens the stream
func join_from_clipboard_pressed() -> void:
	connection = endpoint.connect_to(DisplayServer.clipboard_get(), "awwooo")
	connection.opened.connect(func() -> void:
		stream = connection.open_stream())
