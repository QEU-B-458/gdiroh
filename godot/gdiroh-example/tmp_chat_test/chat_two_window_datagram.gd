extends Node

@onready var copy_endpoint_to_clipboard: Button = %copy_endpoint_to_clipboard
@onready var join_from_clipboard: Button = %join_from_clipboard

var connection: IrohConnection
var endpoint := IrohEndpoint.new()

func _ready() -> void:
	endpoint.bind()
	endpoint.listen("awwooo")
	copy_endpoint_to_clipboard.pressed.connect(copy_endpoint_to_clipboard_pressed)
	join_from_clipboard.pressed.connect(join_from_clipboard_pressed)
	endpoint.connection_received.connect(incoming_connection)

func incoming_connection(incoming_alpn: String, incoming_connection: IrohConnection) -> void:
	connection = incoming_connection
	connection.datagram_received.connect(_on_message)

func _process(delta: float) -> void:
	if is_instance_valid(connection) and connection and connection.is_open():
		connection.send_datagram("woof".to_utf8_buffer())

func _on_message(data: PackedByteArray) -> void:
	print("new message!!\n", data.get_string_from_utf8())

func copy_endpoint_to_clipboard_pressed() -> void:
	DisplayServer.clipboard_set(endpoint.endpoint_id())

func join_from_clipboard_pressed() -> void:
	connection = endpoint.connect_to(DisplayServer.clipboard_get(), "awwooo")
	connection.datagram_received.connect(_on_message)
