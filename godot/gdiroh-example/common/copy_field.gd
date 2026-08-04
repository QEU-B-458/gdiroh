class_name CopyField
extends HBoxContainer

## a read-only value with a button that copies it to the clipboard.
##
## endpoint ids, tickets and hashes are long and unselectable in a Label, so
## every place that shows one uses this instead of asking people to retype it.

signal copied(value: String)

var _pending_caption := ""
var _pending_value := ""

@onready var _caption: Label = $Caption
@onready var _value: LineEdit = $Value
@onready var _button: Button = $Copy


func _ready() -> void:
	_button.pressed.connect(_on_copy_pressed)
	if not _pending_caption.is_empty():
		_caption.text = _pending_caption
	if not _pending_value.is_empty():
		set_value(_pending_value)


## names the field, e.g. "Your id"
func set_caption(text: String) -> void:
	_pending_caption = text
	if is_node_ready():
		_caption.text = text


func set_value(text: String) -> void:
	_pending_value = text
	if not is_node_ready():
		return
	_value.text = text
	_value.tooltip_text = text
	_button.disabled = text.is_empty()


func get_value() -> String:
	return _value.text if is_node_ready() else _pending_value


func _on_copy_pressed() -> void:
	var text := _value.text
	if text.is_empty():
		return
	DisplayServer.clipboard_set(text)
	copied.emit(text)

	# a brief confirmation, so a click that did something looks different
	# from one that did not
	_button.text = "Copied"
	await get_tree().create_timer(0.8).timeout
	if is_instance_valid(_button):
		_button.text = "Copy"
