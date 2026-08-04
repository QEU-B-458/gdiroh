class_name LogPanel
extends VBoxContainer

## a scrolling log that writes to the screen and to the console at once.
##
## the screen copy renders BBCode; the console copy has the tags stripped,
## which is what lets `two-peers.sh` check what happened.

const _BBCODE := "\\[/?[a-z][a-z0-9=#, ]*\\]"

static var _tags := RegEx.create_from_string(_BBCODE)

## prefix put in front of every console line, so several tabs logging at
## once stay tellable apart
var source := ""

@onready var _output: RichTextLabel = $Output


func write(line: String) -> void:
	if is_node_ready():
		_output.append_text(line + "\n")

	var plain := _tags.sub(line, "", true)
	print(plain if source.is_empty() else "[%s] %s" % [source, plain])


## something went wrong, in red
func fail(line: String) -> void:
	write("[color=#ff6b6b]%s[/color]" % line)


## something worked, in green
func good(line: String) -> void:
	write("[color=#8bd450]%s[/color]" % line)


## background detail, dimmed
func note(line: String) -> void:
	write("[color=#9aa0a6]%s[/color]" % line)


func clear() -> void:
	if is_node_ready():
		_output.clear()
