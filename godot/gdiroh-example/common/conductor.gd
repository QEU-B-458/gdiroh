class_name Conductor
extends Node

## follows cues from two-peers.sh during the scripted run.
##
## in the scripted run the shell script is the conductor: it watches both
## peers' console output and appends a line to a peer's cue file when it is
## time for that peer's next step. this node tails the cue file and performs
## each new line by calling the same functions the tab buttons call, so the
## scripted run exercises exactly the code a person clicking around does.
##
## a cue line is `<tab slug> <verb> [argument]`, or `quit` on its own.

## path of the cue file the shell script appends to
var _path := ""
## the tab container whose children cues are addressed to, found by slug
var _tabs: TabContainer
## how many cue lines have been performed already
var _performed := 0
## seconds until the next look at the cue file
var _cooldown := 0.0


func _init(path: String, tabs: TabContainer) -> void:
	_path = path
	_tabs = tabs


func _process(delta: float) -> void:
	# four polls a second is plenty — cues are human-scale steps
	_cooldown -= delta
	if _cooldown > 0.0:
		return
	_cooldown = 0.25

	# the file is reopened every time because the script keeps appending to it
	var file := FileAccess.open(_path, FileAccess.READ)
	if file == null:
		return
	var lines := file.get_as_text().split("\n", false)
	file.close()

	while _performed < lines.size():
		_perform(lines[_performed].strip_edges())
		_performed += 1


func _perform(line: String) -> void:
	if line.is_empty():
		return
	print("cue: %s" % line)

	if line == "quit":
		get_tree().quit()
		return

	# up to three parts: tab, verb, and the rest of the line as the argument
	var parts := line.split(" ", false, 2)
	if parts.size() < 2:
		push_error("a cue needs a tab and a verb: %s" % line)
		return

	for tab in _tabs.get_children():
		if tab is UseCaseTab and tab.slug == parts[0]:
			tab.cue(parts[1], parts[2] if parts.size() > 2 else "")
			return
	push_error("no tab called '%s'" % parts[0])
