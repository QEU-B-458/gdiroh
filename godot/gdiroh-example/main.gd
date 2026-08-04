extends Control

## gdiroh demo — the hub.
##
## every tab on this screen is one use case, and every tab owns one endpoint
## of its own. starting a tab creates and binds its endpoint; stopping it
## drops the reference, which is what closes it. run one tab, or several at
## once — endpoints are instances, so one process holds as many as it needs
## and they do not touch each other. nothing here starts until a tab is
## started.
##
## the hub itself owns no endpoint. it reads the command line, shows the
## tabs, and under --demo follows the cue file two-peers.sh conducts with.
##
## [b]command line[/b]
##
## arguments go after a bare `--`, which separates godot's options from the
## game's:
##
##     --profile <name>   identity slot, so two copies on one machine stay apart
##     --random           throwaway identities instead of saved ones
##     --local            find peers on this network over mDNS
##     --no-dns           do not resolve ids through n0's DNS
##     --no-relay         refuse relays, direct paths only
##     --demo             follow cues from two-peers.sh
##     --cues <file>      the cue file the script appends to
##
## two copies on one machine share `user://`, so `--profile` is what stops
## them loading the same saved keys and coming up as the same peers. copies
## started without it — the editor's multi-instance run does that — can use
## the "Random id" toggle on each tab instead.

@export_group("Command line")

## honour the arguments above. off means a game drives everything itself
@export var allow_command_line := true

## holder for the command line arguments, read once at startup
var _args := PackedStringArray()
## identity slot name; every tab appends its own slug to this
var _profile := "default"

@onready var _tabs: TabContainer = $Layout/Tabs


## the command line is read here rather than in _ready because a parent
## enters the tree before its children — this way the tabs can already ask
## for flags while they are getting ready
func _enter_tree() -> void:
	_args = OS.get_cmdline_user_args() if allow_command_line else PackedStringArray()
	_profile = argument("--profile", "default")


func _ready() -> void:
	# the mascot: the smallest possible native call, made before any
	# networking. if this line runs, the library is loaded and callable.
	# (a script that needs to check for the library rather than assume it
	# must go through ClassDB strings — see IrohPuppy's class docs)
	var mascot := IrohPuppy.new()
	add_child(mascot)
	print("mascot happy: ", mascot.happy)

	# in the scripted run the two-peers.sh script decides what happens when.
	# here we just start following its cue file
	if flag("--demo") and not argument("--cues").is_empty():
		add_child(Conductor.new(argument("--cues"), _tabs))


# --- command line, read by the tabs through here ------------------------------


func flag(name: String) -> bool:
	return _args.has(name)


func argument(name: String, fallback := "") -> String:
	var at := _args.find(name)
	if at != -1 and at + 1 < _args.size():
		return _args[at + 1]
	return fallback


func profile() -> String:
	return _profile
