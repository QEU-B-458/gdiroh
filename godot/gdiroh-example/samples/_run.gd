extends SceneTree

## runs one sample from the command line, headless or not:
##
##   godot --headless --path . -s samples/_run.gd -- stream.gd
##
## every sample is a plain Node script, so this just puts one on the tree and
## lets it play out. samples quit the tree themselves when they are done.


func _initialize() -> void:
	var args := OS.get_cmdline_user_args()
	if args.is_empty():
		push_error("which sample? pass a file name after --, like: -- stream.gd")
		quit(1)
		return
	var path := "res://samples/%s" % args[0]
	if not ResourceLoader.exists(path):
		push_error("no sample called %s" % args[0])
		quit(1)
		return
	var sample := Node.new()
	sample.set_script(load(path))
	root.add_child(sample)
