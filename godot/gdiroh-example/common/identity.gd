class_name Identity
extends RefCounted

## where a peer's secret key comes from.
##
## gdiroh never writes an identity to disk — where a key lives and what format
## it takes is the game's choice. this helper offers the two kinds a game
## realistically wants:
##
## [b]persistent[/b] keeps the same key in `user://`, so the peer id survives
## restarts and friends who saved your id can still reach you tomorrow.
##
## [b]random[/b] makes a fresh key each run, so the peer id changes every
## time. right for throwaway sessions — and worth testing against, because a
## game that assumes a stable id breaks only for these players.

enum Kind { PERSISTENT, RANDOM }

const _KEY_BYTES := 32


## loads or makes a key of the requested kind. `profile` names the slot,
## which is what lets two copies on one machine avoid sharing an identity
static func secret_key(kind: Kind, profile: String) -> PackedByteArray:
	if kind == Kind.RANDOM:
		return IrohEndpoint.generate_secret_key()

	var path := path_for(profile)
	if FileAccess.file_exists(path):
		var file := FileAccess.open(path, FileAccess.READ)
		if file != null:
			var saved := file.get_buffer(_KEY_BYTES)
			file.close()
			if saved.size() == _KEY_BYTES:
				return saved

	var fresh := IrohEndpoint.generate_secret_key()
	var out := FileAccess.open(path, FileAccess.WRITE)
	if out != null:
		out.store_buffer(fresh)
		out.close()
	return fresh


static func path_for(profile: String) -> String:
	var slot := profile.strip_edges()
	if slot.is_empty():
		slot = "default"
	return "user://gdiroh_%s.key" % slot


## forgets a saved identity, so the next persistent bind makes a new one
static func forget(profile: String) -> bool:
	var path := path_for(profile)
	if not FileAccess.file_exists(path):
		return false
	return DirAccess.remove_absolute(ProjectSettings.globalize_path(path)) == OK
