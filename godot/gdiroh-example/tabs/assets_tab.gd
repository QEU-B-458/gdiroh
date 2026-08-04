extends UseCaseTab

## use case: hand an asset to another peer, verified.
##
## a blob is named by the hash of its bytes. the peer fetching it checks what
## arrives against that name, an interrupted transfer resumes instead of
## starting over, and a peer that already has the bytes skips the transfer
## entirely. this is how a game ships an avatar, a level file, or any other
## asset to the players who need it.
##
## publish makes a demo asset and prints its ticket; paste that ticket into
## the other peer's fetch box. the tag keeps the blob safe from garbage
## collection while anyone still wants it.
##
## the `endpoint` everything below talks through is built by use_case_tab.gd
## when start is pressed — IrohEndpoint.new(), set_secret_key, bind — the
## same for every tab, so it lives there once instead of in each.

const TAG := "gdiroh-example/asset"
const ASSET_SIZE := 256 * 1024
## past tense per operation, for the completion line the scripted run greps
const DONE := {
	"publish": "published", "fetch": "fetched", "import": "imported", "export": "exported"
}

## the transfer in flight. transfers are reference counted too, so this holds
## the current one open until it reports back
var _busy: IrohTransfer
## hash of the blob we last published or fetched
var _hash := ""

@onready var _publish_button: Button = $Actions/Publish
@onready var _fetch_field: LineEdit = $Actions/Ticket
@onready var _fetch_button: Button = $Actions/Fetch
@onready var _file_button: Button = $Store/AddFile
@onready var _export_button: Button = $Store/Export
@onready var _list_button: Button = $Store/ListBlobs
@onready var _tags_button: Button = $Store/ListTags
@onready var _untag_button: Button = $Store/Untag
@onready var _hash_field: CopyField = $Hash
@onready var _blob_ticket: CopyField = $BlobTicket
@onready var _progress: ProgressBar = $Progress


func _setup() -> void:
	_hash_field.set_caption("Hash")
	_blob_ticket.set_caption("Blob ticket")
	_publish_button.pressed.connect(publish)
	_fetch_button.pressed.connect(func() -> void: fetch(_fetch_field.text.strip_edges()))
	_file_button.pressed.connect(add_file)
	_export_button.pressed.connect(export_blob)
	_list_button.pressed.connect(func() -> void: endpoint.request_blob_list())
	_tags_button.pressed.connect(func() -> void: endpoint.request_tag_list())
	_untag_button.pressed.connect(untag)
	gate([
		_publish_button, _fetch_button, _file_button,
		_export_button, _list_button, _tags_button, _untag_button,
	])


func _on_started() -> void:
	# the store's answers arrive as endpoint signals, connected once per tab
	# start because a fresh start means a fresh endpoint
	endpoint.blob_list.connect(func(hashes: PackedStringArray) -> void:
		log_panel.write("store holds %d blob(s): %s" % [hashes.size(), _shorten(hashes)]))
	endpoint.tag_list.connect(func(tags: PackedStringArray) -> void:
		log_panel.write("tags (%d): %s" % [
			tags.size(), ", ".join(tags) if tags.size() > 0 else "none"
		]))
	log_panel.note("publish here, then paste the blob ticket into the other peer's fetch box")


## makes a 256 KiB demo asset and adds it to the store
func publish() -> void:
	# a repeating pattern rather than zeroes, so what comes out the other end
	# is worth comparing
	var chunk := PackedByteArray()
	chunk.resize(1024)
	for i in 1024:
		chunk[i] = i % 251
	var payload := PackedByteArray()
	for _i in ASSET_SIZE / 1024:
		payload.append_array(chunk)

	_watch(endpoint.add_bytes(payload, TAG), "publish")


## fetches the blob a ticket names, from the peer the ticket names
func fetch(ticket: String) -> void:
	if ticket.is_empty():
		log_panel.fail("paste a blob ticket first")
		return
	# a blob ticket carries the provider's addresses as well as the hash, so
	# fetching needs no lookup service
	_watch(endpoint.fetch_blob_ticket(ticket, TAG), "fetch")


## stores an actual file. written first so there is always one to store; any
## `user://` or `res://` path works, gdiroh converts it
func add_file() -> void:
	#var path := "user://gdiroh_demo_asset.bin"
	#var file := FileAccess.open(path, FileAccess.WRITE)
	#if file == null:
		#log_panel.fail("could not write %s" % path)
		#return
	#file.store_string("gdiroh demo asset\n".repeat(512))
	#file.close()
	var file_dialog := FileDialog.new()
	file_dialog.file_mode = FileDialog.FILE_MODE_OPEN_FILE
	file_dialog.use_native_dialog = true
	file_dialog.show()
	file_dialog.file_selected.connect(add_file_selected)

func add_file_selected(path: String):
	_watch(endpoint.add_file(path, TAG), "import")


## writes the last blob out to disk, streaming rather than building it in
## memory first
func export_blob() -> void:
	if _hash.is_empty():
		log_panel.fail("nothing to export — publish or fetch something first")
		return
	var file_dialog := FileDialog.new()
	file_dialog.file_mode = FileDialog.FILE_MODE_SAVE_FILE
	file_dialog.use_native_dialog = true
	file_dialog.show()
	file_dialog.file_selected.connect(export_blob_selected)

func export_blob_selected(path: String) -> void:
	_watch(endpoint.export_blob(_hash, path), "export")


## removes the demo tag. once nothing names a blob, garbage collection may
## reclaim it — this is how a game frees space
func untag() -> void:
	endpoint.untag_blob(TAG)
	log_panel.write("removed tag '%s'" % TAG)


## follows one transfer to its end. progress feeds the bar; completion fills
## in the hash and the ticket other peers can fetch with
func _watch(transfer: IrohTransfer, what: String) -> void:
	if transfer == null:
		log_panel.fail("could not start the %s" % what)
		return

	_busy = transfer
	_progress.value = 0
	log_panel.write("%s…" % what)

	transfer.progress.connect(func(done: int, total: int) -> void:
		_progress.max_value = maxi(maxi(total, done), 1)
		_progress.value = done)
	transfer.completed.connect(func(hash: String, _data: PackedByteArray) -> void:
		_hash = hash
		_hash_field.set_value(hash)
		_blob_ticket.set_value(endpoint.blob_ticket(hash))
		_progress.value = _progress.max_value
		log_panel.good("%s %s" % [DONE.get(what, what), short(hash, 12)])
		log_panel.note("blob_ticket %s" % endpoint.blob_ticket(hash))
		# asking the store afterwards is the proof the bytes really are all
		# here — and prints the size, which the scripted run checks
		endpoint.request_blob_status(hash))
	transfer.failed.connect(func(reason: String) -> void:
		log_panel.fail("%s failed: %s" % [what, reason]))

	if not endpoint.blob_status.is_connected(_on_blob_status):
		endpoint.blob_status.connect(_on_blob_status)


func _on_blob_status(hash: String, present: bool, complete: bool, size: int) -> void:
	log_panel.write("status %s present=%s complete=%s size=%d" % [
		short(hash, 12), present, complete, size
	])


func _shorten(values: PackedStringArray) -> String:
	if values.is_empty():
		return "none"
	var out := PackedStringArray()
	for value in values:
		out.append(short(value))
	return ", ".join(out)


func _teardown() -> void:
	_busy = null
	_hash = ""
	_progress.value = 0


func _cue(verb: String, arg: String) -> void:
	match verb:
		"publish":
			publish()
		"fetch":
			fetch(arg)
		"file":
			# the button opens a picker, which a scripted run cannot click.
			# the cue writes a demo file itself and enters at the same place
			# the picker's selection does
			var path := "user://gdiroh_demo_asset.bin"
			var file := FileAccess.open(path, FileAccess.WRITE)
			if file == null:
				log_panel.fail("could not write %s" % path)
				return
			file.store_string("gdiroh demo asset\n".repeat(512))
			file.close()
			add_file_selected(path)
		"export":
			export_blob_selected("user://gdiroh_exported.bin")
		"list":
			endpoint.request_blob_list()
		"tags":
			endpoint.request_tag_list()
		"untag":
			untag()
		_:
			super(verb, arg)
