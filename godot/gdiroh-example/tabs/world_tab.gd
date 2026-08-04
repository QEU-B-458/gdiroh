extends UseCaseTab

## use case: world state every peer can read and write.
##
## a document is a key value store that syncs itself: every peer holds the
## whole document and may write any key, and when two peers wrote the same
## key the later write wins. that suits spawn points, inventories and
## settings — it is not a transaction, so anything where peers must agree
## first belongs on a protocol of your own instead.
##
## create a document here and share its ticket; the other peer joins with it
## and sees the same keys. the ticket grants write access, and that cannot be
## taken back.
##
## the `endpoint` everything below talks through is built by use_case_tab.gd
## when start is pressed — IrohEndpoint.new(), set_secret_key, bind — the
## same for every tab, so it lives there once instead of in each.

## holder for the open document; the tab keeps it open by keeping this
var _doc: IrohDocument
## keys whose value has not arrived yet. a read can answer "not found" while
## the bytes are still on their way, so these are retried on content_ready
var _awaiting := {}

@onready var _create_button: Button = $Open/Create
@onready var _join_field: LineEdit = $Open/Ticket
@onready var _join_button: Button = $Open/Join
@onready var _key: LineEdit = $Edit/Key
@onready var _value: LineEdit = $Edit/Value
@onready var _set_button: Button = $Edit/Set
@onready var _read_button: Button = $Edit/Read
@onready var _delete_button: Button = $Edit/Delete
@onready var _keys_button: Button = $Query/Keys
@onready var _status_button: Button = $Query/Status
@onready var _authors_button: Button = $Query/Authors
@onready var _author_button: Button = $Query/NewAuthor
@onready var _doc_ticket: CopyField = $DocTicket


func _setup() -> void:
	_doc_ticket.set_caption("Doc ticket")
	_create_button.pressed.connect(create)
	_join_button.pressed.connect(func() -> void: join(_join_field.text.strip_edges()))
	_set_button.pressed.connect(_on_set)
	_read_button.pressed.connect(_on_read)
	_delete_button.pressed.connect(_on_delete)
	_keys_button.pressed.connect(func() -> void:
		if _doc != null:
			_doc.list_keys(""))
	_status_button.pressed.connect(func() -> void:
		if _doc != null:
			_doc.request_status())
	_authors_button.pressed.connect(func() -> void: endpoint.request_author_list())
	_author_button.pressed.connect(func() -> void: endpoint.create_author())
	_value.text_submitted.connect(func(_text: String) -> void: _on_set())
	gate([_create_button, _join_button, _authors_button, _author_button])
	_set_editing(false)


func _on_started() -> void:
	# author answers arrive as endpoint signals; a fresh start is a fresh
	# endpoint, so these are connected here each time
	endpoint.author_list.connect(func(authors: PackedStringArray) -> void:
		var names := PackedStringArray()
		for author in authors:
			names.append(short(author))
		log_panel.write("authors (%d): %s" % [
			authors.size(), ", ".join(names) if authors.size() > 0 else "none"
		]))
	endpoint.author_created.connect(func(author: String) -> void:
		log_panel.good("new author %s" % short(author, 12)))
	log_panel.note("create a document and share its ticket, or paste one and join")


## starts a new, empty document
func create() -> void:
	_adopt(endpoint.create_document(), "created")


## joins the document a ticket names and syncs with its peers
func join(ticket: String) -> void:
	if ticket.is_empty():
		log_panel.fail("paste a document ticket first")
		return
	_adopt(endpoint.join_document(ticket), "joined")


func _adopt(doc: IrohDocument, what: String) -> void:
	if doc == null:
		log_panel.fail("could not open the document")
		return
	if _doc != null:
		return

	_doc = doc
	_doc.opened.connect(func(_id: String) -> void:
		_set_editing(true)
		# the document knows its own id once open; the signal argument is
		# the same value
		log_panel.good("document %s: %s" % [what, short(_doc.get_id(), 12)])
		# a share ticket for others to join with. sharing with write access,
		# because this use case is a world everyone may edit
		_doc.share(true)
		# a joiner only learns what is already in the document by asking —
		# the entry signal covers writes from now on, not history
		_doc.list_keys(""))
	_doc.shared.connect(func(ticket: String) -> void:
		_doc_ticket.set_value(ticket)
		log_panel.note("doc_ticket %s" % ticket))
	_doc.entry.connect(_on_entry)
	_doc.value.connect(_on_value)
	_doc.keys.connect(_on_keys)
	_doc.status.connect(func(syncing: bool, subscribers: int, handles: int) -> void:
		log_panel.write("syncing: %s, subscribers: %d, handles: %d" % [
			syncing, subscribers, handles
		]))
	_doc.content_ready.connect(_on_content_ready)
	_doc.sync_finished.connect(func(peer: String) -> void:
		log_panel.note("synced with %s" % short(peer, 12)))
	_doc.closed.connect(func(reason: String) -> void:
		log_panel.fail("document closed: %s" % reason))


func _set_editing(on: bool) -> void:
	var editors: Array[Button] = [
		_set_button, _read_button, _delete_button, _keys_button, _status_button
	]
	for control in editors:
		control.disabled = not on
	_create_button.disabled = on
	_join_button.disabled = on


# --- editing ------------------------------------------------------------------


func _on_set() -> void:
	var key := _key.text.strip_edges()
	if _doc == null or key.is_empty():
		return
	_doc.set(key, _value.text.to_utf8_buffer())


func _on_read() -> void:
	var key := _key.text.strip_edges()
	if _doc != null and not key.is_empty():
		_doc.read(key)


## removes the key and everything under it — deletion is prefix-wide, which
## is how a whole area of the world gets cleared at once
func _on_delete() -> void:
	var key := _key.text.strip_edges()
	if _doc != null and not key.is_empty():
		_doc.delete_prefix(key)
		log_panel.write("deleted everything under '%s'" % key)


## fires for our own writes and for peers'. `from` is empty for our own
func _on_entry(key: String, _author: String, _hash: String, length: int, from: String) -> void:
	if from.is_empty():
		log_panel.write("wrote '%s' (%d bytes)" % [key, length])
	else:
		log_panel.good("%s set '%s' (%d bytes)" % [short(from, 12), key, length])
	# ask for the value — it does not necessarily arrive with the entry
	_doc.read(key)


func _on_value(key: String, data: PackedByteArray, found: bool) -> void:
	if found:
		_awaiting.erase(key)
		log_panel.write("'%s' = '%s'" % [key, data.get_string_from_utf8()])
	else:
		# either the key is unset, or its bytes are still coming. remembered
		# so content_ready below can retry instead of leaving it looking empty
		_awaiting[key] = true
		log_panel.note("'%s' has no value here yet" % key)


func _on_content_ready(_hash: String) -> void:
	for key: String in _awaiting.keys():
		_doc.read(key)


func _on_keys(prefix: String, entries: Array) -> void:
	# say what is there, then read it all — reading every existing key is
	# what makes a fresh joiner print the world it walked into
	var names := PackedStringArray()
	for entry: Dictionary in entries:
		names.append(str(entry["key"]))
	log_panel.write("keys under '%s' (%d): %s" % [
		prefix, entries.size(), ", ".join(names) if entries.size() > 0 else "none"
	])
	for entry: Dictionary in entries:
		_doc.read(entry["key"])


# --- lifecycle and cues -------------------------------------------------------


func _teardown() -> void:
	if _doc != null:
		_doc.leave()
		_doc = null
	_awaiting.clear()
	_doc_ticket.set_value("")
	_set_editing(false)


func _cue(verb: String, arg: String) -> void:
	match verb:
		"create":
			create()
		"join":
			join(arg)
		"set":
			# the first word is the key, the rest is the value
			var space := arg.find(" ")
			if space == -1:
				_key.text = arg
				_value.text = ""
			else:
				_key.text = arg.substr(0, space)
				_value.text = arg.substr(space + 1)
			_on_set()
		"read":
			_key.text = arg
			_on_read()
		"del":
			_key.text = arg
			_on_delete()
		"keys":
			if _doc != null:
				_doc.list_keys("")
		"status":
			if _doc != null:
				_doc.request_status()
		"authors":
			endpoint.request_author_list()
		"author":
			endpoint.create_author()
		_:
			super(verb, arg)
