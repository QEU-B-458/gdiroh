class_name UseCaseTab
extends VBoxContainer

## base for the use case tabs. each tab owns one endpoint of its own.
##
## start builds and binds the tab's endpoint, stop drops every reference to
## it, and dropping the references is what closes it — an endpoint is
## reference counted like any RefCounted, so the member variable below is
## exactly what keeps it alive. because each tab has its own endpoint, two
## tabs running at once really are two independent endpoints in one process,
## and stopping one tab leaves the others untouched.

## short name for this tab. used for the identity slot, the store directory,
## the log prefix, and to address cues in the scripted run
@export var slug := "tab"

## holder for this tab's endpoint. this reference is what keeps the endpoint
## alive — in a local variable it would close when the function returned
var endpoint: IrohEndpoint

## controls that only work once the endpoint is bound; disabled the rest of
## the time
var _gated: Array[Control] = []

## the hub (main.gd). we ask it for command line flags and the profile name
@onready var hub := owner

@onready var start_button: Button = $Lifecycle/Start
@onready var stop_button: Button = $Lifecycle/Stop
## start with a throwaway identity instead of this tab's saved one. the way
## out when extra copies share `user://` without `--profile` — the editor's
## multi-instance run starts copies exactly like that, and without this they
## all load the same keys and come up as the same peer
@onready var random_toggle: CheckBox = $Lifecycle/Random
@onready var status: Label = $Lifecycle/Status
@onready var id_field: CopyField = $Id
@onready var ticket_field: CopyField = $Ticket
@onready var log_panel: LogPanel = $Log


func _ready() -> void:
	log_panel.source = slug
	id_field.set_caption("This tab's id")
	ticket_field.set_caption("Ticket")
	start_button.pressed.connect(start)
	stop_button.pressed.connect(stop)
	# --random is the same choice made from the command line
	random_toggle.button_pressed = hub.flag("--random")
	_setup()
	_set_running(false)


## builds this tab's endpoint and starts binding it
func start() -> void:
	if endpoint != null:
		return

	endpoint = IrohEndpoint.new()

	# each tab gets its own identity slot, so no two tabs ever share an id.
	# the toggle switches to a throwaway key instead of the saved one
	var kind := Identity.Kind.RANDOM if random_toggle.button_pressed else Identity.Kind.PERSISTENT
	endpoint.set_secret_key(Identity.secret_key(kind, "%s-%s" % [hub.profile(), slug]))

	# transport choices go before the bind, because a live endpoint keeps
	# what it was bound with
	if hub.flag("--no-dns"):
		endpoint.set_dns_lookup(false)
	if hub.flag("--no-relay"):
		endpoint.set_relay_mode(IrohEndpoint.RELAY_DISABLED)
	if hub.flag("--local"):
		# a service name of our own keeps this demo from finding unrelated
		# gdiroh games on the same network
		endpoint.set_local_discovery(true)
		endpoint.set_local_discovery_service("gdiroh-example")

	# a tab that stores blobs or documents gets a store directory of its own,
	# per profile and per tab, so no two endpoints open the same database
	if _wants_store():
		endpoint.set_blob_store_path("user://gdiroh_store_%s_%s" % [hub.profile(), slug])

	endpoint.bound.connect(_on_bound)
	endpoint.bind_failed.connect(_on_bind_failed)
	endpoint.bind()

	status.text = "binding…"
	start_button.disabled = true
	# stop is allowed while the bind is still in flight — releasing a binding
	# endpoint just abandons the bind
	stop_button.disabled = false
	# the identity was read at the line above; flipping the toggle now would
	# do nothing until the next start, so it locks with the endpoint
	random_toggle.disabled = true


## drops the endpoint and everything the use case built on it
func stop() -> void:
	if endpoint == null:
		return

	# the use case lets go of its connections, topics, documents and so on
	# first, so the reference below really is the last one
	_teardown()
	endpoint = null

	id_field.set_value("")
	ticket_field.set_value("")
	status.text = "stopped"
	_set_running(false)
	log_panel.write("endpoint released")


func _on_bound(id: String) -> void:
	# asking the endpoint rather than trusting the signal argument — the two
	# are the same, and the field should show what the endpoint says
	id_field.set_value(endpoint.endpoint_id())
	ticket_field.set_value(endpoint.ticket())
	status.text = "listening as %s…" % short(id)
	_set_running(true)

	log_panel.good("listening as %s" % id)
	# the full ticket goes to the console too, so the scripted run can hand it
	# to the other peer the way a person would paste it
	log_panel.note("ticket %s" % endpoint.ticket())
	_on_started()


func _on_bind_failed(reason: String) -> void:
	endpoint = null
	status.text = "bind failed"
	_set_running(false)
	log_panel.fail("could not bind: %s" % reason)


func _set_running(on: bool) -> void:
	start_button.disabled = on
	stop_button.disabled = not on
	random_toggle.disabled = on
	for control in _gated:
		if is_instance_valid(control) and "disabled" in control:
			control.set("disabled", not on)


## marks controls that should stay disabled until the endpoint is bound
func gate(controls: Array) -> void:
	for control in controls:
		if control is Control:
			_gated.append(control)


## shortens an id or hash for a log line; the full value stays copyable in the
## field it came from
func short(value: String, size: int = 8) -> String:
	return value.substr(0, size) if value.length() > size else value


## true when a pasted id or ticket points back at this tab's own endpoint —
## the commonest paste mistake two windows side by side invite
func is_own(target: String) -> bool:
	var id := target
	if target.begins_with("endpoint"):
		id = endpoint.remember_peer(target)
	return not id.is_empty() and id == endpoint.endpoint_id()


## entry point for the scripted run. the conductor feeds tabs the same actions
## a person would click, as `<verb> [argument]`
func cue(verb: String, arg: String) -> void:
	match verb:
		"start":
			start()
		"stop":
			stop()
		_:
			_cue(verb, arg)


# --- what a use case fills in -------------------------------------------------


## wire buttons and set captions here; runs once, at ready
func _setup() -> void:
	pass


## the endpoint has bound. claim protocols and subscribe from here, because
## none of that exists before a bind
func _on_started() -> void:
	pass


## drop every connection, topic, document or transfer built on the endpoint;
## runs right before the endpoint reference itself goes
func _teardown() -> void:
	pass


## true for tabs that keep blobs or documents on disk
func _wants_store() -> bool:
	return false


## use case specific cues; the base class has already handled start and stop
func _cue(verb: String, arg: String) -> void:
	log_panel.fail("unknown cue: %s %s" % [verb, arg])
