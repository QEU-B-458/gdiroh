extends UseCaseTab

## use case: work out why a friend cannot reach you.
##
## when a connection will not form or keeps landing on a relay, the answers
## live on the endpoint itself: which addresses it is reachable on right now,
## which relays it calls home, and every counter iroh keeps. this tab reads
## them all back, which is the whole use case — nothing here talks to anyone.
##
## the readouts change as the network does, so refresh is a button rather
## than something read once and trusted forever.
##
## the `endpoint` everything below talks through is built by use_case_tab.gd
## when start is pressed — IrohEndpoint.new(), set_secret_key, bind — the
## same for every tab, so it lives there once instead of in each.

## seconds until the next automatic refresh of the summary line
var _cooldown := 0.0

@onready var _refresh_button: Button = $Row/Refresh
@onready var _metrics_button: Button = $Row/Metrics
@onready var _forget_button: Button = $Row/Forget
@onready var _summary: Label = $Summary


func _setup() -> void:
	_refresh_button.pressed.connect(refresh)
	_metrics_button.pressed.connect(metrics)
	_forget_button.pressed.connect(forget)
	gate([_refresh_button, _metrics_button])


func _on_started() -> void:
	log_panel.note("this endpoint's own view of its reachability")
	refresh()


## reads back how this endpoint can be reached right now
func refresh() -> void:
	var relays := endpoint.home_relays()
	var addresses := endpoint.direct_addresses()

	_summary.text = "bound: %s   closed: %s   relays: %d   addresses: %d" % [
		endpoint.is_bound(), endpoint.is_closed(), relays.size(), addresses.size()
	]
	log_panel.write("addresses: %s" % (
		", ".join(addresses) if addresses.size() > 0 else "none yet"
	))
	log_panel.write("home relays: %s" % (
		", ".join(relays) if relays.size() > 0 else "none — direct paths only"
	))
	# a ticket is only as good as the addresses inside it, so it is re-read
	# whenever they are
	ticket_field.set_value(endpoint.ticket())


## prints every counter iroh keeps, grouped. the set is whatever the linked
## iroh collects, so nothing here goes stale when iroh adds counters
func metrics() -> void:
	var groups := endpoint.get_metrics()
	if groups.is_empty():
		log_panel.fail("no metrics — is the endpoint bound?")
		return

	for group: String in groups.keys():
		var counters: Dictionary = groups[group]
		var line := ""
		var shown := 0
		for key: String in counters.keys():
			# each counter travels with a `__help` twin describing it; the
			# names alone are enough for a log line
			if key.ends_with("__help") or shown >= 6:
				continue
			line += "%s=%s  " % [key, counters[key]]
			shown += 1
		log_panel.write("[b]%s[/b] (%d counters) %s" % [group, counters.size() / 2, line])


## deletes this tab's saved identity, so its next persistent start gets a new
## id. the other tabs have slots of their own and are not touched
func forget() -> void:
	if Identity.forget("%s-%s" % [hub.profile(), slug]):
		log_panel.good("saved identity deleted — the next start is a new id")
	else:
		log_panel.note("no saved identity for this tab yet")


func _process(delta: float) -> void:
	# the summary line follows the network on its own; the log only moves
	# when asked
	_cooldown -= delta
	if endpoint == null or _cooldown > 0.0:
		return
	_cooldown = 2.0
	_summary.text = "bound: %s   closed: %s   relays: %d   addresses: %d" % [
		endpoint.is_bound(), endpoint.is_closed(),
		endpoint.home_relays().size(), endpoint.direct_addresses().size()
	]


func _teardown() -> void:
	_summary.text = "stopped"


func _cue(verb: String, arg: String) -> void:
	match verb:
		"refresh":
			refresh()
		"metrics":
			metrics()
		"forget":
			forget()
		_:
			super(verb, arg)
