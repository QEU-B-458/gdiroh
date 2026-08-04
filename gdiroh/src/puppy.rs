use godot::classes::INode3D;
use godot::classes::Node3D;
use godot::prelude::*;

/// Barks that lead every log line. Point these at empty strings to retire the
/// mascot — no call site needs to change.
pub(crate) const BARK_INFO: &str = "awoo~";
pub(crate) const BARK_WARN: &str = "grrr?";
pub(crate) const BARK_ERROR: &str = "arf! arf!";

/// The gdiroh mascot, and the cheapest way to prove the native library loaded.
///
/// Constructing one barks to the console, and [member happy] reads back from
/// GDScript — so if [code]IrohPuppy.new()[/code] runs, the extension is present
/// and callable before any networking is attempted.
///
/// To *check* for the library rather than assume it, the checking script must
/// name no gdiroh class directly — a script containing
/// [code]IrohPuppy.new()[/code] fails to compile when the library is missing,
/// so the guard would never run. Go through [ClassDB] strings instead:
///
/// ```gdscript
/// if not ClassDB.class_exists("IrohPuppy"):
///     push_error("gdiroh's native library did not load")
///     return
/// add_child(ClassDB.instantiate("IrohPuppy"))   # awwooo~
/// ```
#[derive(GodotClass)]
#[class(base=Node3D)]
struct IrohPuppy {
    /// Wags when true. Decorative, and readable from GDScript.
    #[var]
    happy: bool,
    base: Base<Node3D>,
}

#[godot_api]
impl INode3D for IrohPuppy {
    fn init(base: Base<Node3D>) -> Self {
        godot_print!("awwooo~");

        Self { happy: true, base }
    }
}
