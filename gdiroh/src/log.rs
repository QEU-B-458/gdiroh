//! Console output for the crate.
//!
//! Every line carries a prefix from [`crate::puppy`], which keeps gdiroh easy to
//! pick out of a busy Godot console and keeps the flavour in one place.
//!
//! These are safe to call from runtime tasks: gdext routes printing through a
//! thread-safe path.
//!
//! They are also safe to call when there is no engine at all. Printing outside
//! the load/unload window panics inside gdext, which a background task or a
//! teardown thread can easily stray into, so every macro checks first — see
//! [`engine_is_up`].

/// Whether the Godot API can be touched at all.
///
/// False under `cargo test`, and false in the moments before load and after
/// unload. Printing anyway aborts the process, so lines that fall outside the
/// window are dropped rather than allowed to take the game with them.
pub(crate) fn engine_is_up() -> bool {
    godot::sys::is_initialized()
}

/// Ordinary progress worth showing the developer.
macro_rules! info {
    ($($arg:tt)*) => {
        if $crate::log::engine_is_up() {
            ::godot::prelude::godot_print!(
                "{} {}", $crate::puppy::BARK_INFO, format_args!($($arg)*)
            )
        }
    };
}

/// Something recoverable that still deserves attention. Named `warning` rather
/// than `warn`, which collides with the built-in attribute.
macro_rules! warning {
    ($($arg:tt)*) => {
        if $crate::log::engine_is_up() {
            ::godot::prelude::godot_warn!(
                "{} {}", $crate::puppy::BARK_WARN, format_args!($($arg)*)
            )
        }
    };
}

/// A failure. Report and carry on — never unwind into Godot.
macro_rules! error {
    ($($arg:tt)*) => {
        if $crate::log::engine_is_up() {
            ::godot::prelude::godot_error!(
                "{} {}", $crate::puppy::BARK_ERROR, format_args!($($arg)*)
            )
        }
    };
}

pub(crate) use {error, info, warning};
