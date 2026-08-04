//! Turning fallible work into logged Godot errors.
//!
//! A Rust panic unwinding into Godot's C ABI is undefined behaviour and takes
//! the editor with it, so recoverable failures are reported and collapsed to
//! `None` rather than unwrapped.

use std::fmt::Display;

/// Logs an error with context and reduces the result to an [`Option`].
pub(crate) trait Report<T> {
    fn report(self, context: &str) -> Option<T>;
}

impl<T, E: Display> Report<T> for Result<T, E> {
    fn report(self, context: &str) -> Option<T> {
        match self {
            Ok(value) => Some(value),
            Err(err) => {
                crate::log::error!("{context}: {err}");
                None
            }
        }
    }
}
