//! Process-local authorization at the storage mutation boundary, never replicated policy.

use super::state::KafStorageState;
use std::io;

/// Call under the state write lock, after all awaited work and before mutation.
pub(super) fn check_mutation(state: &KafStorageState) -> io::Result<()> {
    if !state.admission_required && cfg!(test) {
        return Ok(());
    }
    let session = state.admission.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "storage requires local admission",
        )
    })?;
    session
        .check()
        .map(|_| ())
        .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error))
}
