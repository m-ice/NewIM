//! Notification taps request a refresh through the authenticated connection.

use crate::connection::{Connection, Effect, Event, Status};
use crate::store::StoreError;

pub const SYNC_MARKER: &str = "sync";

/// A host supplies the value of the platform payload's `newim` field. It
/// contains no message or conversation authorization.
pub fn open_notification(
    connection: &mut Connection,
    marker: &str,
    now_ms: u64,
    random: u64,
) -> Result<Vec<Effect>, StoreError> {
    if marker != SYNC_MARKER {
        return Ok(Vec::new());
    }
    let event = match connection.status() {
        Status::Closed | Status::AdvancingFence | Status::Credentials(_) => return Ok(Vec::new()),
        Status::Offline | Status::Idle | Status::Backoff { .. } => Event::Tick,
        Status::Directory
        | Status::Opening
        | Status::Subscribing
        | Status::Recovering
        | Status::Ready => Event::RefreshDirectory,
    };
    connection.update(event, now_ms, random)
}
