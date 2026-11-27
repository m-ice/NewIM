//! NewIM SDK foundation. Build identity, media metadata and send outbox state are available; transport is not implemented.

#![forbid(unsafe_code)]

pub mod build_info;
pub mod conversation_sync;
pub mod media;
pub mod message;
pub mod message_sync;
pub mod outbox;
pub mod store;
