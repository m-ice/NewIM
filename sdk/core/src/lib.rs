//! NewIM SDK core for host-driven connectivity, local sync and message sending.

#![forbid(unsafe_code)]

pub mod build_info;
pub mod connection;
pub mod conversation_sync;
pub mod media;
pub mod message;
pub mod message_sync;
pub mod outbox;
pub mod store;
