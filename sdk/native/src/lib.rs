//! Host-driven native SDK. Unsafe code is confined to the C entry points.
#![deny(unsafe_op_in_unsafe_fn)]

mod abi;
mod client;
mod codec;
mod history;
mod outbound;
mod view;

pub use abi::*;

use newim_sdk_core::store::StoreError;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
enum Status {
    Ok = 0,
    InvalidInput = 1,
    Closed = 2,
    Busy = 3,
    NoEvent = 4,
    BufferTooSmall = 5,
    StoreUnavailable = 6,
    InternalFailure = 7,
}

type Result<T> = std::result::Result<T, Status>;

impl From<StoreError> for Status {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::InvalidInput => Self::InvalidInput,
            StoreError::Busy => Self::Busy,
            _ => Self::StoreUnavailable,
        }
    }
}

fn id() -> Result<u64> {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .map_err(|_| Status::InternalFailure)
}
