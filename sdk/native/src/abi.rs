use crate::{Result, Status, client::Client, id};
use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    slice, str,
    sync::{Arc, LazyLock, Mutex, TryLockError},
};

const ABI_VERSION: u32 = 1;
pub(crate) const MAX_EVENT_BYTES: usize = 1024 * 1024;
const MAX_ROOT_BYTES: usize = 4096;
const MAX_ACCOUNT_BYTES: usize = 128;

type Entry = Arc<Mutex<Option<Client>>>;
static CLIENTS: LazyLock<Mutex<HashMap<u64, Entry>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn boundary(action: impl FnOnce() -> Result<Status>) -> i32 {
    match catch_unwind(AssertUnwindSafe(action)) {
        Ok(Ok(status) | Err(status)) => status as i32,
        Err(_) => Status::InternalFailure as i32,
    }
}

fn with_client(handle: u64, action: impl FnOnce(&mut Client) -> Result<Status>) -> Result<Status> {
    let client = CLIENTS
        .lock()
        .map_err(|_| Status::InternalFailure)?
        .get(&handle)
        .cloned()
        .ok_or(Status::Closed)?;
    let mut guard = client.try_lock().map_err(|error| match error {
        TryLockError::WouldBlock => Status::Busy,
        TryLockError::Poisoned(_) => Status::InternalFailure,
    })?;
    action(guard.as_mut().ok_or(Status::Closed)?)
}

unsafe fn input<'a>(pointer: *const u8, length: usize, max: usize) -> Result<&'a [u8]> {
    if pointer.is_null() || length == 0 || length > max {
        return Err(Status::InvalidInput);
    }
    // SAFETY: The caller supplies length readable bytes throughout the call.
    Ok(unsafe { slice::from_raw_parts(pointer, length) })
}

#[unsafe(no_mangle)]
pub extern "C" fn newim_native_abi_version() -> u32 {
    ABI_VERSION
}

/// # Safety
/// Input ranges within the size limits must be readable. Client must reference writable, aligned storage.
/// The output location must not overlap the inputs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn newim_native_open(
    root: *const u8,
    root_len: usize,
    account: *const u8,
    account_len: usize,
    client: *mut u64,
) -> i32 {
    boundary(|| {
        if client.is_null() || !client.is_aligned() {
            return Err(Status::InvalidInput);
        }
        // SAFETY: The caller provides aligned writable storage for the handle.
        unsafe { client.write(0) };
        // SAFETY: The caller supplies both readable input ranges.
        let root = unsafe { input(root, root_len, MAX_ROOT_BYTES) }?;
        let account = unsafe { input(account, account_len, MAX_ACCOUNT_BYTES) }?;
        let root = str::from_utf8(root).map_err(|_| Status::InvalidInput)?;
        let account = str::from_utf8(account).map_err(|_| Status::InvalidInput)?;
        let path = Path::new(root);
        if !path.is_absolute() || root.as_bytes().contains(&0) {
            return Err(Status::InvalidInput);
        }
        let instance = Client::open(path, account)?;
        let handle = id()?;
        CLIENTS
            .lock()
            .map_err(|_| Status::InternalFailure)?
            .insert(handle, Arc::new(Mutex::new(Some(instance))));
        // SAFETY: The handle location remains valid throughout the call.
        unsafe { client.write(handle) };
        Ok(Status::Ok)
    })
}

/// # Safety
/// Event must reference event_len readable bytes when that length is within the ABI limit.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn newim_native_submit(
    handle: u64,
    event: *const u8,
    event_len: usize,
) -> i32 {
    boundary(|| {
        with_client(handle, |client| {
            // SAFETY: The calling host supplies a readable event range.
            let event = unsafe { input(event, event_len, MAX_EVENT_BYTES) }?;
            client.submit(event)?;
            Ok(Status::Ok)
        })
    })
}

/// # Safety
/// Size must reference writable, properly aligned storage. Out must reference capacity writable
/// bytes when capacity is nonzero. The two outputs must not overlap.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn newim_native_next(
    handle: u64,
    out: *mut u8,
    capacity: usize,
    size: *mut usize,
) -> i32 {
    boundary(|| {
        if size.is_null() || !size.is_aligned() || (out.is_null() && capacity != 0) {
            return Err(Status::InvalidInput);
        }
        // SAFETY: The caller supplies aligned writable length storage.
        unsafe { size.write(0) };
        with_client(handle, |client| {
            let Some(event) = client.peek() else {
                return Ok(Status::NoEvent);
            };
            // SAFETY: The distinct length storage belongs to the caller.
            unsafe { size.write(event.len()) };
            if capacity < event.len() {
                return Ok(Status::BufferTooSmall);
            }
            // SAFETY: Capacity covers the event and the still-owned event cannot overlap out.
            unsafe { out.copy_from_nonoverlapping(event.as_ptr(), event.len()) };
            client.pop();
            Ok(Status::Ok)
        })
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn newim_native_close(handle: u64) -> i32 {
    boundary(|| {
        let mut registry = CLIENTS.lock().map_err(|_| Status::InternalFailure)?;
        let entry = registry.get(&handle).cloned().ok_or(Status::Closed)?;
        let mut guard = match entry.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => return Err(Status::Busy),
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
        };
        let client = guard.take();
        registry.remove(&handle);
        drop(guard);
        drop(registry);
        drop(client);
        Ok(Status::Ok)
    })
}
