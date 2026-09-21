use crate::{StoreError, files};
use newim_sdk_core::store::{Action, Fence, Request, validate_request};
use std::{fs, path::Path};

const LEGACY: &[u8] = b"NewIM LocalStore v1\n";
const HEADER: &str = "NewIM LocalStore identity v1";
pub(crate) struct Identity {
    pub account: String,
    pub instance: String,
}
pub(crate) fn read(root: &Path) -> Result<Option<Identity>, StoreError> {
    let path = root.join("initialized");
    let bytes = match fs::metadata(&path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(StoreError::Io),
        Ok(m) => {
            if !m.is_file() || m.len() > 400 {
                return Err(StoreError::RecoveryRequired);
            }
            fs::read(path).map_err(|_| StoreError::Io)?
        }
    };
    if bytes == LEGACY {
        return Ok(None);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| StoreError::RecoveryRequired)?;
    let mut lines = text.split('\n');
    if lines.next() != Some(HEADER) {
        return Err(StoreError::RecoveryRequired);
    }
    let account = lines.next().ok_or(StoreError::RecoveryRequired)?.to_owned();
    let instance = lines.next().ok_or(StoreError::RecoveryRequired)?.to_owned();
    if lines.next() != Some("") || lines.next().is_some() {
        return Err(StoreError::RecoveryRequired);
    }
    let request = Request {
        fence: Fence {
            account: account.clone(),
            instance: instance.clone(),
            generation: 1,
        },
        operation_id: 1,
        action: Action::Metrics,
    };
    validate_request(&request).map_err(|_| StoreError::RecoveryRequired)?;
    Ok(Some(Identity { account, instance }))
}
/// Only the invocation that exclusively created the directory can initialize schema zero.
pub(crate) fn prepare(
    root: &Path,
    active: &Path,
    recovery: bool,
    account: &str,
) -> Result<(bool, Option<Identity>), StoreError> {
    let identity = read(root)?;
    if identity.as_ref().is_some_and(|id| id.account != account) {
        return Err(StoreError::StaleGeneration);
    }
    if active.exists() {
        if !active.is_dir() {
            return Err(StoreError::RecoveryRequired);
        }
        let db = fs::metadata(active.join("store.db")).map_err(|_| StoreError::RecoveryRequired)?;
        if !db.is_file() || db.len() < 100 {
            return Err(StoreError::RecoveryRequired);
        }
        return Ok((false, identity));
    }
    if root.join("initialized").exists() && !recovery {
        return Err(StoreError::RecoveryRequired);
    }
    fs::create_dir(active).map_err(|_| StoreError::Io)?;
    files::sync_dir(root)?;
    Ok((true, identity))
}
pub(crate) fn publish(root: &Path, account: &str, instance: &str) -> Result<(), StoreError> {
    let bytes = format!("{HEADER}\n{account}\n{instance}\n");
    files::replace_marker(
        &root.join("initialized"),
        &root.join("active/initialized.next"),
        bytes.as_bytes(),
    )
}
