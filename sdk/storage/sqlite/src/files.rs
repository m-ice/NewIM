use crate::StoreError;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
};

pub fn no_symlinks(path: &Path) -> Result<(), StoreError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| StoreError::Io)?
            .join(path)
    };
    let mut part = PathBuf::new();
    for component in absolute.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(StoreError::InvalidInput);
        }
        part.push(component);
        match fs::symlink_metadata(&part) {
            Ok(meta) if meta.file_type().is_symlink() => return Err(StoreError::InvalidInput),
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(StoreError::Io),
        }
    }
    Ok(())
}
pub(crate) struct RootLock(File);

impl Drop for RootLock {
    fn drop(&mut self) {
        // Explicit unlock releases the shared open-file description even if a
        // forked child still owns a duplicated descriptor.
        let _ = self.0.unlock();
    }
}

pub fn lock_root(root: &Path) -> Result<RootLock, StoreError> {
    no_symlinks(root)?;
    if !root.exists() {
        fs::create_dir_all(root).map_err(|_| StoreError::Io)?;
        fs::set_permissions(root, fs::Permissions::from_mode(0o700)).map_err(|_| StoreError::Io)?;
    }
    let meta = fs::metadata(root).map_err(|_| StoreError::Io)?;
    if !meta.is_dir() || meta.permissions().mode() & 0o077 != 0 {
        return Err(StoreError::InvalidInput);
    }
    let path = root.join("owner.lock");
    no_symlinks(&path)?;
    #[cfg(target_os = "macos")]
    let nofollow = 0x100;
    #[cfg(target_os = "linux")]
    let nofollow = 0x20000;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(nofollow)
        .open(path)
        .map_err(|_| StoreError::Io)?;
    lock.try_lock().map_err(|e| match e {
        std::fs::TryLockError::WouldBlock => StoreError::Busy,
        _ => StoreError::Io,
    })?;
    Ok(RootLock(lock))
}
pub fn safe_active(root: &Path) -> Result<PathBuf, StoreError> {
    for name in [
        "active",
        "active/store.db",
        "active/store.db-wal",
        "active/store.db-shm",
        "active/initialized.next",
        "active/recovery.next",
        "initialized",
        "RECOVERY",
    ] {
        no_symlinks(&root.join(name))?;
    }
    Ok(root.join("active"))
}
pub fn sync_dir(path: &Path) -> Result<(), StoreError> {
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(|_| StoreError::Io)
}
pub fn marker(path: &Path, data: &[u8]) -> Result<(), StoreError> {
    use std::io::Write;
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| StoreError::Io)?;
    f.write_all(data)
        .and_then(|_| f.sync_all())
        .map_err(|_| StoreError::Io)?;
    sync_dir(path.parent().ok_or(StoreError::InvalidInput)?)
}

/// Atomically publish a bounded marker; retain an interrupted conflicting temporary file.
pub(crate) fn replace_marker(path: &Path, temporary: &Path, data: &[u8]) -> Result<(), StoreError> {
    no_symlinks(path)?;
    no_symlinks(temporary)?;
    if path.is_file() && fs::read(path).map_err(|_| StoreError::Io)? == data {
        File::open(path)
            .and_then(|f| f.sync_all())
            .map_err(|_| StoreError::Io)?;
        return sync_dir(path.parent().ok_or(StoreError::InvalidInput)?);
    }
    if temporary.exists() {
        let metadata = fs::metadata(temporary).map_err(|_| StoreError::Io)?;
        if !metadata.is_file()
            || metadata.len() != data.len() as u64
            || fs::read(temporary).map_err(|_| StoreError::Io)? != data
        {
            return Err(StoreError::RecoveryRequired);
        }
        File::open(temporary)
            .and_then(|f| f.sync_all())
            .map_err(|_| StoreError::Io)?;
    } else {
        marker(temporary, data)?;
    }
    fs::rename(temporary, path).map_err(|_| StoreError::Io)?;
    sync_dir(temporary.parent().ok_or(StoreError::InvalidInput)?)?;
    sync_dir(path.parent().ok_or(StoreError::InvalidInput)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn dropping_root_lock_releases_cloned_open_description() {
        let base = std::env::temp_dir().canonicalize().unwrap();
        let root = base.join(format!(
            "newim-owner-lock-{}-{}",
            std::process::id(),
            TEST_ID.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = fs::remove_dir_all(&root);
        let lock = lock_root(&root).unwrap();
        let cloned = lock.0.try_clone().unwrap();
        drop(lock);

        let reacquired = lock_root(&root).expect("explicit unlock must release cloned descriptor");
        drop(reacquired);
        drop(cloned);
        fs::remove_dir_all(root).unwrap();
    }
}
