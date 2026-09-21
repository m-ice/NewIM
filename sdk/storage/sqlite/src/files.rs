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
pub fn lock_root(root: &Path) -> Result<File, StoreError> {
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
    Ok(lock)
}
pub fn safe_active(root: &Path) -> Result<PathBuf, StoreError> {
    for name in [
        "active",
        "active/store.db",
        "active/store.db-wal",
        "active/store.db-shm",
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
