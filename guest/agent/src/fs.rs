use crate::{
    number,
    server::{Input, Output, next_input},
    string,
    wire::CHUNK,
};
use anyhow::{Result, bail, ensure};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, percent_encode};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::{CString, OsStr},
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        },
    },
    path::{Component, Path, PathBuf},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const TRANSFER_LIMIT: u64 = 1024 * 1024 * 1024;

// Integration-only scheduling is compiled out of the normal agent entirely.
#[cfg(feature = "filesystem-fixtures")]
pub mod fixtures {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;

    static NEXT: Mutex<Option<(PathBuf, Operation)>> = Mutex::new(None);

    pub struct Snapshot {
        pub path: PathBuf,
        pub device: u64,
        pub inode: u64,
        pub links: u64,
        pub uid: u32,
        pub gid: u32,
    }

    pub struct Fixture {
        pub scratch: Arc<tempfile::TempDir>,
        reached: oneshot::Receiver<Snapshot>,
        release: Option<oneshot::Sender<()>>,
    }

    pub(crate) struct Operation {
        pub scratch: Arc<tempfile::TempDir>,
        reached: oneshot::Sender<Snapshot>,
        release: oneshot::Receiver<()>,
    }

    // A registration owns a new private spool and matches exactly one tree pull.
    pub fn register(target: PathBuf, identity: (u32, u32)) -> Result<Fixture> {
        let scratch = Arc::new(tempfile::tempdir()?);
        let name = CString::new(scratch.path().as_os_str().as_bytes())?;
        ensure!(
            unsafe { libc::chown(name.as_ptr(), identity.0, identity.1) } == 0,
            "fixture scratch ownership failed"
        );
        std::fs::set_permissions(scratch.path(), std::fs::Permissions::from_mode(0o700))?;
        let (reached_tx, reached) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let mut next = NEXT.lock().unwrap();
        ensure!(next.is_none(), "fixture registration already occupied");
        *next = Some((
            target,
            Operation {
                scratch: scratch.clone(),
                reached: reached_tx,
                release: release_rx,
            },
        ));
        Ok(Fixture {
            scratch,
            reached,
            release: Some(release),
        })
    }

    impl Fixture {
        pub async fn reached(&mut self) -> Result<Snapshot> {
            Ok((&mut self.reached).await?)
        }
        pub fn release(&mut self) {
            self.release.take();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let mut next = NEXT.lock().unwrap();
            if next
                .as_ref()
                .is_some_and(|(_, operation)| Arc::ptr_eq(&operation.scratch, &self.scratch))
            {
                next.take();
            }
        }
    }

    pub(crate) fn take(target: &Path) -> Option<Operation> {
        let mut next = NEXT.lock().unwrap();
        if next.as_ref().is_some_and(|(path, _)| path == target) {
            next.take().map(|(_, operation)| operation)
        } else {
            None
        }
    }

    impl Operation {
        pub async fn checkpoint(self, file: &File) -> Result<()> {
            let metadata = file.metadata()?;
            let path = std::fs::read_link(descriptor_path(file))?;
            let bytes = path.as_os_str().as_bytes();
            let path = PathBuf::from(OsStr::from_bytes(
                bytes.strip_suffix(b" (deleted)").unwrap_or(bytes),
            ));
            ensure!(
                path.parent() == Some(self.scratch.path()),
                "fixture spool escaped scratch"
            );
            self.reached
                .send(Snapshot {
                    path,
                    device: metadata.dev(),
                    inode: metadata.ino(),
                    links: metadata.nlink(),
                    uid: metadata.uid(),
                    gid: metadata.gid(),
                })
                .map_err(|_| anyhow::anyhow!("fixture observer closed"))?;
            let _ = self.release.await;
            Ok(())
        }
    }
}

pub fn atomic_write(path: &Path, data: &[u8], mode: u32) -> Result<()> {
    let parent = parent(path)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.as_file()
        .set_permissions(std::fs::Permissions::from_mode(mode & 0o777))?;
    temp.write_all(data)?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|e| e.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
fn parent(path: &Path) -> Result<&Path> {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .ok_or_else(|| anyhow::anyhow!("path needs a parent directory"))
}
fn path(p: &serde_json::Value) -> Result<PathBuf> {
    let path = PathBuf::from(string(p, "path")?);
    ensure!(path.is_absolute(), "path must be absolute");
    ensure!(
        !path.components().any(|c| matches!(c, Component::ParentDir)),
        "parent traversal is invalid"
    );
    Ok(path)
}
fn resolved_path(path: &Path) -> Result<PathBuf> {
    ensure!(
        path.is_absolute() && !path.components().any(|c| matches!(c, Component::ParentDir)),
        "invalid filesystem path"
    );
    let mut existing = path;
    let mut suffix = Vec::new();
    while !existing.try_exists()? {
        suffix.push(
            existing
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("invalid path"))?,
        );
        existing = parent(existing)?;
    }
    let mut resolved = existing.canonicalize()?;
    for part in suffix.into_iter().rev() {
        resolved.push(part);
    }
    Ok(resolved)
}

fn public_path(path: &Path, private: &Path, destructive_tree: bool) -> Result<()> {
    let resolved = resolved_path(path)?;
    ensure!(
        !resolved.starts_with(private) && !(destructive_tree && private.starts_with(&resolved)),
        "agent private state is not accessible through the filesystem API"
    );
    Ok(())
}

fn metadata(path: &Path) -> Result<serde_json::Value> {
    let m = std::fs::symlink_metadata(path)?;
    Ok(
        json!({"path":path.to_string_lossy(),"size":m.len().to_string(),"mode":m.mode() & 0o7777,"uid":m.uid(),"gid":m.gid(),"mtime_seconds":m.mtime(),"directory":m.is_dir(),"symlink":m.file_type().is_symlink()}),
    )
}

// setfsuid/setfsgid via raw syscalls affect only this blocking worker thread.
// No async suspension or runtime thread runs with borrowed filesystem identity.
fn with_identity<T>(identity: (u32, u32), operation: impl FnOnce() -> Result<T>) -> Result<T> {
    struct Restore {
        uid: u32,
        gid: u32,
        groups: Option<Vec<libc::gid_t>>,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            unsafe {
                libc::syscall(libc::SYS_setfsuid, self.uid);
                libc::syscall(libc::SYS_setfsgid, self.gid);
                let groups_ok = self.groups.as_ref().is_none_or(|groups| {
                    libc::syscall(libc::SYS_setgroups, groups.len(), groups.as_ptr()) == 0
                });
                let restored_uid = libc::syscall(libc::SYS_setfsuid, u32::MAX) as u32;
                let restored_gid = libc::syscall(libc::SYS_setfsgid, u32::MAX) as u32;
                if !groups_ok || restored_uid != self.uid || restored_gid != self.gid {
                    std::process::abort();
                }
            }
        }
    }
    let groups = if unsafe { libc::geteuid() } == 0 {
        let n = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
        ensure!(n >= 0, "cannot read supplementary groups");
        let mut groups = vec![0; n as usize];
        ensure!(
            unsafe { libc::getgroups(n, groups.as_mut_ptr()) } >= 0,
            "cannot read supplementary groups"
        );
        ensure!(
            unsafe { libc::syscall(libc::SYS_setgroups, 0, std::ptr::null::<libc::gid_t>()) } == 0,
            "cannot clear filesystem supplementary groups"
        );
        Some(groups)
    } else {
        None
    };
    let gid = unsafe { libc::syscall(libc::SYS_setfsgid, identity.1) } as u32;
    let uid = unsafe { libc::syscall(libc::SYS_setfsuid, identity.0) } as u32;
    let _restore = Restore { uid, gid, groups };
    let current_uid = unsafe { libc::syscall(libc::SYS_setfsuid, u32::MAX) } as u32;
    let current_gid = unsafe { libc::syscall(libc::SYS_setfsgid, u32::MAX) } as u32;
    ensure!(
        current_uid == identity.0 && current_gid == identity.1,
        "cannot assume workload filesystem identity"
    );
    operation()
}
async fn access<T: Send + 'static>(
    identity: (u32, u32),
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(move || with_identity(identity, operation)).await?
}

pub async fn dispatch(
    agent: &std::sync::Arc<crate::Agent>,
    out: &Output,
    mut input: Input,
) -> Result<()> {
    let identity = (agent.config.uid, agent.config.gid);
    let p = out.request.params_json();
    let target = path(&p)?;
    let private = agent.ledger.directory.clone();
    public_path(
        &target,
        &private,
        matches!(
            out.request.method.as_str(),
            "fs_rm" | "fs_rename" | "tree_push"
        ),
    )?;
    if out.request.method == "fs_rename" {
        public_path(Path::new(string(&p, "destination")?), &private, true)?;
    }
    match out.request.method.as_str() {
        "fs_read" => {
            // O_NONBLOCK keeps a writer-less FIFO or device from stalling the
            // RPC slot; the type check runs on the acquired descriptor itself.
            let file = access(identity, move || {
                let file = OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(&target)?;
                ensure!(file.metadata()?.is_file(), "read requires a regular file");
                Ok(file)
            })
            .await?;
            let mut file = tokio::fs::File::from_std(file);
            let offset = if p.get("offset").is_some() {
                number(&p, "offset")?
            } else {
                0
            };
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let mut remaining = if p.get("length").is_some() {
                number(&p, "length")?
            } else {
                u64::MAX
            };
            let mut buf = vec![0; CHUNK];
            let mut size = 0u64;
            while remaining > 0 {
                let limit = remaining.min(CHUNK as u64) as usize;
                let n = tokio::select! {_ = out.cancel.cancelled() => bail!("connection closed"), result = file.read(&mut buf[..limit]) => result?};
                if n == 0 {
                    break;
                }
                out.event(
                    json!({"offset":(offset+size).to_string()}),
                    buf[..n].to_vec(),
                )
                .await?;
                size += n as u64;
                remaining -= n as u64;
            }
            out.done(json!({"bytes":size.to_string()})).await
        }
        "fs_write" => {
            let mode = if p.get("mode").is_some() {
                number(&p, "mode")? as u32
            } else {
                0o600
            };
            let location = target.clone();
            let temp = access(identity, move || {
                Ok(tempfile::NamedTempFile::new_in(parent(&location)?)?)
            })
            .await?;
            temp.as_file()
                .set_permissions(std::fs::Permissions::from_mode(mode & 0o777))?;
            let size = receive(out, &mut input, temp.as_file()).await?;
            temp.as_file().sync_all()?;
            access(identity, move || {
                temp.persist(&target).map_err(|e| e.error)?;
                File::open(parent(&target)?)?.sync_all()?;
                Ok(())
            })
            .await?;
            out.done(json!({"bytes":size.to_string()})).await
        }
        "tree_push" => {
            let location = target.clone();
            let archive = access(identity, move || {
                Ok(tempfile::NamedTempFile::new_in(parent(&location)?)?)
            })
            .await?;
            receive(out, &mut input, archive.as_file()).await?;
            let token = out.cancel.clone();
            access(identity, move || {
                push_tree(&target, archive.path(), &token, Some(&private))
            })
            .await?;
            out.done(json!({})).await
        }
        "tree_pull" => {
            // Spooling bounds memory and permits a known-safe archive before exposing bytes.
            #[cfg(feature = "filesystem-fixtures")]
            let fixture = fixtures::take(&target);
            #[cfg(feature = "filesystem-fixtures")]
            let scratch = fixture.as_ref().map(|fixture| fixture.scratch.clone());
            let mut archive = access(identity, move || {
                #[cfg(feature = "filesystem-fixtures")]
                if let Some(scratch) = scratch {
                    return Ok(tempfile::NamedTempFile::new_in(scratch.path())?.into_file());
                }
                Ok(tempfile::NamedTempFile::new()?.into_file())
            })
            .await?;
            let writer = archive.try_clone()?;
            let token = out.cancel.clone();
            access(identity, move || -> Result<()> {
                let root = open_directory(&target.canonicalize()?)?;
                public_descriptor(&root, &private, false)?;
                archive_tree(&root, writer, Some(&private), &token)
            })
            .await?;
            #[cfg(feature = "filesystem-fixtures")]
            if let Some(fixture) = fixture {
                fixture.checkpoint(&archive).await?;
            }
            // Clones share the offset. Rewind only after the archive writer finishes.
            std::io::Seek::rewind(&mut archive)?;
            let mut file = tokio::fs::File::from_std(archive);
            let mut buf = vec![0; CHUNK];
            loop {
                let n = tokio::select! {
                    _ = out.cancel.cancelled() => bail!("connection closed"),
                    result = file.read(&mut buf) => result?,
                };
                if n == 0 {
                    break;
                }
                out.event(json!({}), buf[..n].to_vec()).await?;
            }
            out.done(json!({})).await
        }
        "fs_watch" => watch(identity, out, &target).await,
        "fs_find" => {
            let needle = p
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned();
            let output = out.clone();
            access(identity, move || {
                let mut count = 0;
                for entry in walkdir::WalkDir::new(&target)
                    .follow_links(false)
                    .into_iter()
                    .filter_entry(|e| public_path(e.path(), &private, false).is_ok())
                {
                    ensure!(!output.cancel.is_cancelled(), "connection closed");
                    let entry = entry?;
                    count += 1;
                    ensure!(count <= 100_000, "find entry limit exceeded");
                    if entry.file_name().to_string_lossy().contains(&needle) {
                        output
                            .tx
                            .blocking_send(output.request.reply(
                                metadata(entry.path())?,
                                vec![],
                                false,
                            ))
                            .map_err(|_| anyhow::anyhow!("connection closed"))?;
                    }
                }
                Ok(())
            })
            .await?;
            out.done(json!({})).await
        }
        _ => {
            let method = out.request.method.clone();
            let result = access(identity, move || -> Result<serde_json::Value> {
                match method.as_str() {
                    "fs_stat" => metadata(&target),
                    "fs_list" => {
                        let mut entries = Vec::new();
                        for entry in std::fs::read_dir(&target)? {
                            let entry = entry?;
                            if public_path(&entry.path(), &private, false).is_err() {
                                continue;
                            }
                            ensure!(
                                entries.len() < 10_000,
                                "directory listing exceeds 10000 entries; use fs_find"
                            );
                            entries.push(metadata(&entry.path())?);
                        }
                        let value = json!({"entries":entries});
                        ensure!(
                            serde_json::to_vec(&value)?.len() < 4 * 1024 * 1024,
                            "directory listing too large; use fs_find"
                        );
                        Ok(value)
                    }
                    "fs_mkdir" => {
                        std::fs::create_dir_all(&target)?;
                        Ok(json!({}))
                    }
                    "fs_rm" => {
                        let m = std::fs::symlink_metadata(&target)?;
                        if m.is_dir() {
                            std::fs::remove_dir_all(&target)?;
                        } else {
                            std::fs::remove_file(&target)?;
                        }
                        Ok(json!({}))
                    }
                    "fs_rename" => {
                        let destination = PathBuf::from(string(&p, "destination")?);
                        ensure!(destination.is_absolute(), "destination must be absolute");
                        std::fs::rename(&target, &destination)?;
                        File::open(parent(&destination)?)?.sync_all()?;
                        File::open(parent(&target)?)?.sync_all()?;
                        Ok(json!({}))
                    }
                    "fs_replace" => {
                        let needle = string(&p, "find")?;
                        ensure!(!needle.is_empty(), "empty find string");
                        let replacement = string(&p, "replace")?;
                        let mut file = OpenOptions::new()
                            .read(true)
                            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                            .open(&target)?;
                        let m = file.metadata()?;
                        ensure!(
                            m.is_file() && m.len() <= 8 * 1024 * 1024,
                            "replace requires a regular UTF-8 file <=8 MiB"
                        );
                        let mut content = String::new();
                        std::io::Read::by_ref(&mut file)
                            .take(8 * 1024 * 1024 + 1)
                            .read_to_string(&mut content)?;
                        ensure!(content.len() <= 8 * 1024 * 1024, "replace input too large");
                        let count = content.matches(needle).count();
                        let estimated = content
                            .len()
                            .checked_add(
                                count
                                    .checked_mul(replacement.len().saturating_sub(needle.len()))
                                    .ok_or_else(|| anyhow::anyhow!("replacement too large"))?,
                            )
                            .ok_or_else(|| anyhow::anyhow!("replacement too large"))?;
                        ensure!(estimated <= 8 * 1024 * 1024, "replacement too large");
                        atomic_write(
                            &target,
                            content.replace(needle, replacement).as_bytes(),
                            m.mode(),
                        )?;
                        Ok(json!({"replacements":count}))
                    }
                    _ => bail!("unknown filesystem method"),
                }
            })
            .await?;
            out.done(result).await
        }
    }
}

async fn receive(out: &Output, input: &mut Input, file: &File) -> Result<u64> {
    let mut writer = tokio::fs::File::from_std(file.try_clone()?);
    let mut total = out.request.data.len() as u64;
    writer.write_all(&out.request.data).await?;
    let mut eof = out.request.eof;
    while !eof {
        let frame = next_input(input, out).await?;
        total += frame.data.len() as u64;
        ensure!(total <= TRANSFER_LIMIT, "transfer exceeds 1 GiB");
        writer.write_all(&frame.data).await?;
        eof = frame.eof;
    }
    writer.flush().await?;
    writer.sync_all().await?;
    ensure!(
        !out.cancel.is_cancelled(),
        "connection closed before commit"
    );
    Ok(total)
}
struct LimitedWriter<W> {
    inner: W,
    bytes: u64,
}
impl<W: Write> Write for LimitedWriter<W> {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if self.bytes + b.len() as u64 > TRANSFER_LIMIT {
            return Err(std::io::Error::other("archive exceeds 1 GiB"));
        }
        let n = self.inner.write(b)?;
        self.bytes += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

const TREE_ENTRIES: usize = 100_000;
const TREE_DEPTH: usize = 64;
const PAX_LIMIT: u64 = 128 * 1024;
const METADATA_LIMIT: usize = 16 * 1024 * 1024;

fn descriptor_path(file: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}
fn public_descriptor(file: &File, private: &Path, destructive: bool) -> Result<()> {
    let path = std::fs::read_link(descriptor_path(file))?;
    ensure!(
        !path.starts_with(private) && !(destructive && private.starts_with(&path)),
        "agent private state is not accessible through the filesystem API"
    );
    Ok(())
}
fn open_directory(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?)
}
fn open_at(directory: &File, name: &OsStr, flags: i32) -> Result<File> {
    let name = CString::new(name.as_bytes())?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    ensure!(
        fd >= 0,
        "open tree entry: {}",
        std::io::Error::last_os_error()
    );
    Ok(unsafe { File::from_raw_fd(fd) })
}
fn tree_path(path: &Path) -> Result<PathBuf> {
    let bytes = path.as_os_str().as_bytes();
    ensure!(
        !bytes.is_empty() && bytes.len() < 4096,
        "invalid tar path length"
    );
    let bytes = bytes.strip_suffix(b"/").unwrap_or(bytes);
    ensure!(
        !bytes
            .split(|b| *b == b'/')
            .any(|p| p.is_empty() || p == b".."),
        "invalid tar path"
    );
    let mut result = PathBuf::new();
    for part in path.components() {
        match part {
            Component::Normal(p) => result.push(p),
            Component::CurDir => (),
            _ => bail!("invalid tar path"),
        }
    }
    ensure!(
        result.components().count() <= TREE_DEPTH,
        "tree depth limit exceeded"
    );
    Ok(result)
}
fn tree_parent(root: &File, path: &Path, create: bool, budget: &mut TreeBudget) -> Result<File> {
    let mut directory = root.try_clone()?;
    for part in path.parent().unwrap_or(Path::new("")).components() {
        let name = part.as_os_str();
        if create {
            let c = CString::new(name.as_bytes())?;
            let rc = unsafe { libc::mkdirat(directory.as_raw_fd(), c.as_ptr(), 0o700) };
            if rc == 0 {
                budget.entry()?;
            } else {
                let error = std::io::Error::last_os_error();
                ensure!(
                    error.raw_os_error() == Some(libc::EEXIST),
                    "create tree directory: {error}"
                );
            }
        }
        directory = open_at(&directory, name, libc::O_RDONLY | libc::O_DIRECTORY)?;
    }
    Ok(directory)
}
fn tree_open(root: &File, path: &Path, flags: i32) -> Result<File> {
    if path.as_os_str().is_empty() {
        return Ok(root.try_clone()?);
    }
    let directory = tree_parent(root, path, false, &mut TreeBudget::default())?;
    open_at(&directory, path.file_name().unwrap(), flags)
}
fn unlink_leaf(directory: &File, name: &OsStr) -> Result<()> {
    let name = CString::new(name.as_bytes())?;
    if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) } < 0 {
        let error = std::io::Error::last_os_error();
        ensure!(
            error.raw_os_error() == Some(libc::ENOENT),
            "replace tree entry: {error}"
        );
    }
    Ok(())
}
fn safe_symlink(path: &Path, link: &Path) -> Result<()> {
    ensure!(
        !link.as_os_str().is_empty() && !link.is_absolute(),
        "unsafe symlink target"
    );
    ensure!(link.as_os_str().len() < 4096, "symlink target too long");
    let mut depth = path.parent().unwrap_or(Path::new("")).components().count();
    for component in link.components() {
        match component {
            Component::Normal(_) => depth += 1,
            Component::CurDir => (),
            Component::ParentDir => {
                ensure!(depth > 0, "symlink target escapes tree");
                depth -= 1;
            }
            _ => bail!("unsafe symlink target"),
        }
    }
    Ok(())
}
fn confined_symlink(root: &File, path: &Path) -> Result<()> {
    // BENEATH also catches escapes caused by chains of otherwise relative links.
    // A dangling relative link is valid; loops and authority failures are explicit errors.
    let name = CString::new(path.as_os_str().as_bytes())?;
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = (libc::O_PATH | libc::O_CLOEXEC) as u64;
    how.resolve = libc::RESOLVE_BENEATH | libc::RESOLVE_NO_MAGICLINKS;
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            root.as_raw_fd(),
            name.as_ptr(),
            &how,
            std::mem::size_of_val(&how),
        )
    };
    if fd >= 0 {
        drop(unsafe { File::from_raw_fd(fd as i32) });
    } else {
        let error = std::io::Error::last_os_error();
        ensure!(
            matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ENOTDIR)),
            "unsafe symlink resolution: {error}"
        );
    }
    Ok(())
}

#[derive(Default)]
struct TreeBudget {
    entries: usize,
    bytes: u64,
    metadata: usize,
}
impl TreeBudget {
    fn entry(&mut self) -> Result<()> {
        self.entries += 1;
        ensure!(self.entries <= TREE_ENTRIES, "tree entry limit exceeded");
        Ok(())
    }
    fn data(&mut self, n: u64) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(n)
            .ok_or_else(|| anyhow::anyhow!("tree size overflow"))?;
        ensure!(self.bytes <= TRANSFER_LIMIT, "expanded tree exceeds 1 GiB");
        Ok(())
    }
    fn metadata(&mut self, n: usize) -> Result<()> {
        self.metadata += n;
        ensure!(
            self.metadata <= METADATA_LIMIT,
            "tree metadata limit exceeded"
        );
        Ok(())
    }
}

#[derive(Clone)]
struct TreeMetadata {
    mode: u32,
    seconds: i64,
    nanos: i64,
    xattrs: BTreeMap<String, Vec<u8>>,
}
impl TreeMetadata {
    fn from_file(file: &File, budget: &mut TreeBudget) -> Result<Self> {
        let m = file.metadata()?;
        let mut result = Self {
            mode: m.mode() & 0o7777,
            seconds: m.mtime(),
            nanos: m.mtime_nsec(),
            xattrs: BTreeMap::new(),
        };
        if !m.file_type().is_symlink() {
            // Linux limits the list and each value to 64 KiB. Use descriptors so
            // neither listing nor reading attributes can follow a replaced path.
            let mut names = vec![0u8; 65536];
            let size = unsafe {
                libc::flistxattr(file.as_raw_fd(), names.as_mut_ptr().cast(), names.len())
            };
            if size < 0 {
                let error = std::io::Error::last_os_error();
                ensure!(
                    error.raw_os_error() == Some(libc::EOPNOTSUPP),
                    "list user xattrs: {error}"
                );
                return Ok(result);
            }
            for name in names[..size as usize]
                .split(|b| *b == 0)
                .filter(|n| n.starts_with(b"user."))
            {
                let key = std::str::from_utf8(name)?.to_owned();
                let c = CString::new(name)?;
                let mut value = vec![0u8; 65536];
                let n = unsafe {
                    libc::fgetxattr(
                        file.as_raw_fd(),
                        c.as_ptr(),
                        value.as_mut_ptr().cast(),
                        value.len(),
                    )
                };
                ensure!(
                    n >= 0,
                    "read user xattr: {}",
                    std::io::Error::last_os_error()
                );
                value.truncate(n as usize);
                budget.metadata(key.len() + value.len())?;
                result.xattrs.insert(key, value);
            }
        }
        Ok(result)
    }
    fn from_entry<R: Read>(entry: &mut tar::Entry<'_, R>, budget: &mut TreeBudget) -> Result<Self> {
        let mode = entry.header().mode()? & 0o7777;
        let mut result = Self {
            mode,
            seconds: entry.header().mtime()?.try_into()?,
            nanos: 0,
            xattrs: BTreeMap::new(),
        };
        let mut keys = HashSet::new();
        if let Some(pax) = entry.pax_extensions()? {
            for extension in pax {
                let extension = extension?;
                let key = extension.key()?;
                ensure!(keys.insert(key.to_owned()), "duplicate PAX metadata key");
                let value = extension.value_bytes();
                budget.metadata(key.len() + value.len())?;
                if key == "mtime" {
                    let text = std::str::from_utf8(value)?;
                    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
                    ensure!(
                        fraction.len() <= 9 && fraction.bytes().all(|b| b.is_ascii_digit()),
                        "invalid PAX mtime"
                    );
                    let whole: i64 = whole.parse()?;
                    let fraction: i128 = if fraction.is_empty() {
                        0
                    } else {
                        fraction.parse()?
                    };
                    let fraction = fraction
                        * 10i128.pow(9 - text.split_once('.').map_or(0, |(_, f)| f.len()) as u32);
                    let total = whole as i128 * 1_000_000_000
                        + if text.starts_with('-') {
                            -fraction
                        } else {
                            fraction
                        };
                    result.seconds = total.div_euclid(1_000_000_000).try_into()?;
                    result.nanos = total.rem_euclid(1_000_000_000) as i64;
                } else if let Some((name, libarchive)) = key
                    .strip_prefix("SCHILY.xattr.")
                    .map(|name| (name, false))
                    .or_else(|| {
                        key.strip_prefix("LIBARCHIVE.xattr.")
                            .map(|name| (name, true))
                    })
                {
                    // The formats encode names differently: a SCHILY suffix is
                    // the literal attribute name, while a LIBARCHIVE suffix is
                    // percent-encoded. Decode only the LIBARCHIVE form.
                    let name = if libarchive {
                        ensure!(name.len() <= 255 * 3, "encoded xattr name too long");
                        // percent_decode deliberately preserves malformed escapes;
                        // validate those before invoking its byte-preserving decoder.
                        for (i, _) in name.bytes().enumerate().filter(|(_, b)| *b == b'%') {
                            ensure!(
                                name.as_bytes()
                                    .get(i + 1)
                                    .is_some_and(u8::is_ascii_hexdigit)
                                    && name
                                        .as_bytes()
                                        .get(i + 2)
                                        .is_some_and(u8::is_ascii_hexdigit),
                                "invalid xattr name encoding"
                            );
                        }
                        percent_decode_str(name).decode_utf8()?.into_owned()
                    } else {
                        name.to_owned()
                    };
                    ensure!(
                        name.starts_with("user.") && name.len() > 5 && name.len() <= 255,
                        "only bounded user xattrs are supported"
                    );
                    CString::new(name.as_bytes())?;
                    let value = if libarchive {
                        ensure!(
                            value.len() <= 4 * 65536usize.div_ceil(3),
                            "encoded xattr value too large"
                        );
                        STANDARD_NO_PAD
                            .decode(value)
                            .or_else(|_| STANDARD.decode(value))?
                    } else {
                        value.to_vec()
                    };
                    ensure!(value.len() <= 65536, "xattr value too large");
                    if let Some(previous) = result.xattrs.get(&name) {
                        ensure!(*previous == value, "conflicting duplicate xattr");
                    } else {
                        result.xattrs.insert(name, value);
                    }
                } else if key.starts_with("GNU.sparse.")
                    || key.starts_with("SCHILY.acl.")
                    || key.starts_with("RHT.security.")
                {
                    bail!("sparse, ACL and privileged metadata are unsupported");
                }
            }
        }
        Ok(result)
    }
    fn apply(&self, file: &File) -> Result<()> {
        let gid = unsafe { libc::syscall(libc::SYS_setfsgid, u32::MAX) } as u32;
        if file.metadata()?.gid() != gid {
            // Changing only to our own workload group is permitted without root.
            ensure!(
                unsafe { libc::fchown(file.as_raw_fd(), u32::MAX, gid) } == 0,
                "set workload group: {}",
                std::io::Error::last_os_error()
            );
        }
        for (name, value) in &self.xattrs {
            let name = CString::new(name.as_bytes())?;
            ensure!(
                unsafe {
                    libc::fsetxattr(
                        file.as_raw_fd(),
                        name.as_ptr(),
                        value.as_ptr().cast(),
                        value.len(),
                        0,
                    )
                } == 0,
                "set user xattr: {}",
                std::io::Error::last_os_error()
            );
        }
        file.set_permissions(std::fs::Permissions::from_mode(self.mode))?;
        ensure!(
            file.metadata()?.mode() & 0o7777 == self.mode,
            "filesystem did not preserve mode bits"
        );
        let times = self.times();
        ensure!(
            unsafe { libc::futimens(file.as_raw_fd(), times.as_ptr()) } == 0,
            "set mtime: {}",
            std::io::Error::last_os_error()
        );
        file.sync_all()?;
        Ok(())
    }
    fn times(&self) -> [libc::timespec; 2] {
        [
            libc::timespec {
                tv_sec: 0,
                tv_nsec: libc::UTIME_OMIT,
            },
            libc::timespec {
                tv_sec: self.seconds as libc::time_t,
                tv_nsec: self.nanos as libc::c_long,
            },
        ]
    }
    fn append_pax<W: Write>(&self, builder: &mut tar::Builder<W>) -> Result<()> {
        let total = self.seconds as i128 * 1_000_000_000 + self.nanos as i128;
        let timestamp = format!(
            "{}{}.{:09}",
            if total < 0 { "-" } else { "" },
            total.abs() / 1_000_000_000,
            total.abs() % 1_000_000_000
        );
        let mut pax = vec![("mtime".to_owned(), timestamp.into_bytes())];
        for (name, value) in &self.xattrs {
            let name = percent_encode(name.as_bytes(), NON_ALPHANUMERIC);
            pax.push((
                format!("LIBARCHIVE.xattr.{name}"),
                STANDARD_NO_PAD.encode(value).into_bytes(),
            ));
        }
        builder.append_pax_extensions(
            pax.iter()
                .map(|(key, value)| (key.as_str(), value.as_slice())),
        )?;
        Ok(())
    }
}

fn archive_tree<W: Write>(
    root: &File,
    writer: W,
    private: Option<&Path>,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    struct ArchiveState<'a, W: Write> {
        builder: tar::Builder<LimitedWriter<W>>,
        links: HashMap<(u64, u64), PathBuf>,
        budget: TreeBudget,
        private: Option<&'a Path>,
        cancel: &'a tokio_util::sync::CancellationToken,
    }
    fn append<W: Write>(
        root: &File,
        file: &File,
        path: &Path,
        state: &mut ArchiveState<'_, W>,
    ) -> Result<()> {
        ensure!(!state.cancel.is_cancelled(), "connection closed");
        ensure!(
            path.components().count() <= TREE_DEPTH,
            "tree depth limit exceeded"
        );
        if let Some(private) = state.private
            && public_descriptor(file, private, false).is_err()
        {
            return Ok(());
        }
        state.budget.entry()?;
        state.budget.metadata(path.as_os_str().len() + 64)?;
        let m = file.metadata()?;
        ensure!(
            m.is_file() || m.is_dir() || m.file_type().is_symlink(),
            "tree contains unsupported special file"
        );
        let readable;
        let file = if m.file_type().is_symlink() {
            file
        } else {
            readable = File::open(descriptor_path(file))?;
            &readable
        };
        let metadata = TreeMetadata::from_file(file, &mut state.budget)?;
        let mut header = tar::Header::new_gnu();
        header.set_metadata(&m);
        header.set_mode(metadata.mode);
        header.set_mtime(m.mtime().max(0) as u64);
        if m.file_type().is_symlink() {
            let link = std::fs::read_link(descriptor_path(file))?;
            // /proc names the link inode; readlinkat with an empty name reads its payload.
            let mut buffer = vec![0u8; 4096];
            let n = unsafe {
                libc::readlinkat(
                    file.as_raw_fd(),
                    c"".as_ptr(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                )
            };
            ensure!(
                n > 0 && (n as usize) < buffer.len(),
                "read tree symlink: {}",
                link.display()
            );
            let target = Path::new(OsStr::from_bytes(&buffer[..n as usize]));
            if let Some(private) = state.private {
                let resolved =
                    resolved_path(&std::fs::read_link(descriptor_path(root))?.join(path))?;
                if resolved.starts_with(private) {
                    return Ok(());
                }
            }
            safe_symlink(path, target)?;
            confined_symlink(root, path)?;
            metadata.append_pax(&mut state.builder)?;
            header.set_size(0);
            state.builder.append_link(&mut header, path, target)?;
        } else if m.is_file() {
            if let Some(target) = state.links.get(&(m.dev(), m.ino())) {
                header.set_entry_type(tar::EntryType::Link);
                header.set_size(0);
                state.builder.append_link(&mut header, path, target)?;
            } else {
                state.budget.data(m.len())?;
                metadata.append_pax(&mut state.builder)?;
                let mut reader = CancellableReader {
                    inner: file.take(m.len()),
                    cancel: state.cancel,
                };
                state.builder.append_data(&mut header, path, &mut reader)?;
                ensure!(
                    reader.inner.limit() == 0,
                    "source file changed during transfer"
                );
                state.links.insert((m.dev(), m.ino()), path.to_owned());
            }
        } else {
            metadata.append_pax(&mut state.builder)?;
            header.set_size(0);
            state.builder.append_data(
                &mut header,
                if path.as_os_str().is_empty() {
                    Path::new(".")
                } else {
                    path
                },
                std::io::empty(),
            )?;
            for entry in std::fs::read_dir(descriptor_path(file))? {
                let entry = entry?;
                let child = open_at(file, &entry.file_name(), libc::O_PATH)?;
                append(root, &child, &path.join(entry.file_name()), state)?;
            }
        }
        Ok(())
    }
    let mut state = ArchiveState {
        builder: tar::Builder::new(LimitedWriter {
            inner: writer,
            bytes: 0,
        }),
        links: HashMap::new(),
        budget: TreeBudget::default(),
        private,
        cancel,
    };
    append(root, root, Path::new(""), &mut state)?;
    state.builder.finish()?;
    Ok(())
}
struct CancellableReader<'a, R> {
    inner: R,
    cancel: &'a tokio_util::sync::CancellationToken,
}
impl<R: Read> Read for CancellableReader<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.cancel.is_cancelled() {
            return Err(std::io::Error::other("connection closed"));
        }
        self.inner.read(buffer)
    }
}

// The crate's normal iterator buffers PAX/GNU extension entries. Inspect raw
// headers first to bound each such allocation, including extensions hidden from
// the normal iterator. Tar framing and all extension parsing remain crate-owned.
fn validate_archive(path: &Path, cancel: &tokio_util::sync::CancellationToken) -> Result<()> {
    let file = File::open(path)?;
    ensure!(
        file.metadata()?.len() <= TRANSFER_LIMIT,
        "archive exceeds 1 GiB"
    );
    let mut tar = tar::Archive::new(file);
    let mut count = 0;
    for entry in tar.entries()?.raw(true) {
        ensure!(!cancel.is_cancelled(), "connection closed");
        let entry = entry?;
        count += 1;
        ensure!(count <= TREE_ENTRIES * 4, "tar header limit exceeded");
        let kind = entry.header().entry_type();
        if kind.is_pax_local_extensions() || kind.is_gnu_longname() || kind.is_gnu_longlink() {
            ensure!(entry.size() <= PAX_LIMIT, "tar extension too large");
        } else {
            ensure!(
                kind.is_file() || kind.is_dir() || kind.is_symlink() || kind.is_hard_link(),
                "unsupported tar entry type"
            );
            ensure!(
                entry.size() <= TRANSFER_LIMIT,
                "expanded tree exceeds 1 GiB"
            );
        }
    }
    Ok(())
}

struct StagedTree {
    root: File,
    metadata: BTreeMap<PathBuf, std::sync::Arc<TreeMetadata>>,
    symlinks: HashSet<PathBuf>,
    budget: TreeBudget,
}
impl StagedTree {
    fn extract(
        &mut self,
        archive: &Path,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        validate_archive(archive, cancel)?;
        let mut tar = tar::Archive::new(File::open(archive)?);
        let mut seen = HashSet::new();
        let mut links = BTreeMap::new();
        for entry in tar.entries()? {
            ensure!(!cancel.is_cancelled(), "connection closed");
            let mut entry = entry?;
            self.budget.entry()?;
            let kind = entry.header().entry_type();
            let path = tree_path(&entry.path()?)?;
            self.budget.metadata(path.as_os_str().len() + 64)?;
            ensure!(
                !path.as_os_str().is_empty() || kind.is_dir(),
                "only directories may name the tree root"
            );
            ensure!(
                seen.insert(path.clone()) || kind.is_dir(),
                "duplicate tar entry"
            );
            let metadata =
                std::sync::Arc::new(TreeMetadata::from_entry(&mut entry, &mut self.budget)?);
            self.budget.data(entry.size())?;
            if kind.is_dir() {
                if !path.as_os_str().is_empty() {
                    let directory = tree_parent(&self.root, &path, true, &mut self.budget)?;
                    let name = CString::new(path.file_name().unwrap().as_bytes())?;
                    if unsafe { libc::mkdirat(directory.as_raw_fd(), name.as_ptr(), 0o700) } < 0 {
                        let error = std::io::Error::last_os_error();
                        ensure!(
                            error.raw_os_error() == Some(libc::EEXIST),
                            "create directory: {error}"
                        );
                    }
                    tree_open(&self.root, &path, libc::O_RDONLY | libc::O_DIRECTORY)?;
                }
                let metadata = if let Some(previous) = self.metadata.get(&path) {
                    let mut merged = (*metadata).clone();
                    let mut xattrs = previous.xattrs.clone();
                    xattrs.extend(merged.xattrs);
                    merged.xattrs = xattrs;
                    std::sync::Arc::new(merged)
                } else {
                    metadata
                };
                self.metadata.insert(path, metadata);
                continue;
            }
            let directory = tree_parent(&self.root, &path, true, &mut self.budget)?;
            let name = path.file_name().unwrap();
            // Never truncate a copied hardlink or follow an existing symlink.
            unlink_leaf(&directory, name)?;
            self.metadata.remove(&path);
            self.symlinks.remove(&path);
            if kind.is_file() {
                let mut file = open_at(
                    &directory,
                    name,
                    libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                )?;
                let n = std::io::copy(
                    &mut CancellableReader {
                        inner: &mut entry,
                        cancel,
                    },
                    &mut file,
                )?;
                ensure!(n == entry.size(), "truncated tar entry");
                self.metadata.insert(path, metadata);
            } else if kind.is_symlink() {
                ensure!(
                    metadata.xattrs.is_empty(),
                    "symlink user xattrs are unsupported by Linux"
                );
                let link = entry
                    .link_name()?
                    .ok_or_else(|| anyhow::anyhow!("missing symlink target"))?;
                safe_symlink(&path, &link)?;
                self.budget.metadata(link.as_os_str().len())?;
                let link = CString::new(link.as_os_str().as_bytes())?;
                let name = CString::new(name.as_bytes())?;
                ensure!(
                    unsafe { libc::symlinkat(link.as_ptr(), directory.as_raw_fd(), name.as_ptr()) }
                        == 0,
                    "create symlink: {}",
                    std::io::Error::last_os_error()
                );
                let times = metadata.times();
                ensure!(
                    unsafe {
                        libc::utimensat(
                            directory.as_raw_fd(),
                            name.as_ptr(),
                            times.as_ptr(),
                            libc::AT_SYMLINK_NOFOLLOW,
                        )
                    } == 0,
                    "set symlink mtime: {}",
                    std::io::Error::last_os_error()
                );
                let gid = unsafe { libc::syscall(libc::SYS_setfsgid, u32::MAX) } as u32;
                let link_file = open_at(&directory, path.file_name().unwrap(), libc::O_PATH)?;
                if link_file.metadata()?.gid() != gid {
                    ensure!(
                        unsafe {
                            libc::fchownat(
                                directory.as_raw_fd(),
                                name.as_ptr(),
                                u32::MAX,
                                gid,
                                libc::AT_SYMLINK_NOFOLLOW,
                            )
                        } == 0,
                        "set symlink workload group: {}",
                        std::io::Error::last_os_error()
                    );
                }
                self.symlinks.insert(path);
            } else if kind.is_hard_link() {
                let link = tree_path(
                    &entry
                        .link_name()?
                        .ok_or_else(|| anyhow::anyhow!("missing hardlink target"))?,
                )?;
                ensure!(!link.as_os_str().is_empty(), "invalid hardlink target");
                self.budget.metadata(link.as_os_str().len())?;
                links.insert(path, link);
            } else {
                bail!("unsupported tar entry type");
            }
        }
        fn link(
            tree: &mut StagedTree,
            path: &Path,
            links: &mut BTreeMap<PathBuf, PathBuf>,
            depth: usize,
        ) -> Result<()> {
            ensure!(
                depth <= TREE_DEPTH,
                "hardlink cycle or depth limit exceeded"
            );
            let Some(target) = links.remove(path) else {
                return Ok(());
            };
            if links.contains_key(&target) {
                link(tree, &target, links, depth + 1)?;
            }
            let source = tree_open(&tree.root, &target, libc::O_PATH)?;
            ensure!(
                source.metadata()?.is_file(),
                "hardlink target must be a staged regular file"
            );
            let from = tree_parent(&tree.root, &target, false, &mut tree.budget)?;
            let to = tree_parent(&tree.root, path, true, &mut tree.budget)?;
            let a = CString::new(target.file_name().unwrap().as_bytes())?;
            let b = CString::new(path.file_name().unwrap().as_bytes())?;
            ensure!(
                unsafe {
                    libc::linkat(from.as_raw_fd(), a.as_ptr(), to.as_raw_fd(), b.as_ptr(), 0)
                } == 0,
                "create hardlink: {}",
                std::io::Error::last_os_error()
            );
            let metadata = tree
                .metadata
                .get(&target)
                .ok_or_else(|| anyhow::anyhow!("hardlink target lacks metadata"))?
                .clone();
            tree.metadata.insert(path.to_owned(), metadata);
            Ok(())
        }
        while let Some(path) = links.keys().next().cloned() {
            link(self, &path, &mut links, 0)?;
        }
        Ok(())
    }
    fn finish(&self, cancel: &tokio_util::sync::CancellationToken) -> Result<()> {
        fn sync_directories(
            root: &File,
            cancel: &tokio_util::sync::CancellationToken,
        ) -> Result<()> {
            for entry in std::fs::read_dir(descriptor_path(root))? {
                ensure!(!cancel.is_cancelled(), "connection closed");
                let entry = entry?;
                let child = open_at(root, &entry.file_name(), libc::O_PATH)?;
                if child.metadata()?.is_dir() {
                    let child = File::open(descriptor_path(&child))?;
                    sync_directories(&child, cancel)?;
                }
            }
            root.sync_all()?;
            Ok(())
        }
        // Include implicit parent directories, which have no archive metadata.
        sync_directories(&self.root, cancel)?;
        for path in &self.symlinks {
            ensure!(!cancel.is_cancelled(), "connection closed");
            confined_symlink(&self.root, path)?;
        }
        // Children precede parents so restrictive directory modes and directory
        // mtimes are applied only after all directory-entry changes are complete.
        let mut applied = HashSet::new();
        for (path, metadata) in self.metadata.iter().rev() {
            ensure!(!cancel.is_cancelled(), "connection closed");
            let file = tree_open(&self.root, path, libc::O_PATH)?;
            let m = file.metadata()?;
            ensure!(m.is_file() || m.is_dir(), "metadata target changed");
            if m.is_file() && !applied.insert((m.dev(), m.ino())) {
                continue;
            }
            let file = File::open(descriptor_path(&file))?;
            metadata.apply(&file)?;
        }
        self.root.sync_all()?;
        Ok(())
    }
}

fn cleanup_tree(root: &File) -> Result<()> {
    // Failed transfers may already have applied mode 000 to some directories.
    // Restore owner access through pinned descriptors before TempDir cleanup.
    std::fs::set_permissions(
        descriptor_path(root),
        std::fs::Permissions::from_mode(0o700),
    )?;
    for entry in std::fs::read_dir(descriptor_path(root))? {
        let entry = entry?;
        let file = open_at(root, &entry.file_name(), libc::O_PATH)?;
        if file.metadata()?.is_dir() {
            cleanup_tree(&file)?;
        }
    }
    Ok(())
}
fn push_tree(
    target: &Path,
    archive: &Path,
    cancel: &tokio_util::sync::CancellationToken,
    private: Option<&Path>,
) -> Result<()> {
    let name = target
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("cannot replace filesystem root"))?;
    let directory = open_directory(&parent(target)?.canonicalize()?)?;
    if let Some(private) = private {
        // Pin the actual parent, then repeat the private-state check after any
        // symlink resolution. All staging and commit paths use this descriptor.
        public_descriptor(&directory, private, false)?;
        public_path(
            &std::fs::read_link(descriptor_path(&directory))?.join(name),
            private,
            true,
        )?;
    }
    let stage = tempfile::tempdir_in(descriptor_path(&directory))?;
    let result = (|| {
        let root = open_directory(stage.path())?;
        let gid = unsafe { libc::syscall(libc::SYS_setfsgid, u32::MAX) } as u32;
        if root.metadata()?.gid() != gid {
            ensure!(
                unsafe { libc::fchown(root.as_raw_fd(), u32::MAX, gid) } == 0,
                "set staging workload group: {}",
                std::io::Error::last_os_error()
            );
        }
        root.set_permissions(std::fs::Permissions::from_mode(0o700))?;
        let mut tree = StagedTree {
            root,
            metadata: BTreeMap::new(),
            symlinks: HashSet::new(),
            budget: TreeBudget::default(),
        };
        let existing = open_at(&directory, name, libc::O_PATH);
        let exists = match existing {
            Ok(existing) => {
                ensure!(
                    existing.metadata()?.is_dir(),
                    "tree destination must be a directory"
                );
                if let Some(private) = private {
                    public_descriptor(&existing, private, true)?;
                }
                let snapshot = tempfile::NamedTempFile::new_in(descriptor_path(&directory))?;
                archive_tree(&existing, snapshot.as_file(), private, cancel)?;
                tree.extract(snapshot.path(), cancel)?;
                true
            }
            Err(error) => {
                // open_at adds context; inspect the entry directly to distinguish
                // a missing destination from an authority or type failure.
                if std::fs::symlink_metadata(descriptor_path(&directory).join(name))
                    .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound)
                {
                    false
                } else {
                    return Err(error);
                }
            }
        };
        tree.extract(archive, cancel)?;
        tree.finish(cancel)?;
        ensure!(!cancel.is_cancelled(), "connection closed before commit");
        let a = CString::new(stage.path().file_name().unwrap().as_bytes())?;
        let b = CString::new(name.as_bytes())?;
        let flags = if exists {
            libc::RENAME_EXCHANGE
        } else {
            libc::RENAME_NOREPLACE
        };
        ensure!(
            unsafe {
                libc::syscall(
                    libc::SYS_renameat2,
                    directory.as_raw_fd(),
                    a.as_ptr(),
                    directory.as_raw_fd(),
                    b.as_ptr(),
                    flags,
                )
            } == 0,
            "commit tree: {}",
            std::io::Error::last_os_error()
        );
        directory.sync_all()?;
        Ok(())
    })();
    if let Ok(root) = open_at(
        &directory,
        stage.path().file_name().unwrap(),
        libc::O_PATH | libc::O_DIRECTORY,
    ) {
        let _ = cleanup_tree(&root);
    }
    // After EXCHANGE the staging name contains the old destination tree.
    drop(stage);
    result
}

async fn watch(identity: (u32, u32), out: &Output, target: &Path) -> Result<()> {
    let target = target.to_owned();
    let fd = access(identity, move || {
        let raw = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if raw < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        let name = CString::new(target.as_os_str().as_bytes())?;
        let mask = libc::IN_MODIFY
            | libc::IN_ATTRIB
            | libc::IN_CREATE
            | libc::IN_DELETE
            | libc::IN_MOVED_FROM
            | libc::IN_MOVED_TO
            | libc::IN_DELETE_SELF
            | libc::IN_MOVE_SELF
            | libc::IN_CLOSE_WRITE
            | libc::IN_DONT_FOLLOW;
        if unsafe { libc::inotify_add_watch(raw, name.as_ptr(), mask) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(fd)
    })
    .await?;
    // OwnedFd keeps the descriptor stable and open for the registration lifetime.
    let fd = unsafe { tokio::io::unix::AsyncFd::register(fd) }?;
    out.event(json!({"ready":true}), vec![]).await?;
    let mut buffer = vec![0u8; CHUNK];
    loop {
        let mut ready = tokio::select! {_ = out.cancel.cancelled()=>bail!("connection closed"),r=fd.readable()=>r?};
        let n = match ready.try_io(|f| {
            let n = unsafe { libc::read(f.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
            if n < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(n as usize)
            }
        }) {
            Ok(r) => r?,
            Err(_) => continue,
        };
        let mut offset = 0;
        while offset + std::mem::size_of::<libc::inotify_event>() <= n {
            let event = unsafe {
                std::ptr::read_unaligned(buffer[offset..].as_ptr().cast::<libc::inotify_event>())
            };
            ensure!(
                event.mask & libc::IN_Q_OVERFLOW == 0,
                "watch queue overflow; rescan required"
            );
            let start = offset + std::mem::size_of::<libc::inotify_event>();
            let end = start + event.len as usize;
            ensure!(end <= n, "invalid inotify event");
            let name = &buffer[start..end];
            let name = &name[..name.iter().position(|b| *b == 0).unwrap_or(name.len())];
            out.event(json!({"mask":event.mask,"cookie":event.cookie,"name":String::from_utf8_lossy(name)}),vec![]).await?;
            if event.mask & libc::IN_IGNORED != 0 {
                return out.done(json!({"removed":true})).await;
            }
            offset = end;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn atomic_permissions_and_tree_rollback() {
        let tmp = tempfile::tempdir().unwrap();
        let f = tmp.path().join("env");
        atomic_write(&f, b"old", 0o600).unwrap();
        assert_eq!(std::fs::metadata(&f).unwrap().mode() & 0o777, 0o600);
        atomic_write(&f, b"new", 0o600).unwrap();
        assert_eq!(std::fs::read(&f).unwrap(), b"new");
        let target = tmp.path().join("tree");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("old"), b"kept").unwrap();
        let archive = tmp.path().join("broken.tar");
        std::fs::write(&archive, b"bad tar").unwrap();
        assert!(push_tree(&target, &archive, &Default::default(), None).is_err());
        assert_eq!(std::fs::read(target.join("old")).unwrap(), b"kept");
        let file = File::create(&archive).unwrap();
        let mut builder = tar::Builder::new(file);
        let mut header = tar::Header::new_gnu();
        header.set_size(3);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, "added", &b"new"[..])
            .unwrap();
        builder.finish().unwrap();
        drop(builder);
        push_tree(&target, &archive, &Default::default(), None).unwrap();
        assert_eq!(std::fs::read(target.join("added")).unwrap(), b"new");
        assert!(target.join("old").exists());
    }
}
