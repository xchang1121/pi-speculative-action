//! Directory images and COW listings share the kernel open-file-description cursor.
//! Path operations are mediated by the common chroot dispatcher, including COW
//! without an explicit root (its notification policy uses the identity root).

use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::cow::seccomp::SeccompCowBranch;
use crate::seccomp::notif::{read_child_mem, write_child_mem, NotifAction};
use crate::seccomp::state::CowState;
use crate::sys::structs::SeccompNotif;

pub(crate) async fn read_directory(
    notif: &SeccompNotif,
    cow_state: &Arc<Mutex<CowState>>,
    notif_fd: RawFd,
    directory_images: &[(PathBuf, Vec<u8>)],
    inode_projection: Option<&(dyn Fn(u64, u64) -> u64 + Sync)>,
) -> NotifAction {
    use std::os::unix::fs::MetadataExt;
    // getdents and seek share the OFD cursor, including dup and fork aliases.
    let cs = cow_state.lock().await;
    if directory_images.is_empty() && inode_projection.is_none() && cs.branch.is_none() { return NotifAction::Continue; }
    let pin = match crate::seccomp::notif::dup_fd_from_pid(notif.pid, notif.data.args[0] as i32) {
        Ok(pin) => pin, Err(_) => return NotifAction::Errno(libc::EBADF),
    };
    let mut state: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(pin.as_raw_fd(), &mut state) } != 0 { return NotifAction::Errno(libc::EBADF); }
    let image = directory_images.iter().find_map(|(directory, bytes)| {
        let named = std::fs::metadata(directory).ok()?;
        (named.dev() == state.st_dev && named.ino() == state.st_ino).then_some(bytes)
    });
    let merged = if image.is_some() { None } else { merged_listing(cs.branch.as_ref(), pin.as_raw_fd(), &state, inode_projection) };
    let image = image.map(Vec::as_slice).or(merged.as_deref());
    let native = image.is_none();
    if native && (inode_projection.is_none() || notif.data.nr as i64 == libc::SYS_lseek) { return NotifAction::Continue; }
    if unsafe { libc::fcntl(pin.as_raw_fd(), libc::F_GETFL) } & libc::O_PATH != 0 { return NotifAction::Errno(libc::EBADF); }
    if notif.data.nr as i64 == libc::SYS_lseek {
        let position = unsafe { libc::lseek(pin.as_raw_fd(), notif.data.args[1] as i64, notif.data.args[2] as i32) };
        return if position < 0 { NotifAction::Errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::EINVAL)) } else { NotifAction::ReturnValue(position) };
    }
    let capacity = notif.data.args[2] as u32 as usize;
    if capacity > i32::MAX as usize { return NotifAction::Errno(libc::EINVAL); }
    let cursor = unsafe { libc::lseek(pin.as_raw_fd(), 0, libc::SEEK_CUR) };
    let restore = || { if native && cursor >= 0 { unsafe { libc::lseek(pin.as_raw_fd(), cursor, libc::SEEK_SET); } } };
    // All sources use getdents64 records. Only a native read advances the OFD now;
    // an image advances it after delivery, while a failed native delivery restores it.
    let mut observed = Vec::new();
    let image = if let Some(image) = image {
        if cursor < 0 { return NotifAction::Errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO)); }
        image
    } else {
        observed.resize(capacity.min(2 * 1024 * 1024), 0);
        let used = unsafe { libc::syscall(libc::SYS_getdents64, pin.as_raw_fd(), observed.as_mut_ptr(), observed.len()) };
        if used < 0 { return NotifAction::Errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO)); }
        observed.truncate(used as usize);
        &observed
    };
    if image.len() > 2 * 1024 * 1024 { return NotifAction::Errno(libc::EINVAL); }
    let mut entries = Vec::new();
    let (mut offset, mut previous, mut used, mut next) = (0usize, 0i64, 0usize, cursor);
    let mut reached = native || cursor == 0;
    while offset < image.len() {
        if image.len() - offset < 20 { return NotifAction::Errno(libc::EIO); }
        let size = u16::from_le_bytes(image[offset+16..offset+18].try_into().unwrap()) as usize;
        let cookie = i64::from_le_bytes(image[offset+8..offset+16].try_into().unwrap());
        if size < 24 || size % 8 != 0 || size > image.len() - offset || (!native && cookie <= previous) { return NotifAction::Errno(libc::EIO); }
        let Some(name) = image[offset+19..offset+size].iter().position(|&byte| byte == 0) else { return NotifAction::Errno(libc::EIO); };
        reached |= cursor == previous;
        if reached {
            if size > capacity - used { break; }
            entries.push((offset, size, name)); used += size; next = cookie;
        }
        previous = cookie; offset += size;
    }
    if !reached && cursor != previous { return NotifAction::Errno(libc::EINVAL); }
    if used == 0 { return if offset == image.len() { NotifAction::ReturnValue(0) } else { NotifAction::Errno(libc::EINVAL) }; }
    // Preserve padding and touch only returned records, including a buffer that
    // ends at a protected page before the caller's requested capacity.
    let mut output = match read_child_mem(notif_fd, notif.id, notif.pid, notif.data.args[1], used) {
        Ok(bytes) => bytes, Err(_) => { restore(); return NotifAction::Errno(libc::EFAULT); }
    };
    let legacy = Some(notif.data.nr as i64) == crate::arch::sys_getdents();
    let mut destination = 0;
    for (source, size, name) in entries {
        output[destination..destination+18].copy_from_slice(&image[source..source+18]);
        if let Some(project) = inode_projection.filter(|_| native) {
            let inode = u64::from_le_bytes(image[source..source+8].try_into().unwrap());
            output[destination..destination+8].copy_from_slice(&project(state.st_dev, inode).to_ne_bytes());
        }
        let name_start = if legacy { 18 } else { 19 };
        output[destination+name_start..destination+name_start+name+1].copy_from_slice(&image[source+19..source+20+name]);
        output[destination+if legacy { size-1 } else { 18 }] = image[source+18];
        destination += size;
    }
    if write_child_mem(notif_fd, notif.id, notif.pid, notif.data.args[1], &output).is_err() { restore(); return NotifAction::Errno(libc::EFAULT); }
    if !native && unsafe { libc::lseek(pin.as_raw_fd(), next, libc::SEEK_SET) } != next { return NotifAction::Errno(libc::EIO); }
    NotifAction::ReturnValue(used as i64)
}

/// The listing of the directory `fd` names when the COW branch holds entries in it: its own entry, its parent's,
/// then each merged name, as getdents64 records keyed by their position.
fn merged_listing(cow: Option<&SeccompCowBranch>, fd: RawFd, state: &libc::stat,
    inode_projection: Option<&(dyn Fn(u64, u64) -> u64 + Sync)>) -> Option<Vec<u8>> {
    use std::os::unix::fs::MetadataExt;
    let cow = cow?;
    let target = std::fs::read_link(format!("/proc/self/fd/{}", fd)).ok()?;
    let rel = match target.strip_prefix(cow.upper_dir()) {
        Ok(rest) => rest.to_str()?.to_string(),
        Err(_) if cow.matches(target.to_str()?) => cow.safe_rel(target.to_str()?)?,
        Err(_) => return None,
    };
    let entries = cow.merged_entries(&rel)?;
    let parent = Path::new(&rel).parent().map(|parent| cow.resolve_read(parent.to_str().unwrap_or("")))
        .unwrap_or_else(|| cow.workdir().parent().unwrap_or(Path::new("/")).to_path_buf());
    let parent = std::fs::symlink_metadata(parent).map_or((state.st_dev, state.st_ino), |m| (m.dev(), m.ino()));
    let mut image = Vec::new();
    let own = [(".".to_string(), state.st_dev, state.st_ino, libc::DT_DIR), ("..".to_string(), parent.0, parent.1, libc::DT_DIR)];
    for (position, (name, device, ino, kind)) in own.into_iter().chain(entries).enumerate() {
        let ino = inode_projection.map_or(ino, |project| project(device, ino));
        image.extend(crate::procfs::build_dirent64(ino, position as i64 + 1, kind, &name)?);
    }
    Some(image)
}
