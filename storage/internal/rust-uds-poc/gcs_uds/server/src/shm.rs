//! Shared-memory data plane (Linux): a memfd per client connection, handed
//! over a side-channel Unix socket with SCM_RIGHTS, mapped by both processes.
//! Object bytes are written directly into the mapping; only (offset,len)
//! descriptors travel over gRPC. This is the only way to get a zero-copy
//! boundary between two processes; it is deliberately minimal (no ring
//! buffer / backpressure: the client reserves `capacity` >= object size).

use std::collections::HashMap;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::UnixListener;
use tokio::sync::Mutex;

pub struct Region {
    pub id: u64,
    pub size: usize,
    base: *mut u8,
    _fd: OwnedFd,
}
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl Region {
    /// Copies `data` to `offset`. The *only* user-space copy on the server
    /// side for the shm path (crate chunk -> shared page).
    pub fn write_at(&self, offset: usize, data: &[u8]) -> Result<(), String> {
        if offset + data.len() > self.size {
            return Err(format!("shm region overflow: {}+{} > {}", offset, data.len(), self.size));
        }
        unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), self.base.add(offset), data.len()) };
        Ok(())
    }
}
impl Drop for Region {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.base as *mut libc::c_void, self.size) };
    }
}

pub struct Registry {
    regions: Mutex<HashMap<u64, Arc<Region>>>,
    next: AtomicU64,
}

impl Registry {
    pub fn new() -> Self {
        Self { regions: Mutex::new(HashMap::new()), next: AtomicU64::new(1) }
    }
    pub async fn get(&self, id: u64) -> Option<Arc<Region>> {
        self.regions.lock().await.get(&id).cloned()
    }

    fn create(&self, size: usize) -> std::io::Result<Arc<Region>> {
        let name = std::ffi::CString::new("gcs-core-shm").unwrap();
        let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        if unsafe { libc::ftruncate(fd.as_raw_fd(), size as libc::off_t) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let base = unsafe {
            libc::mmap(std::ptr::null_mut(), size, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_SHARED, fd.as_raw_fd(), 0)
        };
        if base == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        // Prevent the client from growing/shrinking it under us.
        unsafe {
            libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL);
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(Region { id, size, base: base as *mut u8, _fd: fd }))
    }

    /// Side channel: for every connection, create a region and send
    /// (fd via SCM_RIGHTS, payload = id:u64 size:u64 little-endian).
    pub async fn serve(self: Arc<Self>, path: String, size: usize) -> std::io::Result<()> {
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        loop {
            let (stream, _) = listener.accept().await?;
            let reg = match self.create(size) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("memfd: {e}");
                    continue;
                }
            };
            let std_stream = stream.into_std()?;
            std_stream.set_nonblocking(false)?;
            let mut payload = [0u8; 16];
            payload[..8].copy_from_slice(&reg.id.to_le_bytes());
            payload[8..].copy_from_slice(&(reg.size as u64).to_le_bytes());
            if let Err(e) = send_fd(std_stream.as_raw_fd(), reg._fd.as_raw_fd(), &payload) {
                tracing::error!("sendmsg SCM_RIGHTS: {e}");
                continue;
            }
            self.regions.lock().await.insert(reg.id, reg.clone());
            // Region lives until the side-channel connection closes.
            let me = self.clone();
            tokio::task::spawn_blocking(move || {
                use std::io::Read;
                let mut s = std_stream;
                let mut b = [0u8; 1];
                let _ = s.read(&mut b); // blocks until client closes
                let id = reg.id;
                drop(reg);
                tokio::runtime::Handle::current().block_on(async move {
                    me.regions.lock().await.remove(&id);
                });
            });
        }
    }
}

fn send_fd(sock: i32, fd: i32, payload: &[u8]) -> std::io::Result<()> {
    unsafe {
        let mut iov = libc::iovec { iov_base: payload.as_ptr() as *mut libc::c_void, iov_len: payload.len() };
        let space = libc::CMSG_SPACE(std::mem::size_of::<i32>() as u32) as usize;
        let mut buf = vec![0u8; space];
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = buf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = space as _;
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<i32>() as u32) as _;
        std::ptr::copy_nonoverlapping(&fd as *const i32 as *const u8, libc::CMSG_DATA(cmsg), std::mem::size_of::<i32>());
        if libc::sendmsg(sock, &msg, 0) < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}
