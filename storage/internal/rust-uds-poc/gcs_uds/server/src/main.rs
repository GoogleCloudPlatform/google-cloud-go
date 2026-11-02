//! gcs-core-server: the Rust "thick client" as a local gRPC server over a Unix
//! Domain Socket. A thin, pure-Go (or any language) client talks to it with a
//! normal gRPC stub; no cgo, no FFI.
//!
//! Lifecycle features a real sidecar needs, all implemented here so they can be
//! measured: peer-UID check on every connection, idle exit, parent-death
//! signal, stale-socket cleanup, graceful SIGTERM, and a Stats RPC so the
//! caller can account for this process's CPU and memory.

mod service;
mod session;
mod shm;

use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::UnixListener;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::transport::server::UdsConnectInfo;

pub mod pb {
    tonic::include_proto!("gcscore.v1");
}

#[derive(Parser, Debug, Clone)]
#[command(version, about)]
pub struct Args {
    /// Socket path. Linux abstract namespace is used when it starts with '@'.
    #[arg(long, default_value = "/tmp/gcs-core.sock")]
    pub socket: String,
    /// Exit after this many seconds without any RPC (0 = never).
    #[arg(long, default_value_t = 60)]
    pub idle_secs: u64,
    /// Default transport when a request leaves it unspecified: json | grpc.
    #[arg(long, default_value = "json")]
    pub default_transport: String,
    /// Linux: receive SIGTERM when the parent process dies.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub pdeathsig: bool,
    /// Reject connections from other UIDs (SO_PEERCRED).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub peer_uid_check: bool,
    /// Default bytes per ReadChunk message.
    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    pub chunk_size: usize,
    /// Optional explicit endpoint (emulator/testbench).
    #[arg(long)]
    pub endpoint: Option<String>,
    /// Also listen on this TCP address (for the UDS-vs-TCP microbenchmark).
    #[arg(long)]
    pub tcp: Option<String>,
    /// Refuse data RPCs that do not carry a per-caller session (x-session metadata).
    #[arg(long, default_value_t = false)]
    pub require_session: bool,
    /// Shared-memory region size handed to each client on <socket>.shm (0 = disabled).
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    pub shm_size: usize,
}

pub struct Shared {
    pub last_rpc: AtomicI64, // unix millis
    pub rpcs: AtomicU64,
    pub started: Instant,
    pub shutdown: tokio::sync::Notify,
}

impl Shared {
    pub fn touch(&self) {
        self.rpcs.fetch_add(1, Ordering::Relaxed);
        self.last_rpc.store(now_ms(), Ordering::Relaxed);
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    #[cfg(target_os = "linux")]
    if args.pdeathsig {
        // If the parent (the thin client's process) dies, the kernel sends us
        // SIGTERM. Avoids orphaned sidecars when the app is SIGKILLed.
        unsafe {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
        }
        // Race: parent may already be gone.
        if unsafe { libc::getppid() } == 1 {
            eprintln!("parent already exited; not starting");
            std::process::exit(0);
        }
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("gcs-core")
        .build()?;
    rt.block_on(run(args))
}

pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let shared = Arc::new(Shared {
        last_rpc: AtomicI64::new(now_ms()),
        rpcs: AtomicU64::new(0),
        started: Instant::now(),
        shutdown: tokio::sync::Notify::new(),
    });
    let shm = Arc::new(shm::Registry::new());
    if args.shm_size > 0 && !args.socket.starts_with('@') {
        let r = shm.clone();
        let p = format!("{}.shm", args.socket);
        let shm_size = args.shm_size;
        if let Some(dir) = PathBuf::from(&p).parent() {
            std::fs::create_dir_all(dir)?;
        }
        tokio::spawn(async move {
            if let Err(e) = r.serve(p, shm_size).await {
                tracing::error!("shm side channel: {e}");
            }
        });
    }
    let svc = service::GcsCoreService::new(&args, shared.clone(), shm).await?;
    let my_uid = unsafe { libc::getuid() };
    let peer_check = args.peer_uid_check;

    // Interceptor: allow only same-UID peers (SO_PEERCRED via tonic's UdsConnectInfo).
    let auth = move |req: tonic::Request<()>| -> Result<tonic::Request<()>, tonic::Status> {
        if !peer_check {
            return Ok(req);
        }
        match req.extensions().get::<UdsConnectInfo>() {
            Some(info) => match &info.peer_cred {
                Some(c) if c.uid() == my_uid => Ok(req),
                Some(c) => Err(tonic::Status::permission_denied(format!(
                    "peer uid {} != server uid {}",
                    c.uid(),
                    my_uid
                ))),
                None => Err(tonic::Status::permission_denied("no peer credentials")),
            },
            // TCP listener (microbenchmark only): no UDS info.
            None => Ok(req),
        }
    };
    let inner = pb::gcs_core_server::GcsCoreServer::new(svc)
        .max_decoding_message_size(64 * 1024 * 1024)
        .max_encoding_message_size(64 * 1024 * 1024);
    let grpc = tonic::service::interceptor::InterceptedService::new(inner, auth);

    // Idle watchdog.
    if args.idle_secs > 0 {
        let s = shared.clone();
        let idle = args.idle_secs as i64 * 1000;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if now_ms() - s.last_rpc.load(Ordering::Relaxed) > idle {
                    tracing::info!("idle for {}s, exiting", idle / 1000);
                    s.shutdown.notify_waiters();
                    return;
                }
            }
        });
    }
    // Signals.
    {
        let s = shared.clone();
        tokio::spawn(async move {
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
            tokio::select! {
                _ = term.recv() => {},
                _ = tokio::signal::ctrl_c() => {},
            }
            s.shutdown.notify_waiters();
        });
    }

    // Listener: filesystem path (0600 dir recommended) or Linux abstract name.
    let sock = args.socket.clone();
    let listener = if let Some(name) = sock.strip_prefix('@') {
        #[cfg(target_os = "linux")]
        {
            use std::os::linux::net::SocketAddrExt;
            let addr = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes())?;
            let std_l = std::os::unix::net::UnixListener::bind_addr(&addr)?;
            std_l.set_nonblocking(true)?;
            UnixListener::from_std(std_l)?
        }
        #[cfg(not(target_os = "linux"))]
        {
            return Err("abstract sockets are Linux-only".into());
        }
    } else {
        let p = PathBuf::from(&sock);
        // Stale socket from a crashed previous instance: remove only if nobody answers.
        if p.exists() {
            match std::os::unix::net::UnixStream::connect(&p) {
                Ok(_) => return Err(format!("another server is listening on {sock}").into()),
                Err(_) => {
                    tracing::warn!("removing stale socket {sock}");
                    std::fs::remove_file(&p)?;
                }
            }
        }
        if let Some(dir) = p.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let l = UnixListener::bind(&p)?;
        // Belt and braces with the peer-UID check.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600))?;
        }
        l
    };
    eprintln!("gcs-core-server pid={} listening on {}", std::process::id(), sock);
    println!("READY {}", sock); // the thin client waits for this line

    let uds_shared = shared.clone();
    let uds_grpc = grpc.clone();
    let incoming = UnixListenerStream::new(listener).map(|res| {
        if let Ok(ref s) = res {
            #[cfg(unix)]
            unsafe {
                use std::os::unix::io::AsRawFd;
                let fd = s.as_raw_fd();
                let sz: libc::c_int = 8 * 1024 * 1024;
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_SNDBUF,
                    &sz as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&sz) as libc::socklen_t,
                );
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    &sz as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&sz) as libc::socklen_t,
                );
            }
        }
        res
    });
    let uds = Server::builder()
        .initial_stream_window_size(64 * 1024 * 1024)
        .initial_connection_window_size(256 * 1024 * 1024)
        .max_frame_size(4 * 1024 * 1024)
        .add_service(uds_grpc)
        .serve_with_incoming_shutdown(incoming, async move {
            uds_shared.shutdown.notified().await
        });

    if let Some(tcp) = args.tcp.clone() {
        let tcp_shared = shared.clone();
        let addr: std::net::SocketAddr = tcp.parse()?;
        let tcp_srv = Server::builder()
            .initial_stream_window_size(64 * 1024 * 1024)
            .initial_connection_window_size(256 * 1024 * 1024)
            .max_frame_size(4 * 1024 * 1024)
            .add_service(grpc)
            .serve_with_shutdown(addr, async move { tcp_shared.shutdown.notified().await });
        let (a, b) = tokio::join!(uds, tcp_srv);
        a?;
        b?;
    } else {
        uds.await?;
    }

    if !sock.starts_with('@') {
        let _ = std::fs::remove_file(&sock);
    }
    Ok(())
}
