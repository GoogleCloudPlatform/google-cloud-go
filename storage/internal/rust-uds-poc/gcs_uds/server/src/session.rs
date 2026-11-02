//! Per-caller credential sessions. The app lends short-lived OAuth2 access
//! tokens (never key files); the sidecar builds a dedicated Storage client per
//! session with a credentials provider that serves the current token. Sessions
//! are bound to the peer (uid, pid) that created them.

use google_cloud_auth::credentials::{CacheableResource, Credentials, CredentialsProvider, EntityTag};
use google_cloud_storage::client::{Storage, StorageControl};
use http::{HeaderMap, HeaderValue};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::Mutex;

/// Serves `Authorization: Bearer <token>` from a cell the app refreshes.
#[derive(Debug)]
pub struct LentToken {
    inner: RwLock<(String, i64, EntityTag)>, // token, expiry unix secs, tag
}

impl LentToken {
    pub fn set(&self, token: String, expiry: i64) {
        *self.inner.write().unwrap() = (token, expiry, EntityTag::new());
    }
    pub fn expiry(&self) -> i64 {
        self.inner.read().unwrap().1
    }
}

impl CredentialsProvider for LentToken {
    async fn headers(&self, extensions: http::Extensions) -> std::result::Result<CacheableResource<HeaderMap>, google_cloud_auth::errors::CredentialsError> {
        let (tok, exp, tag) = {
            let g = self.inner.read().unwrap();
            (g.0.clone(), g.1, g.2.clone())
        };
        if exp > 0 && exp < crate::now_ms() / 1000 {
            return Err(google_cloud_auth::errors::CredentialsError::from_msg(false, "lent token expired; app must call UpdateToken"));
        }
        if let Some(t) = extensions.get::<EntityTag>() {
            if *t == tag {
                return Ok(CacheableResource::NotModified);
            }
        }
        let mut h = HeaderMap::new();
        let mut v = HeaderValue::from_str(&format!("Bearer {tok}"))
            .map_err(|e| google_cloud_auth::errors::CredentialsError::from_source(false, e))?;
        v.set_sensitive(true); // keeps it out of Debug output / logs
        h.insert(http::header::AUTHORIZATION, v);
        Ok(CacheableResource::New { entity_tag: tag, data: h })
    }
    async fn universe_domain(&self) -> Option<String> {
        None
    }
}

pub struct Session {
    pub id: String,
    pub peer_uid: u32,
    pub peer_pid: i32,
    pub token: Arc<LentToken>,
    pub storage: Storage,
    pub control: StorageControl,
}

pub struct Sessions {
    map: Mutex<HashMap<String, Arc<Session>>>,
    endpoint: Option<String>,
}

fn random_id() -> String {
    let mut b = [0u8; 32];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut b))
        .expect("urandom");
    b.iter().map(|x| format!("{x:02x}")).collect()
}

impl Sessions {
    pub fn new(endpoint: Option<String>) -> Self {
        Self { map: Mutex::new(HashMap::new()), endpoint }
    }

    pub async fn create(&self, token: String, expiry: i64, quota_project: Option<String>, uid: u32, pid: i32)
        -> Result<Arc<Session>, String> {
        let lent = Arc::new(LentToken { inner: RwLock::new((token, expiry, EntityTag::new())) });
        let creds: Credentials = SharedProvider(lent.clone()).into();
        let mut sb = Storage::builder().with_credentials(creds.clone());
        let mut cb = StorageControl::builder().with_credentials(creds);
        if let Some(e) = &self.endpoint {
            sb = sb.with_endpoint(e.clone());
            cb = cb.with_endpoint(e.clone());
        }
        let _ = quota_project; // POC: quota project not plumbed (per-request option in the crate)
        let storage = sb.build().await.map_err(|e| e.to_string())?;
        let control = cb.build().await.map_err(|e| e.to_string())?;
        let s = Arc::new(Session { id: random_id(), peer_uid: uid, peer_pid: pid, token: lent, storage, control });
        self.map.lock().await.insert(s.id.clone(), s.clone());
        Ok(s)
    }

    /// Looks up a session and enforces the peer binding.
    pub async fn get(&self, id: &str, uid: u32, pid: i32) -> Result<Arc<Session>, tonic::Status> {
        let s = self.map.lock().await.get(id).cloned().ok_or_else(|| tonic::Status::unauthenticated("unknown session"))?;
        if s.peer_uid != uid || s.peer_pid != pid {
            return Err(tonic::Status::permission_denied("session is bound to another process"));
        }
        Ok(s)
    }

    pub async fn remove(&self, id: &str) {
        self.map.lock().await.remove(id);
    }

    pub async fn len(&self) -> usize {
        self.map.lock().await.len()
    }
}

/// Arc wrapper so the same token cell is shared by the provider and the session.
#[derive(Debug)]
struct SharedProvider(Arc<LentToken>);
impl CredentialsProvider for SharedProvider {
    async fn headers(&self, extensions: http::Extensions) -> std::result::Result<CacheableResource<HeaderMap>, google_cloud_auth::errors::CredentialsError> {
        self.0.headers(extensions).await
    }
    async fn universe_domain(&self) -> Option<String> {
        None
    }
}
