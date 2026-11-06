pub mod fs;
pub mod keyed;
pub mod ledger;
pub mod process;
pub mod proxy;
pub mod server;
pub mod session;
pub mod system;
pub mod wire;

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, Semaphore};

#[derive(Clone)]
pub struct Config {
    pub env_file: PathBuf,
    pub machine_id: PathBuf,
    pub ledger_dir: PathBuf,
    pub ledger_max_entries: u32,
    pub uid: u32,
    pub gid: u32,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            env_file: "/run/sandbox.env".into(),
            machine_id: "/etc/machine-id".into(),
            ledger_dir: "/var/lib/sandbox-agent".into(),
            ledger_max_entries: ledger::MAX_ENTRIES,
            uid: 1000,
            gid: 1000,
        }
    }
}
#[derive(Default)]
pub struct Activation {
    pub sandbox_id: String,
    pub network: String,
    pub generation: u64,
}
pub struct Agent {
    pub config: Config,
    pub ledger: ledger::Ledger,
    pub activation: Mutex<Activation>,
    pub processes: Mutex<BTreeMap<String, Arc<process::Process>>>,
    pub sessions: Mutex<BTreeMap<String, Arc<Mutex<session::Session>>>>,
    pub requests: Arc<Semaphore>,
    pub control_requests: Arc<Semaphore>,
    pub policy_changed: tokio::sync::watch::Sender<u64>,
    pub quiesced: std::sync::atomic::AtomicBool,
    pub active_streams: std::sync::atomic::AtomicUsize,
}
impl Agent {
    pub fn new(config: Config) -> anyhow::Result<Arc<Self>> {
        let ledger = ledger::Ledger::open(&config.ledger_dir, config.ledger_max_entries)?;
        let sandbox_id = ledger.identity()?;
        Ok(Arc::new(Self {
            config,
            ledger,
            activation: Mutex::new(Activation {
                sandbox_id,
                network: "none".into(),
                ..Default::default()
            }),
            processes: Mutex::new(BTreeMap::new()),
            sessions: Mutex::new(BTreeMap::new()),
            requests: Arc::new(Semaphore::new(128)),
            control_requests: Arc::new(Semaphore::new(32)),
            policy_changed: tokio::sync::watch::channel(0).0,
            quiesced: std::sync::atomic::AtomicBool::new(false),
            active_streams: std::sync::atomic::AtomicUsize::new(0),
        }))
    }
    pub async fn environment(&self) -> anyhow::Result<BTreeMap<String, String>> {
        let activation = self.activation.lock().await;
        self.environment_for(&activation).await
    }
    pub(crate) async fn environment_for(
        &self,
        activation: &Activation,
    ) -> anyhow::Result<BTreeMap<String, String>> {
        let mut env = match tokio::fs::read(&self.config.env_file).await {
            Ok(data) => serde_json::from_slice(&data)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(e.into()),
        };
        if activation.network == "relay" {
            for k in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"] {
                env.insert(k.into(), "http://127.0.0.1:3128".into());
            }
            for k in ["ALL_PROXY", "all_proxy"] {
                env.insert(k.into(), "socks5h://127.0.0.1:1080".into());
            }
            env.insert("NO_PROXY".into(), "localhost,127.0.0.1,::1".into());
            let ca = self.config.env_file.with_file_name("sandbox-ca.pem");
            if ca.exists() {
                for k in [
                    "SSL_CERT_FILE",
                    "REQUESTS_CA_BUNDLE",
                    "CURL_CA_BUNDLE",
                    "NODE_EXTRA_CA_CERTS",
                ] {
                    env.insert(k.into(), ca.to_string_lossy().to_string());
                }
            }
        }
        Ok(env)
    }
    pub async fn reap(&self) {
        self.processes.lock().await.retain(|_, p| !p.expired());
        let candidates: Vec<_> = self
            .sessions
            .lock()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (id, session) in candidates {
            if let Ok(mut s) = session.try_lock()
                && (s.last_used.elapsed().as_secs() >= 1800 || s.has_exited())
            {
                s.kill();
                let _ = s.child.wait().await;
                self.sessions.lock().await.remove(&id);
            }
        }
    }
}

pub fn string<'a>(p: &'a serde_json::Value, key: &str) -> anyhow::Result<&'a str> {
    p.get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("{key} must be a string"))
}
pub fn number(p: &serde_json::Value, key: &str) -> anyhow::Result<u64> {
    let n = p
        .get(key)
        .and_then(|v| v.as_f64())
        .ok_or_else(|| anyhow::anyhow!("{key} must be an integer"))?;
    anyhow::ensure!(
        n.is_finite() && n >= 0. && n.fract() == 0. && n <= 9007199254740991.,
        "invalid {key}"
    );
    Ok(n as u64)
}
pub fn env_map(p: &serde_json::Value) -> anyhow::Result<BTreeMap<String, String>> {
    let Some(value) = p.get("env") else {
        return Ok(BTreeMap::new());
    };
    let values = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("env must be an object"))?;
    values
        .iter()
        .map(|(k, v)| {
            anyhow::ensure!(
                !k.is_empty() && !k.contains(['=', '\0']),
                "invalid environment key"
            );
            let value = v
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("environment values must be strings"))?;
            anyhow::ensure!(!value.contains('\0'), "invalid environment value");
            Ok((k.clone(), value.into()))
        })
        .collect()
}
