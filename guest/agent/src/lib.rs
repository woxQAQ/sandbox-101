// Validation defaults to invalid; runtime and admission checks name their code.
macro_rules! bail {
    ($code:ident => $($arg:tt)*) => {
        return Err(crate::Error::new(crate::ErrorCode::$code, format!($($arg)*)))
    };
    ($($arg:tt)*) => { bail!(Invalid => $($arg)*) };
}
macro_rules! ensure {
    ($condition:expr, $code:ident => $($arg:tt)*) => {
        if !$condition { bail!($code => $($arg)*); }
    };
    ($condition:expr, $($arg:tt)*) => {
        ensure!($condition, Invalid => $($arg)*);
    };
}

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

pub type Result<T> = std::result::Result<T, Error>;

/// Flat recovery categories; keyed execution retains its more precise fence codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Invalid,
    NotFound,
    Conflict,
    ResourceExhausted,
    Quiesced,
    Unauthorized,
    ExecInProgress,
    ExecKeyConflict,
    ExecLedgerCapacityExhausted,
    ResultUnknown,
    Internal,
}
impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Quiesced => "quiesced",
            Self::Unauthorized => "unauthorized",
            Self::ExecInProgress => "exec_in_progress",
            Self::ExecKeyConflict => "exec_key_conflict",
            Self::ExecLedgerCapacityExhausted => "exec_ledger_capacity_exhausted",
            Self::ResultUnknown => "result_unknown",
            Self::Internal => "internal",
        }
    }
}

#[derive(Debug)]
struct ErrorFrame {
    message: String,
    location: &'static std::panic::Location<'static>,
}

#[derive(Debug)]
pub struct Error {
    pub code: ErrorCode,
    pub params: serde_json::Value,
    frames: Vec<ErrorFrame>,
}
impl Error {
    #[track_caller]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            params: serde_json::json!({}),
            frames: vec![ErrorFrame {
                message: message.into(),
                location: std::panic::Location::caller(),
            }],
        }
    }
    #[track_caller]
    pub fn context(mut self, message: impl Into<String>) -> Self {
        self.frames.push(ErrorFrame {
            message: message.into(),
            location: std::panic::Location::caller(),
        });
        self
    }
    pub fn with_params(mut self, params: serde_json::Value) -> Self {
        self.params = params;
        self
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, frame) in self.frames.iter().rev().enumerate() {
            if index != 0 {
                write!(f, ": ")?;
            }
            write!(f, "{} at {}", frame.message, frame.location)?;
        }
        Ok(())
    }
}
impl std::error::Error for Error {}

/// Context closures run only on failure; caller locations require no backtrace.
pub trait ResultContext<T> {
    #[track_caller]
    fn or_context(self, context: impl FnOnce() -> String) -> Result<T>;
    #[track_caller]
    fn map_code(self, code: ErrorCode, context: impl FnOnce() -> String) -> Result<T>;
}
impl<T, E: Into<Error>> ResultContext<T> for std::result::Result<T, E> {
    #[track_caller]
    fn or_context(self, context: impl FnOnce() -> String) -> Result<T> {
        match self {
            Ok(value) => Ok(value),
            Err(error) => Err(error.into().context(context())),
        }
    }
    #[track_caller]
    fn map_code(self, code: ErrorCode, context: impl FnOnce() -> String) -> Result<T> {
        match self {
            Ok(value) => Ok(value),
            Err(error) => {
                let mut error = error.into();
                error.code = code;
                Err(error.context(context()))
            }
        }
    }
}

impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(error: std::io::Error) -> Self {
        use std::io::ErrorKind;
        if error.get_ref().is_some_and(|e| e.is::<Error>()) {
            return *error.into_inner().unwrap().downcast::<Error>().unwrap();
        }
        let code = match error.kind() {
            ErrorKind::NotFound => ErrorCode::NotFound,
            ErrorKind::AlreadyExists
            | ErrorKind::NotADirectory
            | ErrorKind::IsADirectory
            | ErrorKind::CrossesDevices
            | ErrorKind::DirectoryNotEmpty => ErrorCode::Conflict,
            ErrorKind::InvalidInput | ErrorKind::InvalidData => ErrorCode::Invalid,
            ErrorKind::PermissionDenied => ErrorCode::Unauthorized,
            ErrorKind::StorageFull | ErrorKind::QuotaExceeded => ErrorCode::ResourceExhausted,
            _ => ErrorCode::Internal,
        };
        Self::new(code, format!("I/O: {error}"))
    }
}
macro_rules! error_from {
    ($type:ty, $code:ident, $context:literal) => {
        impl From<$type> for Error {
            #[track_caller]
            fn from(error: $type) -> Self {
                Self::new(ErrorCode::$code, format!("{}: {error}", $context))
            }
        }
    };
}
error_from!(rusqlite::Error, Internal, "exec ledger SQLite");
error_from!(serde_json::Error, Internal, "JSON");
error_from!(prost::DecodeError, Invalid, "decode frame");
error_from!(std::ffi::NulError, Invalid, "NUL in string");
error_from!(std::str::Utf8Error, Invalid, "UTF-8");
error_from!(std::num::ParseIntError, Invalid, "integer");
error_from!(std::num::TryFromIntError, Invalid, "integer range");
error_from!(base64::DecodeError, Invalid, "base64");
error_from!(tokio::task::JoinError, Internal, "join task");
error_from!(tokio::time::error::Elapsed, Internal, "deadline");
error_from!(
    tokio::sync::oneshot::error::RecvError,
    Internal,
    "fixture observer"
);
error_from!(walkdir::Error, Internal, "walk filesystem");

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
    pub fn new(config: Config) -> Result<Arc<Self>> {
        let ledger = ledger::Ledger::open(&config.ledger_dir, config.ledger_max_entries)
            .or_context(|| format!("open exec ledger {}", config.ledger_dir.display()))?;
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
    pub async fn environment(&self) -> Result<BTreeMap<String, String>> {
        let activation = self.activation.lock().await;
        self.environment_for(&activation).await
    }
    pub(crate) async fn environment_for(
        &self,
        activation: &Activation,
    ) -> Result<BTreeMap<String, String>> {
        let mut env = match tokio::fs::read(&self.config.env_file).await {
            Ok(data) => serde_json::from_slice(&data).map_code(ErrorCode::Internal, || {
                format!(
                    "decode activation environment {}",
                    self.config.env_file.display()
                )
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => {
                return Err(Error::from(e).context(format!(
                    "read activation environment {}",
                    self.config.env_file.display()
                )));
            }
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

#[track_caller]
pub fn string<'a>(p: &'a serde_json::Value, key: &str) -> Result<&'a str> {
    match p.get(key).and_then(|v| v.as_str()) {
        Some(value) => Ok(value),
        None => Err(Error::new(
            ErrorCode::Invalid,
            format!("{key} must be a string"),
        )),
    }
}
#[track_caller]
pub fn number(p: &serde_json::Value, key: &str) -> Result<u64> {
    let Some(n) = p.get(key).and_then(|v| v.as_f64()) else {
        bail!("{key} must be an integer");
    };
    ensure!(
        n.is_finite() && n >= 0. && n.fract() == 0. && n <= 9007199254740991.,
        "invalid {key}"
    );
    Ok(n as u64)
}
pub fn env_map(p: &serde_json::Value) -> Result<BTreeMap<String, String>> {
    let Some(value) = p.get("env") else {
        return Ok(BTreeMap::new());
    };
    let values = value
        .as_object()
        .ok_or_else(|| Error::new(ErrorCode::Invalid, "env must be an object"))?;
    values
        .iter()
        .map(|(k, v)| {
            ensure!(
                !k.is_empty() && !k.contains(['=', '\0']),
                "invalid environment key"
            );
            let value = v.as_str().ok_or_else(|| {
                Error::new(ErrorCode::Invalid, "environment values must be strings")
            })?;
            ensure!(!value.contains('\0'), "invalid environment value");
            Ok((k.clone(), value.into()))
        })
        .collect()
}
