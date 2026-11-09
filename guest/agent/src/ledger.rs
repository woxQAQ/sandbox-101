//! Private, host-controlled execution intents. Keys are never expired or evicted.
use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    fs::{File, OpenOptions},
    io::Read,
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::Mutex,
};

pub const OUTPUT_BYTES: usize = 64 * 1024;
pub const MAX_ENTRIES: u32 = 1024;

#[derive(Default)]
pub struct Saved {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub exit_code: i32,
}
impl Saved {
    pub fn append(&mut self, stream: &str, bytes: &[u8]) {
        let (buffer, count) = if stream == "stdout" {
            (&mut self.stdout, &mut self.stdout_bytes)
        } else {
            (&mut self.stderr, &mut self.stderr_bytes)
        };
        *count = count.saturating_add(bytes.len() as u64);
        buffer.extend_from_slice(&bytes[..bytes.len().min(OUTPUT_BYTES - buffer.len())]);
    }
}

pub enum Admission {
    New([u8; 32]),
    Completed(Saved),
    InProgress,
    Unknown,
    Conflict,
    Exhausted,
}

pub struct Ledger {
    connection: Mutex<Connection>,
    salt: [u8; 32],
    max_entries: u32,
    pub directory: PathBuf,
    // Fields drop in declaration order: close SQLite before releasing ownership.
    _lock: LedgerLock,
}

struct LedgerLock {
    file: File,
    owner_pid: libc::pid_t,
}

impl LedgerLock {
    fn acquire(path: &Path) -> Result<Self> {
        let file = private_file(path)?;
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "exec ledger already owned by another agent"
        );
        Ok(Self {
            file,
            owner_pid: unsafe { libc::getpid() },
        })
    }
}

impl Drop for LedgerLock {
    fn drop(&mut self) {
        // flock follows the open-file description, so close alone would let an
        // unrelated fork-before-exec child prolong a finished Ledger's ownership.
        // An inherited guard may close its copy, but must not unlock a live owner.
        if unsafe { libc::getpid() } == self.owner_pid {
            unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}

fn private_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let m = file.metadata()?;
    ensure!(
        m.is_file()
            && m.uid() == unsafe { libc::geteuid() }
            && m.mode() & 0o7777 == 0o600
            && m.nlink() == 1,
        "unsafe exec ledger file"
    );
    Ok(file)
}

impl Ledger {
    pub fn open(directory: &Path, max_entries: u32) -> Result<Self> {
        ensure!(
            (1..=MAX_ENTRIES).contains(&max_entries),
            "invalid exec ledger admission bound"
        );
        ensure!(
            directory.is_absolute(),
            "exec ledger directory must be absolute"
        );
        match std::fs::DirBuilder::new().mode(0o700).create(directory) {
            Ok(()) => {
                File::open(directory.parent().unwrap())?.sync_all()?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let m = std::fs::symlink_metadata(directory)?;
        ensure!(
            m.is_dir() && m.uid() == unsafe { libc::geteuid() } && m.mode() & 0o7777 == 0o700,
            "unsafe exec ledger directory"
        );
        let directory = directory.canonicalize()?;
        let lock = LedgerLock::acquire(&directory.join("agent.lock"))?;
        let path = directory.join("exec.sqlite3");
        let db_file = private_file(&path)?;
        db_file.sync_all()?;
        File::open(&directory)?.sync_all()?;
        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;
            PRAGMA secure_delete=ON; PRAGMA temp_store=MEMORY; PRAGMA max_page_count=40960;",
        )?;
        let version: u32 = connection.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        ensure!(version <= 1, "unsupported exec ledger schema");
        connection.execute_batch(
            "BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS identity (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                sandbox_id TEXT NOT NULL, salt BLOB NOT NULL CHECK(length(salt)=32));
            CREATE TABLE IF NOT EXISTS executions (
                key_hash BLOB PRIMARY KEY CHECK(length(key_hash)=32),
                params_hash BLOB NOT NULL CHECK(length(params_hash)=32),
                state TEXT NOT NULL CHECK(state IN ('in_progress','completed','result_unknown')),
                admitted_at INTEGER NOT NULL DEFAULT(unixepoch()), finished_at INTEGER,
                exit_code INTEGER, stdout BLOB, stderr BLOB,
                stdout_bytes INTEGER, stderr_bytes INTEGER,
                CHECK(length(stdout)<=65536), CHECK(length(stderr)<=65536));
            PRAGMA user_version=1; COMMIT;",
        )?;
        let mut salt = [0; 32];
        File::open("/dev/urandom")?.read_exact(&mut salt)?;
        connection.execute(
            "INSERT OR IGNORE INTO identity VALUES(1,'',?1)",
            [&salt[..]],
        )?;
        let salt: Vec<u8> =
            connection.query_row("SELECT salt FROM identity WHERE singleton=1", [], |r| {
                r.get(0)
            })?;
        // No process may be inferred to have completed from its PID after restart.
        connection.execute(
            "UPDATE executions SET state='result_unknown', finished_at=unixepoch()
            WHERE state='in_progress'",
            [],
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
            salt: salt.try_into().unwrap(),
            max_entries,
            directory,
            _lock: lock,
        })
    }

    pub fn identity(&self) -> Result<String> {
        Ok(self.connection.lock().unwrap().query_row(
            "SELECT sandbox_id FROM identity WHERE singleton=1",
            [],
            |r| r.get(0),
        )?)
    }

    pub fn set_identity(&self, id: &str, prepare: bool) -> Result<()> {
        let mut db = self.connection.lock().unwrap();
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let old: String = tx.query_row(
            "SELECT sandbox_id FROM identity WHERE singleton=1",
            [],
            |r| r.get(0),
        )?;
        if old == id && !prepare {
            return Ok(());
        }
        let running: u32 = tx.query_row(
            "SELECT count(*) FROM executions WHERE state='in_progress'",
            [],
            |r| r.get(0),
        )?;
        ensure!(
            running == 0,
            "keyed exec is still in progress; drain before changing identity"
        );
        tx.execute("DELETE FROM executions", [])?;
        tx.execute("UPDATE identity SET sandbox_id=?1 WHERE singleton=1", [id])?;
        tx.commit()?;
        // secure_delete also clears freed pages; vacuum prevents template size/history inheritance.
        db.execute_batch("VACUUM")?;
        Ok(())
    }

    pub fn fingerprint(&self, domain: &str, data: &[u8]) -> [u8; 32] {
        let mut hash = blake3::Hasher::new_keyed(&self.salt);
        hash.update(domain.as_bytes());
        hash.update(&[0]);
        hash.update(data);
        *hash.finalize().as_bytes()
    }

    pub fn admit(&self, identity: &str, key: &str, fingerprint: &[u8; 32]) -> Result<Admission> {
        let key = self.fingerprint("exec-key-v1", key.as_bytes());
        let mut db = self.connection.lock().unwrap();
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let scope: String = tx.query_row(
            "SELECT sandbox_id FROM identity WHERE singleton=1",
            [],
            |r| r.get(0),
        )?;
        ensure!(scope == identity, "exec ledger identity changed");
        let old = tx
            .query_row(
                "SELECT params_hash,state,exit_code,stdout,stderr,stdout_bytes,stderr_bytes
            FROM executions WHERE key_hash=?1",
                [&key[..]],
                |r| {
                    let hash: Vec<u8> = r.get(0)?;
                    let state: String = r.get(1)?;
                    let saved = if state == "completed" {
                        Some(Saved {
                            exit_code: r.get(2)?,
                            stdout: r.get(3)?,
                            stderr: r.get(4)?,
                            stdout_bytes: r.get(5)?,
                            stderr_bytes: r.get(6)?,
                        })
                    } else {
                        None
                    };
                    Ok((hash, state, saved))
                },
            )
            .optional()?;
        if let Some((hash, state, saved)) = old {
            return Ok(if hash != fingerprint {
                Admission::Conflict
            } else if let Some(saved) = saved {
                Admission::Completed(saved)
            } else if state == "in_progress" {
                Admission::InProgress
            } else {
                Admission::Unknown
            });
        }
        let count: u32 = tx.query_row("SELECT count(*) FROM executions", [], |r| r.get(0))?;
        if count >= self.max_entries {
            return Ok(Admission::Exhausted);
        }
        tx.execute(
            "INSERT INTO executions(key_hash,params_hash,state) VALUES(?1,?2,'in_progress')",
            params![&key[..], &fingerprint[..]],
        )?;
        tx.commit()?; // FULL synchronous commit precedes any spawn or output event.
        Ok(Admission::New(key))
    }

    pub fn complete(&self, key: &[u8; 32], saved: &Saved) -> Result<()> {
        let changed = self.connection.lock().unwrap().execute(
            "UPDATE executions SET state='completed',finished_at=unixepoch(),exit_code=?2,
                stdout=?3,stderr=?4,stdout_bytes=?5,stderr_bytes=?6 WHERE key_hash=?1 AND state='in_progress'",
            params![&key[..], saved.exit_code, &saved.stdout, &saved.stderr, saved.stdout_bytes, saved.stderr_bytes])?;
        ensure!(changed == 1, "exec intent missing at completion");
        Ok(())
    }

    pub fn unknown(&self, key: &[u8; 32]) -> Result<()> {
        self.connection.lock().unwrap().execute(
            "UPDATE executions SET state='result_unknown',finished_at=unixepoch() WHERE key_hash=?1 AND state='in_progress'",
            [&key[..]])?;
        Ok(())
    }
}
