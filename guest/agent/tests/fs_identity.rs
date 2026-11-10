#![cfg(feature = "filesystem-fixtures")]

use sandbox_agent::{
    Agent, Config,
    fs::fixtures,
    server::{Portal, serve_connection},
    wire::{CHUNK, Frame, read_frame, write_frame},
};
use serde_json::json;
use std::{
    ffi::CString,
    fs::{File, Permissions},
    io::Read,
    os::unix::{
        ffi::OsStrExt, fs::MetadataExt, fs::PermissionsExt, fs::symlink, process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::net::UnixStream;

const DEADLINE: Duration = Duration::from_secs(15);

// Every privileged scenario runs in a fresh exec, with a parent hard deadline.
fn isolated(name: &str, scenario: impl FnOnce()) {
    if std::env::var("FS_IDENTITY_CHILD").as_deref() == Ok(name) {
        prove_namespaces();
        scenario();
        return;
    }
    if std::env::var("FS_IDENTITY_REQUIRED").as_deref() != Ok("1") {
        eprintln!("SKIP {name}: requires make guest-fs-isolation-e2e");
        return;
    }
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env("FS_IDENTITY_CHILD", name)
        .stdin(Stdio::null());
    run_child(command);
}

fn run_child(mut command: Command) {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let mut stdout = String::new();
            let mut stderr = String::new();
            child
                .stdout
                .take()
                .unwrap()
                .read_to_string(&mut stdout)
                .unwrap();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            print!("{stdout}");
            eprint!("{stderr}");
            assert!(status.success(), "fixture child failed: {status}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("fixture child exceeded hard deadline");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn prove_namespaces() {
    assert_eq!(
        unsafe {
            (
                libc::getuid(),
                libc::geteuid(),
                libc::getgid(),
                libc::getegid(),
            )
        },
        (0, 0, 0, 0)
    );
    for (kind, variable) in [
        ("user", "FS_PARENT_USER"),
        ("mnt", "FS_PARENT_MNT"),
        ("pid", "FS_PARENT_PID"),
    ] {
        let current = std::fs::read_link(format!("/proc/self/ns/{kind}")).unwrap();
        assert_ne!(
            current,
            PathBuf::from(std::env::var(variable).unwrap()),
            "namespace was inherited"
        );
    }
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let effective = status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .unwrap()
        .trim();
    let effective = u64::from_str_radix(effective, 16).unwrap();
    let required = (1 << 1) | (1 << 6) | (1 << 7) | (1 << 21);
    assert_eq!(
        effective & required,
        required,
        "daemon lacks DAC_OVERRIDE/SETGID/SETUID/SYS_ADMIN"
    );
    println!(
        "daemon proof: UID/GID=0; DAC_OVERRIDE/SETGID/SETUID/SYS_ADMIN effective; fresh user/mount/PID namespaces"
    );
    for kind in ["uid", "gid"] {
        let mapping = std::fs::read_to_string(format!("/proc/self/{kind}_map")).unwrap();
        let maps: Vec<Vec<u32>> = mapping
            .lines()
            .map(|line| {
                line.split_whitespace()
                    .map(|n| n.parse().unwrap())
                    .collect()
            })
            .collect();
        assert!(
            maps.iter().any(|m| m[0] <= 1 && 1 - m[0] < m[2]),
            "workload ID is not mapped"
        );
        assert!(
            maps.iter().any(|m| m[0] == 0 && m[2] == 1),
            "daemon must have a distinct mapping"
        );
        println!(
            "{kind}_map: {}",
            mapping.split_whitespace().collect::<Vec<_>>().join(" ")
        );
    }
}

fn owner(path: &Path, uid: u32, gid: u32, mode: u32) {
    let name = CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::chown(name.as_ptr(), uid, gid) }, 0);
    std::fs::set_permissions(path, Permissions::from_mode(mode)).unwrap();
}

struct Context {
    _root: tempfile::TempDir,
    source: PathBuf,
    sentinel: PathBuf,
    sentinel_bytes: Vec<u8>,
    agent: Arc<Agent>,
}
impl Context {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        owner(root.path(), 0, 0, 0o755);
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        owner(&source, 1, 1, 0o700);
        std::fs::write(source.join("allowed"), b"authorized fixture bytes").unwrap();
        owner(&source.join("allowed"), 1, 1, 0o600);
        let private = root.path().join("root-private");
        std::fs::create_dir(&private).unwrap();
        owner(&private, 0, 0, 0o700);
        let sentinel = private.join("sentinel");
        let mut sentinel_bytes = vec![0; 64];
        assert_eq!(
            unsafe { libc::getrandom(sentinel_bytes.as_mut_ptr().cast(), sentinel_bytes.len(), 0) },
            64
        );
        std::fs::write(&sentinel, &sentinel_bytes).unwrap();
        owner(&sentinel, 0, 0, 0o600);
        let agent = Agent::new(Config {
            env_file: root.path().join("unused.env"),
            machine_id: root.path().join("unused-machine-id"),
            ledger_dir: source.join("ledger"),
            ledger_max_entries: sandbox_agent::ledger::MAX_ENTRIES,
            uid: 1,
            gid: 1,
        })
        .unwrap();
        Self {
            _root: root,
            source,
            sentinel,
            sentinel_bytes,
            agent,
        }
    }

    fn prove_workload(&self) {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "identity_fixture_proves_distinct_authorities",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("FS_WORKLOAD_PROBE", "1")
            .env("FS_PROBE_ALLOWED", self.source.join("allowed"))
            .env("FS_PROBE_SENTINEL", &self.sentinel)
            .env(
                "FS_PROBE_SENTINEL_DEV",
                std::fs::metadata(&self.sentinel).unwrap().dev().to_string(),
            )
            .env(
                "FS_PROBE_SENTINEL_INO",
                std::fs::metadata(&self.sentinel).unwrap().ino().to_string(),
            )
            .env_remove("FS_IDENTITY_CHILD")
            .stdin(Stdio::null());
        // exec closes CLOEXEC descriptors. No sentinel File is ever kept open.
        unsafe {
            command.pre_exec(|| {
                for cap in 0..64 {
                    if libc::prctl(libc::PR_CAPBSET_DROP, cap, 0, 0, 0) != 0
                        && std::io::Error::last_os_error().raw_os_error() != Some(libc::EINVAL)
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                if libc::setgroups(0, std::ptr::null()) != 0
                    || libc::setresgid(1, 1, 1) != 0
                    || libc::setresuid(1, 1, 1) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        run_child(command);
    }
}

fn workload_probe() {
    assert_eq!(
        unsafe {
            (
                libc::getuid(),
                libc::geteuid(),
                libc::getgid(),
                libc::getegid(),
            )
        },
        (1, 1, 1, 1)
    );
    let mut uids = [0; 3];
    let mut gids = [0; 3];
    unsafe {
        assert_eq!(libc::getresuid(&mut uids[0], &mut uids[1], &mut uids[2]), 0);
        assert_eq!(libc::getresgid(&mut gids[0], &mut gids[1], &mut gids[2]), 0);
    }
    assert_eq!(uids, [1; 3]);
    assert_eq!(gids, [1; 3]);
    assert_eq!(unsafe { libc::getgroups(0, std::ptr::null_mut()) }, 0);
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    for key in ["CapInh:", "CapPrm:", "CapEff:", "CapBnd:", "CapAmb:"] {
        let value = status
            .lines()
            .find_map(|line| line.strip_prefix(key))
            .unwrap()
            .trim();
        assert_eq!(
            u64::from_str_radix(value, 16).unwrap(),
            0,
            "workload retains {key}"
        );
    }
    let sentinel = PathBuf::from(std::env::var("FS_PROBE_SENTINEL").unwrap());
    let denied = File::open(&sentinel).unwrap_err();
    assert_eq!(denied.kind(), std::io::ErrorKind::PermissionDenied);
    let sentinel_device: u64 = std::env::var("FS_PROBE_SENTINEL_DEV")
        .unwrap()
        .parse()
        .unwrap();
    let sentinel_inode: u64 = std::env::var("FS_PROBE_SENTINEL_INO")
        .unwrap()
        .parse()
        .unwrap();
    for entry in std::fs::read_dir("/proc/self/fd").unwrap() {
        let entry = entry.unwrap();
        if let Ok(metadata) = std::fs::metadata(entry.path()) {
            assert_ne!(
                (metadata.dev(), metadata.ino()),
                (sentinel_device, sentinel_inode),
                "workload inherited sentinel inode"
            );
        }
        if let Ok(path) = std::fs::read_link(entry.path()) {
            assert_ne!(path, sentinel, "workload inherited sentinel descriptor");
        }
    }
    assert!(
        std::fs::read(std::env::var("FS_PROBE_ALLOWED").unwrap()).unwrap()
            == b"authorized fixture bytes"
    );
    println!(
        "workload proof: real/effective/saved UID/GID=1; groups empty; all capability sets zero; private fixture EACCES; authorized file readable; no sentinel FD"
    );
}

fn runtime(scenario: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(scenario);
}
fn connect(agent: &Arc<Agent>) -> UnixStream {
    let (client, server) = UnixStream::pair().unwrap();
    let agent = agent.clone();
    tokio::spawn(async move {
        let _ = serve_connection(agent, server, Portal::Workload).await;
    });
    client
}
async fn start(context: &Context) -> UnixStream {
    let mut client = connect(&context.agent);
    let request = Frame {
        id: "identity-tree".into(),
        method: "tree_pull".into(),
        ..Default::default()
    }
    .reply(json!({"path": context.source}), vec![], true);
    write_frame(&mut client, &request).await.unwrap();
    client
}
async fn receive(client: &mut UnixStream) -> (Vec<u8>, String) {
    tokio::time::timeout(DEADLINE, async {
        let mut bytes = Vec::new();
        loop {
            let frame = read_frame(client)
                .await
                .unwrap()
                .expect("RPC closed without completion");
            assert!(frame.data.len() <= CHUNK);
            bytes.extend(frame.data);
            if frame.eof {
                return (bytes, frame.error);
            }
        }
    })
    .await
    .expect("tree pull blocked")
}
fn contents(bytes: &[u8]) -> Vec<(PathBuf, Vec<u8>)> {
    let mut archive = tar::Archive::new(bytes);
    archive
        .entries()
        .unwrap()
        .map(|entry| {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().into_owned();
            let mut data = Vec::new();
            entry.read_to_end(&mut data).unwrap();
            (path, data)
        })
        .collect()
}
fn no_sentinel(context: &Context, bytes: &[u8]) {
    assert!(
        !bytes
            .windows(context.sentinel_bytes.len())
            .any(|part| part == context.sentinel_bytes),
        "private fixture appeared in output"
    );
}
async fn settled(context: &Context, snapshot: Option<&fixtures::Snapshot>) {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let mut alive = false;
            if let Some(snapshot) = snapshot {
                for entry in std::fs::read_dir("/proc/self/fd").unwrap() {
                    if let Ok(metadata) = std::fs::metadata(entry.unwrap().path())
                        && metadata.dev() == snapshot.device
                        && metadata.ino() == snapshot.inode
                    {
                        alive = true;
                    }
                }
            }
            if context.agent.requests.available_permits() == 128 && !alive {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("archive descriptor or RPC slot leaked");
}

fn push_archive(mode: Option<u32>) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    if let Some(mode) = mode {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Directory);
        header.set_mode(mode);
        header.set_size(0);
        header.set_cksum();
        builder
            .append_data(&mut header, "restricted", std::io::empty())
            .unwrap();
    }
    let mut header = tar::Header::new_gnu();
    header.set_mode(0o600);
    header.set_size(9);
    header.set_cksum();
    builder
        .append_data(&mut header, "new", &b"new bytes"[..])
        .unwrap();
    builder.into_inner().unwrap()
}

async fn push(context: &Context, target: &Path, archive: &[u8]) -> Frame {
    let mut client = connect(&context.agent);
    let request = Frame {
        id: "identity-push".into(),
        method: "tree_push".into(),
        ..Default::default()
    }
    .reply(json!({"path": target}), vec![], false);
    write_frame(&mut client, &request).await.unwrap();
    for (index, chunk) in archive.chunks(CHUNK).enumerate() {
        write_frame(
            &mut client,
            &Frame {
                id: request.id.clone(),
                data: chunk.to_vec(),
                eof: (index + 1) * CHUNK >= archive.len(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    tokio::time::timeout(DEADLINE, async {
        loop {
            let frame = read_frame(&mut client)
                .await
                .unwrap()
                .expect("push closed without terminal frame");
            assert!(frame.data.is_empty());
            if frame.eof {
                return frame;
            }
        }
    })
    .await
    .expect("tree push blocked")
}

fn directory_names(path: &Path) -> Vec<std::ffi::OsString> {
    let mut names: Vec<_> = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    names
}

#[test]
fn tree_push_mixed_owner_rejected_before_commit() {
    isolated("tree_push_mixed_owner_rejected_before_commit", || {
        let context = Context::new();
        context.prove_workload();
        let target = context.source.join("tree");
        let nested = target.join("readonly");
        std::fs::create_dir_all(&nested).unwrap();
        owner(&target, 1, 1, 0o700);
        std::fs::write(nested.join("old"), b"old bytes").unwrap();
        owner(&nested.join("old"), 0, 0, 0o444);
        owner(&nested, 0, 0, 0o555);
        let before = directory_names(&context.source);
        let inode = std::fs::metadata(&target).unwrap().ino();
        runtime(async {
            let frame = push(&context, &target, &push_archive(None)).await;
            assert!(
                !frame.error.is_empty(),
                "mixed-owner admission RED: tree committed instead of rejecting"
            );
            assert_eq!(frame.params_json()["error_code"], "unauthorized");
            assert!(frame.params_json().get("committed").is_none());
            assert_eq!(std::fs::metadata(&target).unwrap().ino(), inode);
            let unchanged = std::fs::metadata(&nested).unwrap();
            assert_eq!((unchanged.uid(), unchanged.mode() & 0o777), (0, 0o555));
            assert!(!target.join("new").exists());
            assert_eq!(std::fs::read(nested.join("old")).unwrap(), b"old bytes");
            assert_eq!(
                directory_names(&context.source),
                before,
                "precommit staging leaked"
            );
            // The fixture owner changes only its own read-only directory for the control.
            owner(&nested, 1, 1, 0o555);
            let frame = push(&context, &target, &push_archive(None)).await;
            assert!(frame.error.is_empty(), "{}", frame.error);
            assert_eq!(std::fs::read(target.join("new")).unwrap(), b"new bytes");
            assert_eq!(directory_names(&context.source), before);
            // Restoring access to this newly published fixture is test teardown only.
            owner(&target.join("readonly"), 1, 1, 0o700);
            settled(&context, None).await;
        });
    });
}

fn assert_clean_push(frame: &Frame) {
    assert!(frame.error.is_empty(), "{}", frame.error);
    assert_eq!(
        frame.params_json(),
        json!({
            "committed": true,
            "publication_durable": true,
            "cleanup_complete": true,
            "cleanup_required": false,
        })
    );
}

fn pending_evidence(frame: &Frame, target: &Path, old_inode: u64, durable: bool) -> PathBuf {
    let params = frame.params_json();
    assert_eq!(params["committed"], true);
    assert_eq!(params["publication_durable"], durable);
    assert_eq!(params["cleanup_complete"], false);
    assert_eq!(params["cleanup_required"], true);
    assert_eq!(
        params["cleanup_reason"],
        if durable {
            "old_tree_cleanup_failed"
        } else {
            "publication_sync_failed_cleanup_not_attempted"
        }
    );
    let name = Path::new(params["cleanup_name"].as_str().unwrap());
    assert_eq!(name.components().count(), 1);
    assert!(matches!(
        name.components().next(),
        Some(std::path::Component::Normal(_))
    ));
    assert!(name.file_name().unwrap().as_bytes().starts_with(b".tmp"));
    let evidence = target.parent().unwrap().join(name);
    assert_ne!(evidence, target);
    assert_eq!(std::fs::metadata(&evidence).unwrap().ino(), old_inode);
    assert!(!evidence.join("new").exists());
    assert_eq!(std::fs::read(evidence.join("old")).unwrap(), b"old bytes");
    assert_eq!(std::fs::read(target.join("new")).unwrap(), b"new bytes");
    assert_ne!(std::fs::metadata(target).unwrap().ino(), old_inode);
    evidence
}

fn old_push_tree(target: &Path) -> u64 {
    std::fs::create_dir(target).unwrap();
    owner(target, 1, 1, 0o700);
    std::fs::write(target.join("old"), b"old bytes").unwrap();
    owner(&target.join("old"), 1, 1, 0o600);
    owner(target, 1, 1, 0o555);
    std::fs::metadata(target).unwrap().ino()
}

#[test]
fn tree_push_postcommit_cleanup_outcome_is_explicit() {
    isolated("tree_push_postcommit_cleanup_outcome_is_explicit", || {
        let context = Context::new();
        context.prove_workload();
        let fixture = fixtures::register_push((1, 1), fixtures::PushFailure::Cleanup).unwrap();
        let target = fixture.scratch.path().join("tree");
        let inode = old_push_tree(&target);
        runtime(async {
            // The fault cannot affect another destination, even in the same RPC server.
            let other = context.source.join("unrelated");
            assert_clean_push(&push(&context, &other, &push_archive(None)).await);
            let frame = push(&context, &target, &push_archive(None)).await;
            assert!(
                frame.error.is_empty(),
                "durable publication must report cleanup pending: {}",
                frame.error
            );
            let evidence = pending_evidence(&frame, &target, inode, true);
            let published_inode = std::fs::metadata(&target).unwrap().ino();
            settled(&context, None).await;
            assert_eq!(std::fs::metadata(&target).unwrap().ino(), published_inode);
            assert_eq!(directory_names(fixture.scratch.path()).len(), 2);
            // Registration is consumed by one operation; a later push has no fault.
            assert_clean_push(&push(&context, &target, &push_archive(None)).await);
            assert!(
                evidence.exists(),
                "later push must not collect another operation's evidence"
            );
            assert_eq!(directory_names(fixture.scratch.path()).len(), 2);
        });
    });
}

#[test]
fn tree_push_postcommit_sync_failure_remains_error() {
    isolated("tree_push_postcommit_sync_failure_remains_error", || {
        let context = Context::new();
        context.prove_workload();
        for existing in [true, false] {
            let fixture =
                fixtures::register_push((1, 1), fixtures::PushFailure::PublicationSync).unwrap();
            let target = fixture.scratch.path().join("tree");
            let inode = existing.then(|| old_push_tree(&target));
            runtime(async {
                let frame = push(&context, &target, &push_archive(None)).await;
                assert!(
                    !frame.error.is_empty(),
                    "old client must observe fsync failure"
                );
                assert!(
                    frame
                        .error
                        .contains("tree publication fsync failed after commit")
                );
                assert!(frame.error.contains(target.to_str().unwrap()));
                assert!(frame.error.contains("guest/agent/src/fs.rs:"));
                assert_eq!(frame.params_json()["error_code"], "internal");
                if let Some(inode) = inode {
                    let evidence = pending_evidence(&frame, &target, inode, false);
                    assert_eq!(std::fs::metadata(&evidence).unwrap().mode() & 0o777, 0o555);
                    assert_eq!(directory_names(fixture.scratch.path()).len(), 2);
                } else {
                    assert_eq!(
                        frame.params_json(),
                        json!({
                            "committed": true, "publication_durable": false,
                            "cleanup_complete": true, "cleanup_required": false,
                            "error_code": "internal",
                        })
                    );
                    assert_eq!(directory_names(fixture.scratch.path()).len(), 1);
                }
                let published_inode = std::fs::metadata(&target).unwrap().ino();
                settled(&context, None).await;
                assert_eq!(
                    std::fs::metadata(&target).unwrap().ino(),
                    published_inode,
                    "unexpected second commit"
                );
                assert_eq!(std::fs::read(target.join("new")).unwrap(), b"new bytes");
            });
        }
    });
}

#[test]
fn tree_push_cleanup_success_is_reported() {
    isolated("tree_push_cleanup_success_is_reported", || {
        let context = Context::new();
        context.prove_workload();
        runtime(async {
            for scenario in [
                "new",
                "empty",
                "readonly",
                "foreign-file",
                "foreign-writable",
                "foreign-group-writable",
                "foreign-acl-writable",
                "mode000",
            ] {
                let target = context.source.join(scenario);
                if scenario != "new" {
                    std::fs::create_dir(&target).unwrap();
                    owner(&target, 1, 1, 0o700);
                }
                if scenario == "readonly"
                    || scenario.starts_with("foreign-") && scenario.ends_with("writable")
                {
                    let nested = target.join("nested");
                    std::fs::create_dir(&nested).unwrap();
                    std::fs::write(nested.join("old"), b"old bytes").unwrap();
                    owner(&nested.join("old"), 0, 0, 0o444);
                    if scenario == "readonly" {
                        owner(&nested, 1, 1, 0o555);
                    } else if scenario == "foreign-group-writable" {
                        owner(&nested, 0, 1, 0o770);
                    } else if scenario == "foreign-acl-writable" {
                        owner(&nested, 0, 0, 0o755);
                        // Linux POSIX ACL xattr v2: owner, named workload user,
                        // owning group, mask, other. All bytes are fixture data.
                        let mut acl = 2u32.to_le_bytes().to_vec();
                        for (tag, permissions, id) in [
                            (1u16, 7u16, u32::MAX),
                            (2, 7, 1),
                            (4, 5, u32::MAX),
                            (16, 7, u32::MAX),
                            (32, 5, u32::MAX),
                        ] {
                            acl.extend(tag.to_le_bytes());
                            acl.extend(permissions.to_le_bytes());
                            acl.extend(id.to_le_bytes());
                        }
                        let path = CString::new(nested.as_os_str().as_bytes()).unwrap();
                        assert_eq!(
                            unsafe {
                                libc::setxattr(
                                    path.as_ptr(),
                                    c"system.posix_acl_access".as_ptr(),
                                    acl.as_ptr().cast(),
                                    acl.len(),
                                    0,
                                )
                            },
                            0
                        );
                    } else {
                        owner(&nested, 0, 0, 0o777);
                    }
                }
                if scenario == "foreign-file" {
                    std::fs::write(target.join("foreign"), b"foreign bytes").unwrap();
                    owner(&target.join("foreign"), 0, 0, 0o444);
                }
                let before = directory_names(&context.source);
                let archive = if scenario == "empty" {
                    tar::Builder::new(Vec::new()).into_inner().unwrap()
                } else {
                    push_archive((scenario == "mode000").then_some(0))
                };
                let frame = push(&context, &target, &archive).await;
                assert_clean_push(&frame);
                let mut expected = before;
                if scenario == "new" {
                    expected.push("new".into());
                    expected.sort();
                }
                assert_eq!(
                    directory_names(&context.source),
                    expected,
                    "stage remains after {scenario}"
                );
                if scenario == "mode000" {
                    assert_eq!(
                        std::fs::metadata(target.join("restricted")).unwrap().mode() & 0o777,
                        0
                    );
                    // Teardown touches only the new directory owned by this fixture.
                    owner(&target.join("restricted"), 1, 1, 0o700);
                }
                if scenario == "readonly" {
                    owner(&target.join("nested"), 1, 1, 0o700);
                }
                if scenario == "foreign-file" {
                    assert_eq!(std::fs::metadata(target.join("foreign")).unwrap().uid(), 1);
                }
            }
            settled(&context, None).await;
        });
    });
}

#[test]
fn identity_fixture_proves_distinct_authorities() {
    if std::env::var("FS_WORKLOAD_PROBE").as_deref() == Ok("1") {
        workload_probe();
        return;
    }
    isolated("identity_fixture_proves_distinct_authorities", || {
        let context = Context::new();
        assert!(std::fs::read(&context.sentinel).unwrap() == context.sentinel_bytes);
        context.prove_workload();
        runtime(async {
            let mut denied_client = connect(&context.agent);
            let denied_request = Frame {
                id: "denied-fixture".into(),
                method: "fs_read".into(),
                ..Default::default()
            }
            .reply(json!({"path": context.sentinel}), vec![], true);
            write_frame(&mut denied_client, &denied_request)
                .await
                .unwrap();
            let (denied_bytes, denied_error) = receive(&mut denied_client).await;
            assert!(denied_bytes.is_empty());
            assert!(denied_error.contains("Permission denied"), "{denied_error}");
            let mut client = start(&context).await;
            let (bytes, error) = receive(&mut client).await;
            assert!(error.is_empty(), "{error}");
            no_sentinel(&context, &bytes);
            let entries = contents(&bytes);
            assert!(
                entries
                    .iter()
                    .any(|(path, data)| path == Path::new("allowed")
                        && data == b"authorized fixture bytes")
            );
            assert!(!entries.iter().any(|(path, _)| path.starts_with("ledger")));
        });
    });
}

#[test]
fn tree_pull_authority_stays_on_created_descriptor() {
    isolated("tree_pull_authority_stays_on_created_descriptor", || {
        let context = Context::new();
        context.prove_workload();
        runtime(async {
            for replacement in ["sentinel-symlink", "fifo", "regular"] {
                let mut fixture = fixtures::register(context.source.clone(), (1, 1)).unwrap();
                let mut client = start(&context).await;
                let snapshot = tokio::time::timeout(DEADLINE, fixture.reached())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    snapshot.links, 0,
                    "FD invariant RED: completed archive still has a name before daemon streaming"
                );
                assert_eq!(
                    (snapshot.uid, snapshot.gid),
                    (1, 1),
                    "archive was not created under workload identity"
                );
                assert!(!snapshot.path.exists());
                match replacement {
                    "sentinel-symlink" => symlink(&context.sentinel, &snapshot.path).unwrap(),
                    "fifo" => {
                        let name = CString::new(snapshot.path.as_os_str().as_bytes()).unwrap();
                        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
                    }
                    _ => std::fs::write(&snapshot.path, b"replacement fixture bytes").unwrap(),
                }
                let replacement_metadata = std::fs::symlink_metadata(&snapshot.path).unwrap();
                assert_ne!(
                    (replacement_metadata.dev(), replacement_metadata.ino()),
                    (snapshot.device, snapshot.inode)
                );
                fixture.release();
                let (bytes, error) = receive(&mut client).await;
                assert!(error.is_empty(), "{error}");
                no_sentinel(&context, &bytes);
                assert!(
                    contents(&bytes)
                        .iter()
                        .any(|(path, data)| path == Path::new("allowed")
                            && data == b"authorized fixture bytes")
                );
                settled(&context, Some(&snapshot)).await;
                std::fs::remove_file(&snapshot.path).unwrap();
                assert_eq!(
                    std::fs::read_dir(fixture.scratch.path()).unwrap().count(),
                    0
                );
                println!(
                    "descriptor invariant PASS: {replacement}, original inode retained, output authorized, no block, FD closed"
                );
            }
        });
    });
}

#[test]
fn tree_pull_descriptor_cleanup_and_cancellation() {
    isolated("tree_pull_descriptor_cleanup_and_cancellation", || {
        let context = Context::new();
        context.prove_workload();
        runtime(async {
            for large in [false, true] {
                std::fs::remove_file(context.source.join("allowed")).ok();
                let data = if large {
                    vec![b'x'; CHUNK * 129 + 7]
                } else {
                    Vec::new()
                };
                if large {
                    std::fs::write(context.source.join("large"), &data).unwrap();
                    owner(&context.source.join("large"), 1, 1, 0o600);
                }
                let mut fixture = fixtures::register(context.source.clone(), (1, 1)).unwrap();
                let mut client = start(&context).await;
                let snapshot = fixture.reached().await.unwrap();
                assert_eq!(snapshot.links, 0);
                fixture.release();
                let (bytes, error) = receive(&mut client).await;
                assert!(error.is_empty(), "{error}");
                no_sentinel(&context, &bytes);
                let entries = contents(&bytes);
                assert!(!entries.iter().any(|(path, _)| path.starts_with("ledger")));
                if large {
                    assert!(bytes.len() > CHUNK);
                    assert!(
                        entries
                            .iter()
                            .any(|(path, actual)| path == Path::new("large") && actual == &data),
                        "archive offset or content corrupted"
                    );
                } else {
                    assert_eq!(entries.len(), 1);
                    assert_eq!(entries[0].0, Path::new("."));
                }
                settled(&context, Some(&snapshot)).await;
                assert_eq!(
                    std::fs::read_dir(fixture.scratch.path()).unwrap().count(),
                    0
                );
            }
            for disconnect_after_frame in [false, true] {
                let mut fixture = fixtures::register(context.source.clone(), (1, 1)).unwrap();
                let mut client = start(&context).await;
                let snapshot = fixture.reached().await.unwrap();
                if !disconnect_after_frame {
                    drop(client);
                    fixture.release();
                } else {
                    fixture.release();
                    let frame = read_frame(&mut client).await.unwrap().unwrap();
                    assert!(!frame.data.is_empty());
                    drop(client);
                }
                settled(&context, Some(&snapshot)).await;
                assert_eq!(
                    std::fs::read_dir(fixture.scratch.path()).unwrap().count(),
                    0
                );
            }
            let fifo = context.source.join("unsupported-fifo");
            let name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
            assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
            let fixture = fixtures::register(context.source.clone(), (1, 1)).unwrap();
            let mut client = start(&context).await;
            let (bytes, error) = receive(&mut client).await;
            assert!(bytes.is_empty());
            assert!(error.contains("unsupported special file"), "{error}");
            settled(&context, None).await;
            assert_eq!(
                std::fs::read_dir(fixture.scratch.path()).unwrap().count(),
                0
            );
            println!(
                "cleanup PASS: empty tree, offset rewind, >CHUNK archive, private ledger excluded, disconnect before/during streaming, archive error"
            );
        });
    });
}
