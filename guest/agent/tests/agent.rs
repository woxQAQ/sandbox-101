use sandbox_agent::{
    Agent, Config,
    server::{Portal, serve_connection},
    wire::{Frame, read_frame, write_frame},
};
use serde_json::{Value, json};
use std::{
    ffi::CString,
    os::unix::{ffi::OsStrExt, fs::FileTypeExt},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

fn setup() -> (tempfile::TempDir, Arc<Agent>) {
    let tmp = tempfile::tempdir().unwrap();
    let agent = Agent::new(Config {
        env_file: tmp.path().join("sandbox.env"),
        machine_id: tmp.path().join("machine-id"),
        ledger_dir: tmp.path().join("ledger"),
        ledger_max_entries: sandbox_agent::ledger::MAX_ENTRIES,
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
    })
    .unwrap();
    (tmp, agent)
}
fn request(id: &str, method: &str, mut params: Value) -> Frame {
    if matches!(method, "exec" | "pty_open" | "session_create") {
        if params.get("env").is_none() {
            params["env"] = json!({});
        }
        params["env"]["PATH"] = json!(std::env::var("PATH").unwrap());
    }
    let base = Frame {
        id: id.into(),
        method: method.into(),
        ..Default::default()
    };
    base.reply(params, vec![], true)
}
fn connection(agent: Arc<Agent>, portal: Portal) -> UnixStream {
    let (a, b) = UnixStream::pair().unwrap();
    tokio::spawn(async move {
        let _ = serve_connection(agent, b, portal).await;
    });
    a
}
// Variant whose server task can be joined, so a test can prove the server
// observed a disconnect instead of inferring it from a sleep.
fn connection_joinable(
    agent: Arc<Agent>,
    portal: Portal,
) -> (UnixStream, tokio::task::JoinHandle<()>) {
    let (a, b) = UnixStream::pair().unwrap();
    let server = tokio::spawn(async move {
        let _ = serve_connection(agent, b, portal).await;
    });
    (a, server)
}
async fn next(s: &mut UnixStream) -> Frame {
    tokio::time::timeout(Duration::from_secs(10), read_frame(s))
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn rpc(s: &mut UnixStream, f: Frame) -> Vec<Frame> {
    let id = f.id.clone();
    write_frame(s, &f).await.unwrap();
    let mut frames = Vec::new();
    loop {
        let f = next(s).await;
        assert_eq!(f.id, id);
        let done = f.eof;
        if !f.error.is_empty() {
            assert!(f.eof);
            assert!(f.params_json()["error_code"].is_string(), "{f:?}");
            assert!(f.error.contains("guest/agent/src/"), "{f:?}");
        }
        frames.push(f);
        if done {
            return frames;
        }
    }
}
fn output(frames: &[Frame]) -> String {
    String::from_utf8(frames.iter().flat_map(|f| f.data.clone()).collect()).unwrap()
}
fn success(frames: &[Frame]) {
    assert!(
        frames.last().unwrap().error.is_empty(),
        "{:?}",
        frames.last()
    );
}

#[tokio::test]
async fn portal_separation_activation_and_multiplex() {
    let (_tmp, agent) = setup();
    let mut workload = connection(agent.clone(), Portal::Workload);
    for method in ["reseed", "clock_sync", "ping", "activate", "quiesce"] {
        let frames = rpc(&mut workload, request("deny", method, json!({}))).await;
        assert_eq!(
            frames.last().unwrap().params_json()["error_code"],
            "unauthorized"
        );
    }
    let mut system = connection(agent.clone(), Portal::System);
    let frames = rpc(&mut system, request("ping", "ping", json!({}))).await;
    success(&frames);
    let denied = rpc(
        &mut system,
        request("deny", "exec", json!({"command":"true"})),
    )
    .await;
    assert!(!denied.last().unwrap().error.is_empty());
    let mut admin = connection(agent.clone(), Portal::Admin);
    let frames=rpc(&mut admin,request("activate","activate",json!({"sandbox_id":"S1","hostname":"ignored-on-resume","network":"relay","reset_identity":false,"env":{"X":"claimed"}}))).await;
    success(&frames);
    assert_eq!(agent.environment().await.unwrap()["X"], "claimed");
    assert_eq!(
        agent.environment().await.unwrap()["http_proxy"],
        "http://127.0.0.1:3128"
    );
    assert!(!agent.config.machine_id.exists());
    write_frame(
        &mut workload,
        &request("one", "exec", json!({"command":"sleep 0.05; printf one"})),
    )
    .await
    .unwrap();
    write_frame(&mut workload, &request("two", "info", json!({})))
        .await
        .unwrap();
    let mut complete = std::collections::BTreeSet::new();
    while complete.len() < 2 {
        let f = next(&mut workload).await;
        assert!(f.error.is_empty(), "{}", f.error);
        if f.eof {
            complete.insert(f.id);
        }
    }
    let frames = rpc(
        &mut admin,
        request(
            "activate",
            "activate",
            json!({"sandbox_id":"S2","network":"none","env":{}}),
        ),
    )
    .await;
    success(&frames);
    assert!(agent.environment().await.unwrap().is_empty());
}

#[tokio::test]
async fn structured_failures_cover_files_processes_sessions_and_admission() {
    let (tmp, agent) = setup();
    let mut workload = connection(agent.clone(), Portal::Workload);
    let missing = tmp.path().join("missing");
    let binary = tmp.path().join("binary");
    std::fs::write(&binary, [0xff]).unwrap();
    for (method, params, code, target, origin) in [
        (
            "fs_read",
            json!({"path":missing}),
            "not_found",
            missing.to_str().unwrap(),
            "fs.rs:",
        ),
        (
            "fs_stat",
            json!({"path":binary.join("child")}),
            "conflict",
            "child",
            "fs.rs:",
        ),
        (
            "fs_read",
            json!({"path":"relative"}),
            "invalid",
            "relative",
            "fs.rs:",
        ),
        (
            "fs_replace",
            json!({"path":binary,"find":"x","replace":"y"}),
            "invalid",
            "binary",
            "fs.rs:",
        ),
        (
            "proc_kill",
            json!({"process_id":"absent-process"}),
            "not_found",
            "absent-process",
            "process.rs:",
        ),
        (
            "session_rm",
            json!({"session_id":"absent-session"}),
            "not_found",
            "absent-session",
            "session.rs:",
        ),
        (
            "fs_read",
            json!({"path":agent.ledger.directory.join("exec.sqlite3")}),
            "unauthorized",
            "exec.sqlite3",
            "fs.rs:",
        ),
        (
            "exec",
            json!({"command":"","env":{"PRIVATE_VALUE":"not-for-diagnostics"}}),
            "invalid",
            "guest exec",
            "process.rs:",
        ),
    ] {
        let frames = rpc(&mut workload, request("failure", method, params)).await;
        let terminal = frames.last().unwrap();
        assert_eq!(terminal.params_json()["error_code"], code, "{terminal:?}");
        assert!(terminal.error.contains(target), "{terminal:?}");
        assert!(terminal.error.contains(origin), "{terminal:?}");
        assert!(!terminal.error.contains("not-for-diagnostics"));
    }
    let permit = agent
        .requests
        .clone()
        .acquire_many_owned(128)
        .await
        .unwrap();
    let frames = rpc(&mut workload, request("capacity", "info", json!({}))).await;
    assert_eq!(
        frames.last().unwrap().params_json()["error_code"],
        "resource_exhausted"
    );
    drop(permit);
    let mut admin = connection(agent.clone(), Portal::Admin);
    success(&rpc(&mut admin, request("gate", "quiesce", json!({}))).await);
    let frames = rpc(
        &mut workload,
        request("gated", "fs_stat", json!({"path":binary})),
    )
    .await;
    assert_eq!(
        frames.last().unwrap().params_json()["error_code"],
        "quiesced"
    );
    let mut system = connection(agent.clone(), Portal::System);
    let frames = rpc(
        &mut system,
        request(
            "clock",
            "clock_sync",
            json!({"unix_seconds":1,"nanoseconds":1_000_000_000}),
        ),
    )
    .await;
    assert_eq!(
        frames.last().unwrap().params_json()["error_code"],
        "invalid"
    );
    assert!(frames.last().unwrap().error.contains("system.rs:"));
}

#[tokio::test]
async fn exec_environment_streaming_and_disconnect_cleanup() {
    let (_tmp, agent) = setup();
    let mut stream = connection(agent.clone(), Portal::Workload);
    let frames=rpc(&mut stream,request("exec","exec",json!({"command":"printf '%s' \"$X\"; printf err >&2; head -c 1048576 /dev/zero","env":{"X":"override"}}))).await;
    success(&frames);
    assert!(
        frames.first().unwrap().params_json()["started"]
            .as_bool()
            .unwrap()
    );
    assert!(frames.iter().any(|f| f.params_json()["stream"] == "stderr"));
    assert_eq!(
        frames.iter().map(|f| f.data.len()).sum::<usize>(),
        8 + 3 + 1048576
    );
    let f = request("lost", "exec", json!({"command":"sleep 300"}));
    write_frame(&mut stream, &f).await.unwrap();
    let started = next(&mut stream).await;
    let id = started.params_json()["process_id"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(stream);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let p = agent.processes.lock().await[&id].clone();
            if p.status.lock().unwrap().exit.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn detached_ring_and_persistent_shell() {
    let (_tmp, agent) = setup();
    let mut s = connection(agent.clone(), Portal::Workload);
    let frames = rpc(
        &mut s,
        request(
            "bg",
            "exec",
            json!({"command":"head -c 2097152 /dev/zero","detach":true}),
        ),
    )
    .await;
    success(&frames);
    let id = frames.last().unwrap().params_json()["process_id"]
        .as_str()
        .unwrap()
        .to_owned();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if agent.processes.lock().await[&id]
                .status
                .lock()
                .unwrap()
                .exit
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let logs = rpc(
        &mut s,
        request("logs", "proc_logs", json!({"process_id":id})),
    )
    .await;
    success(&logs);
    assert!(logs.iter().map(|f| f.data.len()).sum::<usize>() <= 1024 * 1024);
    assert_eq!(logs[0].params_json()["truncated"], true);
    let created = rpc(&mut s, request("sc", "session_create", json!({}))).await;
    success(&created);
    let id = created.last().unwrap().params_json()["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let a = rpc(
        &mut s,
        request(
            "sa",
            "exec",
            json!({"session_id":id,"command":"cd /tmp; SECRET_STATE=kept"}),
        ),
    )
    .await;
    success(&a);
    let b = rpc(
        &mut s,
        request(
            "sb",
            "exec",
            json!({"session_id":id,"command":"printf '%s:%s' \"$PWD\" \"$SECRET_STATE\""}),
        ),
    )
    .await;
    success(&b);
    assert_eq!(output(&b), "/tmp:kept");
    let removed = rpc(
        &mut s,
        request("sr", "session_rm", json!({"session_id":id})),
    )
    .await;
    success(&removed);
}

#[tokio::test]
async fn files_rollback_and_watch_ready() {
    let (tmp, agent) = setup();
    let target = tmp.path().join("data");
    std::fs::write(&target, b"original").unwrap();
    let mut stream = connection(agent.clone(), Portal::Workload);
    let mut first = request("write", "fs_write", json!({"path":target}));
    first.data = b"partial".to_vec();
    first.eof = false;
    write_frame(&mut stream, &first).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(stream);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(std::fs::read(&target).unwrap(), b"original");
    let mut stream = connection(agent.clone(), Portal::Workload);
    first.eof = true;
    first.id = "full".into();
    success(&rpc(&mut stream, first).await);
    assert_eq!(std::fs::read(&target).unwrap(), b"partial");
    let frames = rpc(
        &mut stream,
        request("read", "fs_read", json!({"path":target})),
    )
    .await;
    success(&frames);
    assert_eq!(output(&frames), "partial");
    let mut watcher = connection(agent, Portal::Workload);
    write_frame(
        &mut watcher,
        &request("watch", "fs_watch", json!({"path":tmp.path()})),
    )
    .await
    .unwrap();
    assert_eq!(next(&mut watcher).await.params_json()["ready"], true);
    std::fs::write(tmp.path().join("changed"), b"x").unwrap();
    let event = next(&mut watcher).await;
    assert_eq!(event.params_json()["name"], "changed");
}

async fn rpc_timed(s: &mut UnixStream, f: Frame, timeout: Duration) -> Option<Vec<Frame>> {
    let id = f.id.clone();
    write_frame(s, &f).await.unwrap();
    let mut frames = Vec::new();
    tokio::time::timeout(timeout, async {
        loop {
            let f = read_frame(s).await.unwrap().unwrap();
            assert_eq!(f.id, id);
            let done = f.eof;
            frames.push(f);
            if done {
                break;
            }
        }
    })
    .await
    .ok()?;
    Some(frames)
}

#[tokio::test]
async fn filesystem_fifo_rejection_releases_rpc_slots() {
    let (tmp, agent) = setup();
    let fifo = tmp.path().join("fifo");
    let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);

    // Collect rejection outcomes first; assertions happen only after cleanup so
    // a blocking implementation cannot hang this test command.
    let mut outcomes = Vec::new();
    let mut reader = connection(agent.clone(), Portal::Workload);
    outcomes.push((
        "fs_read",
        rpc_timed(
            &mut reader,
            request("fifo-read", "fs_read", json!({"path":fifo})),
            Duration::from_secs(5),
        )
        .await,
    ));
    let mut replacer = connection(agent.clone(), Portal::Workload);
    outcomes.push((
        "fs_replace",
        rpc_timed(
            &mut replacer,
            request(
                "fifo-replace",
                "fs_replace",
                json!({"path":fifo,"find":"a","replace":"b"}),
            ),
            Duration::from_secs(5),
        )
        .await,
    ));
    // Recovery for a blocking implementation: a rescue writer FD owned by this
    // test unblocks open() calls stuck on the writer-less FIFO. This exists
    // only to finish RED runs cleanly and is not part of the product fix.
    let rescue = unsafe { libc::open(fifo_c.as_ptr(), libc::O_RDWR | libc::O_NONBLOCK) };
    assert!(
        rescue >= 0,
        "rescue open: {}",
        std::io::Error::last_os_error()
    );
    let recovered = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if agent.requests.available_permits() == 128 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(unsafe { libc::close(rescue) }, 0);
    recovered.expect("FIFO handlers did not release workload RPC permits");

    // Assertions run after cleanup so a blocking implementation fails here
    // (RED) instead of hanging the command. Nothing below may introduce a new
    // blocked FIFO open against an old implementation.
    for (name, outcome) in outcomes {
        let frames =
            outcome.unwrap_or_else(|| panic!("{name} on a FIFO hung instead of being rejected"));
        let last = frames.last().unwrap();
        assert!(!last.error.is_empty(), "{name} on a FIFO must be rejected");
        assert!(
            last.error.contains("regular"),
            "{name} rejection must name the regular-file requirement: {}",
            last.error
        );
    }

    // More sequential FIFO rejections than the global slot capacity prove
    // every rejected admission released its permit: a single leak per request
    // would surface as "request capacity exhausted" within 128 iterations,
    // which cannot satisfy the regular-file assertion.
    let mut stream = connection(agent.clone(), Portal::Workload);
    for i in 0..130u32 {
        let (method, params) = if i % 2 == 0 {
            ("fs_read", json!({"path":fifo}))
        } else {
            ("fs_replace", json!({"path":fifo,"find":"a","replace":"b"}))
        };
        let frames = rpc_timed(
            &mut stream,
            request(&format!("fifo-{i}"), method, params),
            Duration::from_secs(5),
        )
        .await
        .unwrap_or_else(|| panic!("{method} FIFO rejection #{i} hung"));
        let last = frames.last().unwrap();
        assert!(
            last.error.contains("regular"),
            "#{i} {method} must fail as non-regular, not as capacity exhaustion: {}",
            last.error
        );
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if agent.requests.available_permits() == 128 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("FIFO rejection stress leaked workload RPC permits");

    // Dropped-connection case, kept separate from the rescue phase above so
    // the rescue FD cannot mask whether a dropped connection releases its
    // slot. Joining the server task proves the disconnect was delivered (the
    // handler itself may outlive the connection); the bounded permit poll
    // then proves the slot came back instead of relying on a sleep snapshot.
    let (mut lost, lost_server) = connection_joinable(agent.clone(), Portal::Workload);
    write_frame(&mut lost, &request("lost", "fs_read", json!({"path":fifo})))
        .await
        .unwrap();
    drop(lost);
    tokio::time::timeout(Duration::from_secs(5), lost_server)
        .await
        .expect("server did not observe the disconnect")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if agent.requests.available_permits() == 128 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnected FIFO request held a workload RPC permit");

    // Concurrent ordinary requests still complete normally.
    let mut a = connection(agent.clone(), Portal::Workload);
    let mut b = connection(agent.clone(), Portal::Workload);
    let (ra, rb) = tokio::join!(
        rpc(&mut a, request("info-a", "info", json!({}))),
        rpc(&mut b, request("info-b", "info", json!({}))),
    );
    success(&ra);
    success(&rb);

    // Rejection must not rewrite the FIFO or turn it into a regular file.
    assert!(
        std::fs::symlink_metadata(&fifo)
            .unwrap()
            .file_type()
            .is_fifo()
    );

    // Ordinary file behavior is preserved: full read, offset/length and EOF.
    let data = tmp.path().join("data");
    std::fs::write(&data, b"hello world").unwrap();
    let frames = rpc(
        &mut stream,
        request("full", "fs_read", json!({"path":data})),
    )
    .await;
    success(&frames);
    assert_eq!(output(&frames), "hello world");
    let frames = rpc(
        &mut stream,
        request("offset", "fs_read", json!({"path":data,"offset":6})),
    )
    .await;
    success(&frames);
    assert_eq!(output(&frames), "world");
    let frames = rpc(
        &mut stream,
        request(
            "window",
            "fs_read",
            json!({"path":data,"offset":6,"length":2}),
        ),
    )
    .await;
    success(&frames);
    assert_eq!(output(&frames), "wo");
    let frames = rpc(
        &mut stream,
        request("eof", "fs_read", json!({"path":data,"offset":11})),
    )
    .await;
    success(&frames);
    assert_eq!(output(&frames), "");
    assert_eq!(frames.last().unwrap().params_json()["bytes"], "0");

    // fs_read still follows a symlink to a regular file.
    let link = tmp.path().join("link");
    std::os::unix::fs::symlink(&data, &link).unwrap();
    let frames = rpc(
        &mut stream,
        request("link-read", "fs_read", json!({"path":link})),
    )
    .await;
    success(&frames);
    assert_eq!(output(&frames), "hello world");

    // fs_replace keeps rejecting symlinks and must not rewrite the target.
    let frames = rpc(
        &mut stream,
        request(
            "link-replace",
            "fs_replace",
            json!({"path":link,"find":"hello","replace":"goodbye"}),
        ),
    )
    .await;
    assert!(!frames.last().unwrap().error.is_empty());
    assert_eq!(std::fs::read(&data).unwrap(), b"hello world");

    // A successful fs_replace on a regular file still works with O_NONBLOCK.
    let frames = rpc(
        &mut stream,
        request(
            "replace",
            "fs_replace",
            json!({"path":data,"find":"world","replace":"there"}),
        ),
    )
    .await;
    success(&frames);
    assert_eq!(std::fs::read(&data).unwrap(), b"hello there");

    // Directories are rejected promptly as non-regular files.
    let frames = rpc(
        &mut stream,
        request("dir", "fs_read", json!({"path":tmp.path()})),
    )
    .await;
    let last = frames.last().unwrap();
    assert!(!last.error.is_empty());
    assert!(last.error.contains("regular"), "{}", last.error);
}

#[tokio::test]
async fn raw_port_forward_supports_half_close() {
    let (_tmp, agent) = setup();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut data = Vec::new();
        s.read_to_end(&mut data).await.unwrap();
        assert_eq!(data, b"request");
        s.write_all(b"response-after-fin").await.unwrap();
        s.shutdown().await.unwrap();
    });
    let mut stream = connection(agent, Portal::Workload);
    write_frame(
        &mut stream,
        &request("p", "port_forward", json!({"port":port})),
    )
    .await
    .unwrap();
    assert_eq!(next(&mut stream).await.params_json()["ready"], true);
    stream.write_all(b"request").await.unwrap();
    stream.shutdown().await.unwrap();
    let mut data = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut data))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data, b"response-after-fin");
    server.await.unwrap();
}

#[tokio::test]
async fn pty_is_real_terminal_and_resize() {
    let (_tmp, agent) = setup();
    let mut s = connection(agent, Portal::Workload);
    write_frame(
        &mut s,
        &request(
            "pty",
            "pty_open",
            json!({"command":"test -t 0 && printf terminal; sleep 0.1","rows":24,"cols":80}),
        ),
    )
    .await
    .unwrap();
    let first = next(&mut s).await;
    assert!(first.error.is_empty(), "{}", first.error);
    let id = first.params_json()["process_id"]
        .as_str()
        .unwrap()
        .to_owned();
    write_frame(
        &mut s,
        &request(
            "resize",
            "pty_resize",
            json!({"process_id":id,"rows":30,"cols":100}),
        ),
    )
    .await
    .unwrap();
    let mut terminal = Vec::new();
    let mut complete = std::collections::BTreeSet::new();
    while complete.len() < 2 {
        let f = next(&mut s).await;
        assert!(f.error.is_empty(), "{}", f.error);
        if f.id == "pty" {
            terminal.extend(f.data);
        }
        if f.eof {
            complete.insert(f.id);
        }
    }
    assert!(String::from_utf8(terminal).unwrap().contains("terminal"));
}
