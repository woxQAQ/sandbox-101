use sandbox_agent::{
    Agent, Config,
    server::{Portal, serve_connection},
    wire::{Frame, read_frame, write_frame},
};
use serde_json::{Value, json};
use std::{os::unix::fs::MetadataExt, path::Path, sync::Arc, time::Duration};
use tokio::net::UnixStream;

fn config(root: &Path) -> Config {
    Config {
        env_file: root.join("sandbox.env"),
        machine_id: root.join("machine-id"),
        ledger_dir: root.join("ledger"),
        ledger_max_entries: 1024,
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
    }
}
fn request(id: &str, method: &str, mut params: Value) -> Frame {
    if matches!(method, "exec" | "pty_open") {
        if params.get("env").is_none() {
            params["env"] = json!({});
        }
        params["env"]["PATH"] = json!(std::env::var("PATH").unwrap());
    }
    Frame {
        id: id.into(),
        method: method.into(),
        ..Default::default()
    }
    .reply(params, vec![], true)
}
fn exec(id: &str, key: &str, command: &str) -> Frame {
    request(id, "exec", json!({"command":command,"exec_key":key}))
}
fn connect(agent: &Arc<Agent>, portal: Portal) -> UnixStream {
    let (a, b) = UnixStream::pair().unwrap();
    let agent = agent.clone();
    tokio::spawn(async move {
        let _ = serve_connection(agent, b, portal).await;
    });
    a
}
async fn next(stream: &mut UnixStream) -> Frame {
    tokio::time::timeout(Duration::from_secs(10), read_frame(stream))
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn rpc(stream: &mut UnixStream, frame: Frame) -> Vec<Frame> {
    write_frame(stream, &frame).await.unwrap();
    let mut frames = Vec::new();
    loop {
        let frame = next(stream).await;
        let eof = frame.eof;
        frames.push(frame);
        if eof {
            return frames;
        }
    }
}
fn success(frames: &[Frame]) {
    assert!(
        frames.last().unwrap().error.is_empty(),
        "{:?}",
        frames.last()
    );
}
fn bytes(frames: &[Frame], stream: &str) -> Vec<u8> {
    frames
        .iter()
        .filter(|f| f.params_json()["stream"] == stream)
        .flat_map(|f| f.data.clone())
        .collect()
}
async fn activate(agent: &Arc<Agent>, id: &str, env: Value) -> Vec<Frame> {
    rpc(
        &mut connect(agent, Portal::Admin),
        request(
            "activate",
            "activate",
            json!({"sandbox_id":id,"network":"none","env":env}),
        ),
    )
    .await
}
async fn wait_idle(agent: &Arc<Agent>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while agent.requests.available_permits() != 128 || Arc::strong_count(agent) != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn saved_result_survives_restart_and_hashes_all_inputs() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = config(tmp.path());
    let agent = Agent::new(cfg.clone()).unwrap();
    success(&activate(&agent, "sandbox-a", json!({"BASE":"ambient-secret"})).await);
    let command = format!(
        "cat; printf err >&2; printf x >> {}/effect",
        tmp.path().display()
    );
    let mut frame = exec("initial", "a-private-key", &command);
    frame.data = b"input\0bytes".to_vec();
    let mut stream = connect(&agent, Portal::Product);
    let first = rpc(&mut stream, frame.clone()).await;
    success(&first);
    assert_eq!(bytes(&first, "stdout"), frame.data);
    assert_eq!(bytes(&first, "stderr"), b"err");
    for field in ["command", "argv", "env", "cwd", "extra"] {
        let mut p = frame.params_json();
        p[field] = match field {
            "argv" => json!(["different"]),
            "env" => json!({"SECRET":"changed"}),
            "cwd" => json!(tmp.path()),
            _ => json!("changed"),
        };
        let changed = frame.reply(p, frame.data.clone(), true);
        let result = rpc(&mut stream, changed).await;
        assert_eq!(result.last().unwrap().error, "exec_key_conflict", "{field}");
    }
    let mut changed = frame.clone();
    changed.data.push(1);
    assert_eq!(
        rpc(&mut stream, changed).await.last().unwrap().error,
        "exec_key_conflict"
    );
    drop(stream);
    wait_idle(&agent).await;
    drop(agent);
    let agent = Agent::new(cfg).unwrap();
    assert_eq!(agent.activation.lock().await.sandbox_id, "sandbox-a");
    success(&activate(&agent, "sandbox-a", json!({"BASE":"ambient-secret"})).await);
    frame.id = "different-connection-id".into();
    let replay = rpc(&mut connect(&agent, Portal::Product), frame.clone()).await;
    success(&replay);
    assert_eq!(replay.last().unwrap().params_json()["replayed"], true);
    assert_eq!(bytes(&replay, "stdout"), frame.data);
    assert_eq!(std::fs::read(tmp.path().join("effect")).unwrap(), b"x");
    success(&activate(&agent, "sandbox-a", json!({"BASE":"changed-secret"})).await);
    assert_eq!(
        rpc(&mut connect(&agent, Portal::Product), frame)
            .await
            .last()
            .unwrap()
            .error,
        "exec_key_conflict"
    );
    let db = std::fs::read(agent.ledger.directory.join("exec.sqlite3")).unwrap();
    for secret in [
        "a-private-key",
        "ambient-secret",
        "changed-secret",
        &command,
    ] {
        assert!(!db.windows(secret.len()).any(|w| w == secret.as_bytes()));
    }
    assert_eq!(
        std::fs::metadata(&agent.ledger.directory).unwrap().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(agent.ledger.directory.join("exec.sqlite3"))
            .unwrap()
            .mode()
            & 0o777,
        0o600
    );
}

#[tokio::test]
async fn concurrent_key_is_admitted_once_and_identity_cannot_change_while_running() {
    let tmp = tempfile::tempdir().unwrap();
    let agent = Agent::new(config(tmp.path())).unwrap();
    success(&activate(&agent, "a", json!({})).await);
    let command = format!(
        "printf x >> {}/effect; sleep 0.25; printf done",
        tmp.path().display()
    );
    let mut first = connect(&agent, Portal::Product);
    write_frame(&mut first, &exec("same-id", "operation-step", &command))
        .await
        .unwrap();
    assert_eq!(next(&mut first).await.params_json()["started"], true);
    let duplicate = rpc(
        &mut connect(&agent, Portal::Product),
        exec("same-id", "operation-step", &command),
    )
    .await;
    assert_eq!(
        duplicate.last().unwrap().params_json()["result_state"],
        "in_progress"
    );
    let changed = activate(&agent, "b", json!({})).await;
    assert!(changed.last().unwrap().error.contains("still in progress"));
    loop {
        if next(&mut first).await.eof {
            break;
        }
    }
    let replay = rpc(
        &mut connect(&agent, Portal::Product),
        exec("other-id", "operation-step", &command),
    )
    .await;
    success(&replay);
    assert_eq!(replay.last().unwrap().params_json()["replayed"], true);
    assert_eq!(std::fs::read(tmp.path().join("effect")).unwrap(), b"x");
    // Same identity activation retains history; a fork's new identity clears it.
    success(&activate(&agent, "a", json!({})).await);
    success(&activate(&agent, "b", json!({})).await);
    let new_scope = rpc(
        &mut connect(&agent, Portal::Product),
        exec("new", "operation-step", &command),
    )
    .await;
    success(&new_scope);
    assert_eq!(new_scope.last().unwrap().params_json()["replayed"], false);
    assert_eq!(std::fs::read(tmp.path().join("effect")).unwrap(), b"xx");
}

#[tokio::test]
async fn output_and_admission_bounds_do_not_evict_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = config(tmp.path());
    cfg.ledger_max_entries = 2;
    let agent = Agent::new(cfg).unwrap();
    let mut stream = connect(&agent, Portal::Product);
    let f = exec(
        "big",
        "one",
        "head -c 131072 /dev/zero; head -c 131073 /dev/zero >&2",
    );
    let first = rpc(&mut stream, f.clone()).await;
    success(&first);
    for name in ["stdout", "stderr"] {
        assert_eq!(bytes(&first, name).len(), 65536);
    }
    let metadata = first.last().unwrap().params_json();
    assert_eq!(metadata["stdout_truncated"], true);
    assert_eq!(metadata["stderr_truncated"], true);
    assert_eq!(metadata["stdout_bytes"], "131072");
    let replay = rpc(&mut stream, f.clone()).await;
    assert_eq!(bytes(&replay, "stdout"), bytes(&first, "stdout"));
    assert_eq!(bytes(&replay, "stderr"), bytes(&first, "stderr"));
    success(&rpc(&mut stream, exec("two", "two", "true")).await);
    assert_eq!(
        rpc(&mut stream, exec("three", "three", "true"))
            .await
            .last()
            .unwrap()
            .error,
        "exec_ledger_capacity_exhausted"
    );
    assert_eq!(
        rpc(&mut stream, f).await.last().unwrap().params_json()["replayed"],
        true
    );
    // An unkeyed request keeps its original streaming behavior and is not deduplicated by frame.id.
    for _ in 0..2 {
        let f = request(
            "same-frame-id",
            "exec",
            json!({"command":"head -c 131072 /dev/zero"}),
        );
        let result = rpc(&mut stream, f).await;
        success(&result);
        assert_eq!(bytes(&result, "stdout").len(), 131072);
        assert!(
            result
                .last()
                .unwrap()
                .params_json()
                .get("replayed")
                .is_none()
        );
    }
}

#[tokio::test]
async fn unsupported_keyed_modes_and_private_files_are_rejected_before_side_effects() {
    let tmp = tempfile::tempdir().unwrap();
    let agent = Agent::new(config(tmp.path())).unwrap();
    let mut stream = connect(&agent, Portal::Product);
    let effect = tmp.path().join("should-not-exist");
    let command = format!("touch {}", effect.display());
    let initial = exec("initial", "key", &command);
    let mut streaming = initial.clone();
    streaming.eof = false;
    assert!(
        rpc(&mut stream, streaming)
            .await
            .last()
            .unwrap()
            .error
            .contains("complete stdin")
    );
    for (method, extra) in [
        ("exec", json!({"detach":true})),
        ("exec", json!({"session_id":"absent"})),
        ("pty_open", json!({})),
        ("session_create", json!({})),
    ] {
        let mut p = initial.params_json();
        p.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert!(
            !rpc(&mut stream, request("reject", method, p))
                .await
                .last()
                .unwrap()
                .error
                .is_empty()
        );
    }
    assert!(!effect.exists());
    let db = agent.ledger.directory.join("exec.sqlite3");
    let alias = tmp.path().join("alias");
    std::os::unix::fs::symlink(&db, &alias).unwrap();
    for path in [&db, &alias] {
        for method in ["fs_read", "fs_stat", "fs_rm"] {
            let result = rpc(
                &mut stream,
                request("private", method, json!({"path":path})),
            )
            .await;
            assert!(result.last().unwrap().error.contains("private state"));
        }
    }
    let listing = rpc(
        &mut stream,
        request("list", "fs_list", json!({"path":tmp.path()})),
    )
    .await;
    success(&listing);
    let listing = listing.last().unwrap().params_json().to_string();
    assert!(!listing.contains("ledger") && !listing.contains("alias"));
    let find = rpc(
        &mut stream,
        request("find", "fs_find", json!({"path":tmp.path()})),
    )
    .await;
    success(&find);
    assert!(!find.iter().any(|f| {
        f.params_json()["path"]
            .as_str()
            .is_some_and(|p| p.contains("ledger"))
    }));
    let rename = rpc(
        &mut stream,
        request(
            "rename",
            "fs_rename",
            json!({"path":effect,"destination":db}),
        ),
    )
    .await;
    assert!(rename.last().unwrap().error.contains("private state"));
}

fn crash_request(root: &Path) -> Frame {
    request(
        "exec",
        "exec",
        json!({"exec_key":"crash-step","command":"/bin/sh",
        "argv":["-c","printf x >> \"$1\"; sleep 300","sh",root.join("effect")]}),
    )
}

// Runs in a separate OS process so SIGKILL tests recovery without destructor cleanup.
#[test]
fn keyed_crash_fixture() {
    let Some(root) = std::env::var_os("SANDBOX_KEYED_CRASH_FIXTURE") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let agent = Agent::new(config(&root)).unwrap();
        let mut stream = connect(&agent, Portal::Product);
        write_frame(&mut stream, &crash_request(&root))
            .await
            .unwrap();
        let started = next(&mut stream).await;
        let pid = sandbox_agent::number(&started.params_json(), "pid").unwrap();
        while !root.join("effect").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        std::fs::write(root.join("workload-pid"), pid.to_string()).unwrap();
        loop {
            next(&mut stream).await;
        }
    });
}

#[tokio::test]
async fn crash_after_side_effect_reports_unknown_and_never_replays() {
    let tmp = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "keyed_crash_fixture", "--nocapture"])
        .env("SANDBOX_KEYED_CRASH_FIXTURE", tmp.path())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !tmp.path().join("workload-pid").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "crash fixture exited early"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    let pid: i32 = std::fs::read_to_string(tmp.path().join("workload-pid"))
        .unwrap()
        .parse()
        .unwrap();
    unsafe {
        libc::kill(-pid, libc::SIGKILL);
    }
    let cfg = config(tmp.path());
    for _ in 0..2 {
        let agent = Agent::new(cfg.clone()).unwrap();
        let mut stream = connect(&agent, Portal::Product);
        let result = rpc(&mut stream, crash_request(tmp.path())).await;
        assert_eq!(result.last().unwrap().error, "result_unknown");
        assert_eq!(
            result.last().unwrap().params_json()["result_state"],
            "result_unknown"
        );
        assert_eq!(std::fs::read(tmp.path().join("effect")).unwrap(), b"x");
        drop(stream);
        wait_idle(&agent).await;
    }
}

#[tokio::test]
async fn prepare_template_removes_saved_history_before_a_new_sandbox() {
    let tmp = tempfile::tempdir().unwrap();
    let agent = Agent::new(config(tmp.path())).unwrap();
    let mut stream = connect(&agent, Portal::Product);
    let frame = exec("warm", "operation-step", "printf TEMPLATE_OUTPUT_PRIVATE");
    success(&rpc(&mut stream, frame.clone()).await);
    drop(stream);
    wait_idle(&agent).await;
    let prepared = rpc(
        &mut connect(&agent, Portal::Admin),
        request("prepare", "prepare_template", json!({})),
    )
    .await;
    success(&prepared);
    assert_eq!(prepared.last().unwrap().params_json()["prepared"], true);
    let db = std::fs::read(agent.ledger.directory.join("exec.sqlite3")).unwrap();
    assert!(
        !db.windows(b"TEMPLATE_OUTPUT_PRIVATE".len())
            .any(|w| w == b"TEMPLATE_OUTPUT_PRIVATE")
    );
    assert!(agent.processes.lock().await.is_empty());
    success(&activate(&agent, "new-sandbox", json!({})).await);
    let first = rpc(&mut connect(&agent, Portal::Product), frame).await;
    success(&first);
    assert_eq!(first.last().unwrap().params_json()["replayed"], false);
}
