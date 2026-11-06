use crate::{
    Agent,
    ledger::{Admission, Saved},
    server::{Input, Output},
};
use anyhow::{Result, ensure};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::mpsc;

async fn failure(out: &Output, state: &str, code: &str) -> Result<()> {
    let mut frame = out.request.reply(
        json!({"result_state":state,"error_code":code}),
        vec![],
        true,
    );
    frame.error = code.into();
    out.send(frame).await
}

async fn result(out: &Output, saved: &Saved, replayed: bool) -> Result<()> {
    if replayed {
        for (stream, bytes) in [("stdout", &saved.stdout), ("stderr", &saved.stderr)] {
            if !bytes.is_empty() {
                out.event(json!({"stream":stream,"replayed":true}), bytes.clone())
                    .await?;
            }
        }
    }
    out.done(
        json!({"result_state":"completed","exit_code":saved.exit_code,"replayed":replayed,
        "stdout_bytes":saved.stdout_bytes.to_string(),"stderr_bytes":saved.stderr_bytes.to_string(),
        "stdout_truncated":saved.stdout_bytes > saved.stdout.len() as u64,
        "stderr_truncated":saved.stderr_bytes > saved.stderr.len() as u64}),
    )
    .await
}

// A task failure, including a panic before result persistence, must never reopen the key.
struct Intent {
    agent: Arc<Agent>,
    key: [u8; 32],
    completed: bool,
}
impl Drop for Intent {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.agent.ledger.unknown(&self.key);
        }
    }
}

pub async fn execute(agent: &Arc<Agent>, out: &Output, input: Input) -> Result<()> {
    let mut params = out.request.params_json();
    let key = crate::string(&params, "exec_key")?.to_owned();
    ensure!(
        !key.is_empty() && key.len() <= 256 && key.bytes().all(|b| (0x21..=0x7e).contains(&b)),
        "exec_key must contain 1..256 visible ASCII bytes"
    );
    ensure!(
        out.request.method == "exec" && params.get("session_id").is_none(),
        "exec_key is unsupported for sessions and PTYs"
    );
    ensure!(
        params.get("detach").is_none_or(|v| v == false),
        "exec_key does not support detach"
    );
    ensure!(
        out.request.eof && out.request.data.len() <= crate::wire::CHUNK,
        "exec_key requires complete stdin in the initial EOF frame (at most 64 KiB); streaming stdin is unsupported"
    );
    params.as_object_mut().unwrap().remove("exec_key");
    // Hold activation through the environment snapshot and durable admission. This
    // orders identity changes against all keyed starts, including concurrent connections.
    let activation = agent.activation.lock().await;
    ensure!(
        !agent.quiesced.load(std::sync::atomic::Ordering::SeqCst),
        "agent is quiesced"
    );
    let environment = agent.environment_for(&activation).await?;
    let mut command = crate::process::command_with_environment(agent, &params, environment)?;
    let cwd = command
        .as_std()
        .get_current_dir()
        .map(|p| p.to_path_buf())
        .unwrap_or(std::env::current_dir()?);
    let cwd = if cwd.is_absolute() {
        cwd
    } else {
        std::env::current_dir()?.join(cwd)
    };
    command.current_dir(&cwd);
    let effective_env: BTreeMap<_, _> = command
        .as_std()
        .get_envs()
        .map(|(key, value)| {
            (
                key.to_string_lossy().into_owned(),
                value.map(|v| v.to_string_lossy().into_owned()),
            )
        })
        .collect();
    let cwd = cwd
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("keyed exec requires a UTF-8 cwd"))?;
    let fingerprint = agent.ledger.fingerprint("exec-input-v1", &serde_json::to_vec(&json!({
        "method":out.request.method,"params":params,"stdin":agent.ledger.fingerprint("stdin",&out.request.data),
        "eof":out.request.eof,"cwd":cwd,"environment":effective_env,
        "uid":agent.config.uid,"gid":agent.config.gid,"sandbox_id":activation.sandbox_id,
    }))?);
    let admission = agent
        .ledger
        .admit(&activation.sandbox_id, &key, &fingerprint)?;
    drop(activation);
    let key = match admission {
        Admission::New(key) => key,
        Admission::Completed(saved) => return result(out, &saved, true).await,
        Admission::InProgress => return failure(out, "in_progress", "exec_in_progress").await,
        Admission::Unknown => return failure(out, "result_unknown", "result_unknown").await,
        Admission::Conflict => return failure(out, "conflict", "exec_key_conflict").await,
        Admission::Exhausted => {
            return failure(out, "rejected", "exec_ledger_capacity_exhausted").await;
        }
    };
    let mut intent = Intent {
        agent: agent.clone(),
        key,
        completed: false,
    };
    let (tx, mut rx) = mpsc::channel(8);
    let internal = Output {
        request: out.request.clone(),
        tx,
        cancel: out.cancel.clone(),
    };
    let runner_agent = agent.clone();
    let runner = tokio::spawn(async move {
        crate::process::execute_command(&runner_agent, &internal, input, command).await
    });
    let mut saved = Saved::default();
    let mut terminal = false;
    let mut connected = true;
    while let Some(frame) = rx.recv().await {
        let p = frame.params_json();
        if frame.eof {
            if let Ok(code) = crate::number(&p, "exit_code")
                && code <= 255
            {
                saved.exit_code = code as i32;
                terminal = true;
            }
        } else if let Some(stream @ ("stdout" | "stderr")) =
            p.get("stream").and_then(|v| v.as_str())
        {
            let before = if stream == "stdout" {
                saved.stdout.len()
            } else {
                saved.stderr.len()
            };
            saved.append(stream, &frame.data);
            let buffer = if stream == "stdout" {
                &saved.stdout
            } else {
                &saved.stderr
            };
            if connected && buffer.len() > before {
                connected = out.event(p, buffer[before..].to_vec()).await.is_ok();
            }
        } else if connected {
            connected = out.send(frame).await.is_ok();
        }
    }
    let success = matches!(runner.await, Ok(Ok(()))) && terminal;
    if !success || agent.ledger.complete(&key, &saved).is_err() {
        // A failure to commit a terminal result is also unknown, even if a child exited.
        intent.completed = agent.ledger.unknown(&key).is_ok();
        return failure(out, "result_unknown", "result_unknown").await;
    }
    intent.completed = true;
    result(out, &saved, false).await
}
