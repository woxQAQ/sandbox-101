use crate::{
    Agent, string,
    wire::{self, Frame},
};
use anyhow::{Result, bail, ensure};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, mpsc},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Portal {
    System,
    Product,
    Admin,
}
#[derive(Clone)]
pub struct Output {
    pub request: Frame,
    pub tx: mpsc::Sender<Frame>,
    pub cancel: CancellationToken,
}
impl Output {
    pub async fn send(&self, frame: Frame) -> Result<()> {
        tokio::select! {
            _ = self.cancel.cancelled() => bail!("connection closed"),
            result = tokio::time::timeout(Duration::from_secs(30),self.tx.send(frame)) => {
                match result {
                    Ok(Ok(())) => Ok(()),
                    _ => { self.cancel.cancel(); bail!("output stalled or disconnected") }
                }
            }
        }
    }
    pub async fn event(&self, params: serde_json::Value, data: Vec<u8>) -> Result<()> {
        self.send(self.request.reply(params, data, false)).await
    }
    pub async fn done(&self, params: serde_json::Value) -> Result<()> {
        self.send(self.request.reply(params, vec![], true)).await
    }
}
pub type Input = mpsc::Receiver<Frame>;

pub async fn serve_connection<S>(agent: Arc<Agent>, mut stream: S, portal: Portal) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut first =
        tokio::time::timeout(Duration::from_secs(30), wire::read_frame(&mut stream)).await??;
    if portal == Portal::Product && first.as_ref().is_some_and(|f| f.method == "port_forward") {
        use std::sync::atomic::Ordering;
        struct StreamGuard(Arc<Agent>);
        impl Drop for StreamGuard {
            fn drop(&mut self) {
                self.0.active_streams.fetch_sub(1, Ordering::SeqCst);
            }
        }
        agent.active_streams.fetch_add(1, Ordering::SeqCst);
        let _guard = StreamGuard(agent.clone());
        let request = first.take().unwrap();
        if agent.quiesced.load(Ordering::SeqCst) {
            wire::write_frame(&mut stream, &request.failure("agent is quiesced")).await?;
            return Ok(());
        }
        return crate::proxy::raw_port_forward(stream, request).await;
    }
    let (mut read, mut write) = tokio::io::split(stream);
    let (tx, mut rx) = mpsc::channel::<Frame>(8);
    let cancel = CancellationToken::new();
    let writer_cancel = cancel.clone();
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = writer_cancel.cancelled() => break,
                frame = rx.recv() => {
                    let Some(frame) = frame else { break; };
                    if !matches!(tokio::time::timeout(Duration::from_secs(30),wire::write_frame(&mut write,&frame)).await,Ok(Ok(()))) { writer_cancel.cancel(); break; }
                }
            }
        }
    });
    let active: Arc<Mutex<BTreeMap<String, mpsc::Sender<Frame>>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
    let outcome = async {
        loop {
            let next = if let Some(frame) = first.take() { Some(frame) } else { tokio::select! { _ = cancel.cancelled() => break, result = wire::read_frame(&mut read) => result? } };
            let Some(frame) = next else { break; };
            ensure!(!frame.id.is_empty() && frame.id.len() <= 128 && frame.method.len() <= 128, "invalid request identity");
            ensure!(frame.error.is_empty(), "client error frames are invalid");
            if frame.method.is_empty() {
                ensure!(frame.data.len() <= wire::CHUNK, "input chunk exceeds 64 KiB");
                ensure!(frame.params.as_ref().is_none_or(|p|p.fields.is_empty()), "continuation must not carry params");
                let sender = active.lock().await.get(&frame.id).cloned();
                if let Some(sender) = sender {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        result = tokio::time::timeout(Duration::from_secs(30), sender.send(frame)) => {
                            ensure!(matches!(result,Ok(Ok(()))), "input stalled or RPC already completed");
                        }
                    }
                } else { bail!("continuation for unknown request"); }
                continue;
            }
            let mut requests = active.lock().await;
            // Duplicate active IDs are a connection error, never a second execution.
            ensure!(!requests.contains_key(&frame.id), "duplicate active request id");
            let slots=if portal==Portal::Product{agent.requests.clone()}else{agent.control_requests.clone()};
            let permit = if requests.len() < 16 { slots.try_acquire_owned().ok() } else { None };
            let Some(permit) = permit else {
                drop(requests);
                Output {request:frame.clone(),tx:tx.clone(),cancel:cancel.clone()}.send(frame.failure("request capacity exhausted")).await?;
                continue;
            };
            let (input_tx,input) = mpsc::channel(8);
            requests.insert(frame.id.clone(),input_tx); drop(requests);
            let agent = agent.clone(); let active = active.clone();
            let output = Output {request:frame.clone(),tx:tx.clone(),cancel:cancel.clone()};
            tokio::spawn(async move {
                let _permit = permit;
                let result = dispatch(agent,portal,&output,input).await;
                if let Err(error) = result { let _ = output.send(frame.failure(format!("{error:#}"))).await; }
                active.lock().await.remove(&frame.id);
            });
        }
        Ok(())
    }.await;
    cancel.cancel();
    writer.await?;
    outcome
}

async fn dispatch(agent: Arc<Agent>, portal: Portal, out: &Output, input: Input) -> Result<()> {
    let f = &out.request;
    if portal == Portal::System {
        return out.send(crate::system::dispatch(f)?).await;
    }
    let p = f.params_json();
    if portal == Portal::Admin {
        return match f.method.as_str() {
            "activate" => out.send(crate::system::activate(&agent, f).await?).await,
            "prepare_template" => {
                out.send(crate::system::prepare_template(&agent, f).await?)
                    .await
            }
            "quiesce" => {
                agent
                    .quiesced
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                let running = agent
                    .processes
                    .lock()
                    .await
                    .values()
                    .filter(|p| p.status.lock().unwrap().exit.is_none())
                    .count();
                let sessions = agent.sessions.lock().await.len();
                let in_flight = 128 - agent.requests.available_permits()
                    + agent
                        .active_streams
                        .load(std::sync::atomic::Ordering::SeqCst);
                out.done(json!({"quiescent":running==0 && sessions==0 && in_flight==0,"in_flight":in_flight,"running_processes":running,"sessions":sessions})).await
            }
            _ => bail!("method is not authorized on admin portal"),
        };
    }
    ensure!(
        !agent.quiesced.load(std::sync::atomic::Ordering::SeqCst) || f.method == "info",
        "agent is quiesced"
    );
    ensure!(
        p.get("exec_key").is_none() || matches!(f.method.as_str(), "exec" | "pty_open"),
        "exec_key is unsupported for this method"
    );
    match f.method.as_str() {
        "info" => {
            let a = agent.activation.lock().await;
            out.done(json!({"protocol":1,"portal":"product","sandbox_id":a.sandbox_id,"network":a.network,"active":!a.sandbox_id.is_empty(),"processes":agent.processes.lock().await.len(),"in_flight":(128-agent.requests.available_permits()).saturating_sub(1)+agent.active_streams.load(std::sync::atomic::Ordering::SeqCst),"quiesced":agent.quiesced.load(std::sync::atomic::Ordering::SeqCst)})).await
        }
        "activate" | "quiesce" | "prepare_template" => {
            bail!("method is not authorized on product portal")
        }
        "ping" | "reseed" | "clock_sync" => {
            bail!("system method is not authorized on product portal")
        }
        "exec" | "pty_open" if p.get("exec_key").is_some() => {
            crate::keyed::execute(&agent, out, input).await
        }
        "exec" if p.get("session_id").is_some() => crate::session::execute(&agent, out).await,
        "exec" | "pty_open" => crate::process::execute(&agent, out, input).await,
        "proc_ps" | "proc_kill" | "proc_logs" | "proc_attach" | "pty_resize" => {
            crate::process::dispatch(&agent, out).await
        }
        "session_create" | "session_list" | "session_rm" => {
            crate::session::dispatch(&agent, out).await
        }
        "fs_read" | "fs_write" | "fs_stat" | "fs_list" | "fs_mkdir" | "fs_rm" | "fs_rename"
        | "fs_find" | "fs_replace" | "fs_watch" | "tree_push" | "tree_pull" => {
            crate::fs::dispatch(&agent, out, input).await
        }
        "port_forward" => bail!("port_forward requires a fresh exclusive connection"),
        "cancel" => {
            let process_id = string(&p, "process_id")?;
            let process = agent
                .processes
                .lock()
                .await
                .get(process_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("unknown process"))?;
            process.signal(libc::SIGKILL)?;
            out.done(json!({})).await
        }
        _ => bail!("unknown product method"),
    }
}

pub async fn next_input(input: &mut Input, out: &Output) -> Result<Frame> {
    tokio::select! {
        _ = out.cancel.cancelled() => bail!("connection closed"),
        result = tokio::time::timeout(Duration::from_secs(60),input.recv()) => {
            result?.ok_or_else(||anyhow::anyhow!("input closed without eof"))
        }
    }
}
