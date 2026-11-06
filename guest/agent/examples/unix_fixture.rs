//! Explicit local integration fixture. Production remains AF_VSOCK-only.
//! Only the unprivileged product portal is exposed: no host entropy, clock,
//! hostname, machine-id, template preparation, or activation operations.
use anyhow::Result;
use sandbox_agent::{
    Agent, Config,
    server::{Portal, serve_connection},
};
use std::{path::PathBuf, time::Duration};
use tokio::net::UnixListener;

#[tokio::main]
async fn main() -> Result<()> {
    let dir = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or_else(|| anyhow::anyhow!("usage: unix_fixture SOCKET_DIR"))?,
    );
    std::fs::create_dir_all(&dir)?;
    let listener = UnixListener::bind(dir.join("product.sock"))?;
    let agent = Agent::new(Config {
        env_file: dir.join("sandbox.env"),
        machine_id: dir.join("machine-id"),
        ledger_dir: dir.join("ledger"),
        ledger_max_entries: sandbox_agent::ledger::MAX_ENTRIES,
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
    })?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut reap = tokio::time::interval(Duration::from_secs(30));
    println!("ready {}", dir.join("product.sock").display());
    loop {
        tokio::select! {
            next=listener.accept()=>{
                let (stream,_)=next?;
                let agent=agent.clone();
                tokio::spawn(async move{let _=serve_connection(agent,stream,Portal::Product).await;});
            }
            _=reap.tick()=>agent.reap().await,
            _=terminate.recv()=>break,
            _=tokio::signal::ctrl_c()=>break,
        }
    }
    for process in agent.processes.lock().await.values() {
        let _ = process.signal(libc::SIGKILL);
    }
    for session in agent.sessions.lock().await.values() {
        session.lock().await.kill();
    }
    std::fs::remove_file(dir.join("product.sock"))?;
    Ok(())
}
