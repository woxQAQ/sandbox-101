use anyhow::{Result, ensure};
use sandbox_agent::{
    Agent, Config,
    server::{Portal, serve_connection},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--uid" => {
                config.uid = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing uid"))?
                    .parse()?
            }
            "--gid" => {
                config.gid = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing gid"))?
                    .parse()?
            }
            _ => anyhow::bail!("usage: sandbox-agent [--uid 1000] [--gid 1000]"),
        }
    }
    ensure!(
        config.uid != 0 && config.gid != 0,
        "workloads must use an unprivileged uid and gid"
    );
    let agent = Agent::new(config)?;
    let admin = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, 2046))?;
    let system = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, 2047))?;
    let product = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, 2048))?;
    let proxy_agent = agent.clone();
    let mut proxy = tokio::spawn(async move { sandbox_agent::proxy::listeners(proxy_agent).await });
    let connections = Arc::new(Semaphore::new(64));
    let system_connections = Arc::new(Semaphore::new(8));
    let admin_connections = Arc::new(Semaphore::new(8));
    let mut reap = tokio::time::interval(Duration::from_secs(30));
    loop {
        let (stream, address, portal) = tokio::select! {
            r=admin.accept()=>{let(s,a)=r?;(s,a,Portal::Admin)},
            r=system.accept()=>{let(s,a)=r?;(s,a,Portal::System)},
            r=product.accept()=>{let(s,a)=r?;(s,a,Portal::Product)},
            _=reap.tick()=>{agent.reap().await;continue;},
            r=&mut proxy=>{r??;anyhow::bail!("proxy listeners stopped");},
            _=tokio::signal::ctrl_c()=>break,
        };
        if address.cid() != 2 {
            continue;
        }
        let budget = match portal {
            Portal::System => &system_connections,
            Portal::Admin => &admin_connections,
            Portal::Product => &connections,
        };
        let Ok(permit) = budget.clone().try_acquire_owned() else {
            continue;
        };
        let agent = agent.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = serve_connection(agent, stream, portal).await {
                eprintln!("agent connection failed: {error:#}");
            }
        });
    }
    proxy.abort();
    Ok(())
}
