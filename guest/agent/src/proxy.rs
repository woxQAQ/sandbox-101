use crate::Agent;
use anyhow::Result;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::Semaphore};
use tokio_vsock::{VsockAddr, VsockStream};

pub async fn listeners(agent: Arc<Agent>) -> Result<()> {
    let http = TcpListener::bind("127.0.0.1:3128").await?;
    let socks = TcpListener::bind("127.0.0.1:1080").await?;
    let slots = Arc::new(Semaphore::new(64));
    loop {
        let (mut guest, port) = tokio::select! {r=http.accept()=>{let(s,_)=r?;(s,2049)},r=socks.accept()=>{let(s,_)=r?;(s,2050)}};
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            continue;
        };
        let agent = agent.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let mut policy = agent.policy_changed.subscribe();
            if agent.activation.lock().await.network != "relay" {
                return;
            }
            let host = tokio::select! {
                _=policy.changed()=>return,
                result=tokio::time::timeout(Duration::from_secs(5),VsockStream::connect(VsockAddr::new(2,port)))=>match result{Ok(Ok(s))=>s,_=>return}
            };
            let mut host = host;
            tokio::select! {
                _=policy.changed()=>{},
                _=tokio::io::copy_bidirectional(&mut guest,&mut host)=>{},
            }
        });
    }
}

pub async fn raw_port_forward<S>(mut stream: S, request: crate::wire::Frame) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let result: Result<tokio::net::TcpStream> = async {
        anyhow::ensure!(
            !request.id.is_empty() && request.id.len() <= 128 && request.error.is_empty(),
            "invalid port_forward request"
        );
        anyhow::ensure!(
            request.data.is_empty(),
            "raw port_forward sends payload only after ready"
        );
        let port = crate::number(&request.params_json(), "port")?;
        anyhow::ensure!((1..=65535).contains(&port), "invalid port");
        Ok(tokio::time::timeout(
            Duration::from_secs(5),
            tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port as u16)),
        )
        .await??)
    }
    .await;
    let mut guest = match result {
        Ok(guest) => guest,
        Err(error) => {
            crate::wire::write_frame(&mut stream, &request.failure(error)).await?;
            return Ok(());
        }
    };
    crate::wire::write_frame(
        &mut stream,
        &request.reply(json!({"ready":true}), vec![], false),
    )
    .await?;
    tokio::io::copy_bidirectional(&mut stream, &mut guest).await?;
    Ok(())
}
