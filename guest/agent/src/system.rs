use crate::{Agent, env_map, number, string, wire::Frame};
use anyhow::{Result, bail, ensure};
use serde_json::json;
use std::{fs::OpenOptions, io::Read, os::fd::AsRawFd, sync::Arc};

pub fn dispatch(frame: &Frame) -> Result<Frame> {
    let p = frame.params_json();
    match frame.method.as_str() {
        "ping" => Ok(frame.reply(
            json!({"protocol":1,"service":"sandbox-agent","portal":"system"}),
            vec![],
            true,
        )),
        "reseed" => {
            ensure!(
                (32..=4096).contains(&frame.data.len()),
                "reseed requires 32..4096 bytes of fresh host entropy"
            );
            // linux/random.h: entropy_count, buf_size, flexible byte array.
            let mut pool = vec![0i32; 2 + frame.data.len().div_ceil(4)];
            pool[0] = (frame.data.len() * 8) as i32;
            pool[1] = frame.data.len() as i32;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    frame.data.as_ptr(),
                    pool.as_mut_ptr().add(2).cast::<u8>(),
                    frame.data.len(),
                );
            }
            let random = OpenOptions::new()
                .read(true)
                .write(true)
                .open("/dev/urandom")?;
            if unsafe { libc::ioctl(random.as_raw_fd(), 0x40085203_u32 as _, pool.as_ptr()) } < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let reseeded = if unsafe { libc::ioctl(random.as_raw_fd(), 0x5207 as _, 0) } < 0 {
                let error = std::io::Error::last_os_error();
                // Linux before 4.17 mixes credited entropy through RNDADDENTROPY.
                if error.raw_os_error() != Some(libc::ENOTTY) {
                    return Err(error.into());
                }
                false
            } else {
                true
            };
            Ok(frame.reply(
                json!({"entropy_bytes":frame.data.len(),"reseed_ioctl":reseeded}),
                vec![],
                true,
            ))
        }
        "clock_sync" => {
            let seconds = number(&p, "unix_seconds")?;
            let nanos = number(&p, "nanoseconds")?;
            ensure!(
                seconds <= i64::MAX as u64 && nanos < 1_000_000_000,
                "invalid realtime timestamp"
            );
            let ts = libc::timespec {
                tv_sec: seconds as _,
                tv_nsec: nanos as _,
            };
            if unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &ts) } < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(frame.reply(json!({}), vec![], true))
        }
        _ => bail!("method is not authorized on system portal"),
    }
}

pub async fn prepare_template(agent: &Arc<Agent>, frame: &Frame) -> Result<Frame> {
    agent
        .quiesced
        .store(true, std::sync::atomic::Ordering::SeqCst);
    ensure!(
        agent.requests.available_permits() == 128
            && agent
                .active_streams
                .load(std::sync::atomic::Ordering::SeqCst)
                == 0,
        "other RPCs are still in flight; retry after draining"
    );
    ensure!(
        agent.sessions.lock().await.is_empty(),
        "remove persistent sessions before preparing a template"
    );
    let mut activation = agent.activation.lock().await;
    agent.ledger.set_identity("", true)?;
    activation.network = "none".into();
    activation.sandbox_id.clear();
    activation.generation += 1;
    agent.policy_changed.send_replace(activation.generation);
    crate::fs::atomic_write(&agent.config.env_file, b"{}", 0o600)?;
    crate::fs::atomic_write(&agent.config.machine_id, b"", 0o444)?;
    let ca = agent.config.env_file.with_file_name("sandbox-ca.pem");
    match std::fs::remove_file(ca) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let mut table = agent.processes.lock().await;
    table.retain(|_, p| p.status.lock().unwrap().exit.is_none());
    for process in table.values() {
        let mut s = process.status.lock().unwrap();
        s.logs.clear();
        s.bytes = 0;
    }
    let root = std::fs::File::open("/")?;
    if unsafe { libc::syncfs(root.as_raw_fd()) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(frame.reply(json!({"quiescent":true,"prepared":true}), vec![], true))
}

pub async fn activate(agent: &Arc<Agent>, frame: &Frame) -> Result<Frame> {
    let p = frame.params_json();
    let id = string(&p, "sandbox_id")?;
    ensure!(!id.is_empty() && id.len() <= 128, "invalid sandbox_id");
    let network = string(&p, "network")?;
    ensure!(
        ["none", "relay", "direct"].contains(&network),
        "invalid network mode"
    );
    let env = env_map(&p)?;
    let reset = p
        .get("reset_identity")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut activation = agent.activation.lock().await;
    agent.ledger.set_identity(id, false)?;
    // Disallow egress while committing activation, including on a partial failure.
    activation.network = "none".into();
    activation.generation += 1;
    agent.policy_changed.send_replace(activation.generation);
    if reset {
        let hostname = string(&p, "hostname")?;
        ensure!(
            !hostname.is_empty()
                && hostname.len() <= 63
                && hostname
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.'),
            "invalid hostname"
        );
        if unsafe { libc::sethostname(hostname.as_ptr().cast(), hostname.len()) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut bytes = [0u8; 16];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        let machine_id = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>() + "\n";
        crate::fs::atomic_write(&agent.config.machine_id, machine_id.as_bytes(), 0o444)?;
        crate::fs::atomic_write(
            &agent.config.machine_id.with_file_name("hostname"),
            format!("{hostname}\n").as_bytes(),
            0o644,
        )?;
    }
    let ca_file = agent.config.env_file.with_file_name("sandbox-ca.pem");
    let ca = p.get("ca_pem").and_then(|v| v.as_str()).unwrap_or("");
    ensure!(ca.len() <= 128 * 1024, "CA PEM exceeds 128 KiB");
    if ca.is_empty() {
        match std::fs::remove_file(&ca_file) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    } else {
        ensure!(
            ca.contains("-----BEGIN CERTIFICATE-----") && ca.contains("-----END CERTIFICATE-----"),
            "invalid CA PEM envelope"
        );
        crate::fs::atomic_write(&ca_file, ca.as_bytes(), 0o644)?;
    }
    crate::fs::atomic_write(&agent.config.env_file, &serde_json::to_vec(&env)?, 0o600)?;
    activation.sandbox_id = id.into();
    activation.network = network.into();
    agent
        .quiesced
        .store(false, std::sync::atomic::Ordering::SeqCst);
    Ok(frame.reply(
        json!({"sandbox_id":id,"network":network,"identity_reset":reset}),
        vec![],
        true,
    ))
}
