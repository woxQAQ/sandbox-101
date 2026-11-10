use crate::{
    Agent, env_map,
    process::{command, random_id},
    server::Output,
    string,
    wire::CHUNK,
};
use crate::{Error, ErrorCode, Result, ResultContext};
use serde_json::json;
use std::{
    collections::BTreeSet,
    os::fd::AsRawFd,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStderr, ChildStdin, ChildStdout},
    sync::Mutex,
};

pub struct Session {
    pub child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: ChildStderr,
    completion: tokio::net::UnixStream,
    pub last_used: Instant,
    injected: BTreeSet<String>,
    defaults: std::collections::BTreeMap<String, String>,
}
impl Session {
    pub fn has_exited(&self) -> bool {
        let Some(pid) = self.child.id() else {
            return true;
        };
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        result == 0 && unsafe { info.si_pid() } != 0
    }
    pub fn kill(&mut self) {
        if let Some(pid) = self.child.id() {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
        let _ = self.child.start_kill();
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.kill();
    }
}
fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}
fn shell_key(s: &str) -> bool {
    let mut b = s.bytes();
    b.next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == b'_')
        && b.all(|c| c.is_ascii_alphanumeric() || c == b'_')
}

pub async fn dispatch(agent: &Arc<Agent>, out: &Output) -> Result<()> {
    let p = out.request.params_json();
    match out.request.method.as_str() {
        "session_create" => {
            let mut config = p.clone();
            config["command"] = json!("/bin/sh");
            config["argv"] = json!([]);
            let defaults = env_map(&p)?;
            let injected = agent
                .environment()
                .await?
                .keys()
                .chain(defaults.keys())
                .cloned()
                .collect();
            let mut cmd = command(agent, &config).await?;
            cmd.stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let (parent, child_fd) = std::os::unix::net::UnixStream::pair()
                .or_context(|| "create session completion channel".into())?;
            let fd = child_fd.as_raw_fd();
            unsafe {
                cmd.pre_exec(move || {
                    if libc::dup2(fd, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let mut sessions = agent.sessions.lock().await;
            ensure!(sessions.len() < 64, ResourceExhausted => "session capacity exhausted");
            let mut child = cmd.spawn().or_context(|| "spawn session /bin/sh".into())?;
            drop(child_fd);
            parent.set_nonblocking(true)?;
            let id = format!("s-{}", random_id()?);
            let pid = child.id();
            let session = Session {
                stdin: child.stdin.take().unwrap(),
                stdout: child.stdout.take().unwrap(),
                stderr: child.stderr.take().unwrap(),
                completion: tokio::net::UnixStream::from_std(parent)?,
                last_used: Instant::now(),
                injected,
                defaults,
                child,
            };
            sessions.insert(id.clone(), Arc::new(Mutex::new(session)));
            drop(sessions);
            out.done(json!({"session_id":id,"pid":pid})).await
        }
        "session_list" => {
            let sessions = agent.sessions.lock().await;
            let ids: Vec<_> = sessions.keys().cloned().collect();
            drop(sessions);
            out.done(json!({"sessions":ids})).await
        }
        "session_rm" => {
            let id = string(&p, "session_id")?;
            let session =
                agent.sessions.lock().await.remove(id).ok_or_else(|| {
                    Error::new(ErrorCode::NotFound, format!("unknown session {id}"))
                })?;
            // Removal waits for the current command, bounded by its five-minute timeout.
            let mut session = session.lock().await;
            session.kill();
            let _ = session.child.wait().await;
            out.done(json!({})).await
        }
        _ => bail!("unknown session method"),
    }
}

pub async fn execute(agent: &Arc<Agent>, out: &Output) -> Result<()> {
    let p = out.request.params_json();
    ensure!(
        out.request.data.is_empty(),
        "session exec does not accept stdin data"
    );
    ensure!(
        !p.get("detach").and_then(|v| v.as_bool()).unwrap_or(false),
        "session exec cannot detach"
    );
    ensure!(
        p.get("argv").is_none(),
        "session exec requires a shell command, without argv"
    );
    let id = string(&p, "session_id")?;
    let entry = agent
        .sessions
        .lock()
        .await
        .get(id)
        .cloned()
        .ok_or_else(|| Error::new(ErrorCode::NotFound, format!("unknown session {id}")))?;
    let mut session = tokio::select! {_=out.cancel.cancelled()=>bail!(Internal => "connection closed"),s=entry.lock()=>s};
    let mut env = agent.environment().await?;
    env.extend(session.defaults.clone());
    env.extend(env_map(&p)?);
    let result=tokio::time::timeout(Duration::from_secs(300),async {
        ensure!(!session.has_exited(), Conflict => "session {id} shell exited");
        let mut script=String::new();
        for old in &session.injected {if !env.contains_key(old){script.push_str(&format!("unset {old}\n"));}}
        for (key,value) in &env {ensure!(shell_key(key),"invalid shell environment key");script.push_str(&format!("export {key}={}\n",quote(value)));}
        session.injected=env.keys().cloned().collect();
        let mut shell=string(&p,"command")?.to_owned();
        if let Some(cwd)=p.get("cwd"){shell=format!("cd {} && {{\n{shell}\n}}",quote(cwd.as_str().ok_or_else(||Error::new(ErrorCode::Invalid, "cwd must be a string"))?));}
        script.push_str(&format!("eval {}\n__sandbox_status=$?\nprintf '%s\\n' \"$__sandbox_status\" >&3\n",quote(&shell)));
        session.stdin.write_all(script.as_bytes()).await?;session.stdin.flush().await?;
        let mut stdout=vec![0;CHUNK];let mut stderr=vec![0;CHUNK];let mut status=Vec::new();let mut code=None;
        loop {
            let Session{stdout:out_pipe,stderr:err_pipe,completion,..}=&mut *session;
            tokio::select!{
                biased;
                _=out.cancel.cancelled()=>bail!(Internal => "connection closed"),
                n=out_pipe.read(&mut stdout)=>{let n=n?;ensure!(n>0, Conflict => "session stdout closed");out.event(json!({"stream":"stdout","session_id":id}),stdout[..n].to_vec()).await?;},
                n=err_pipe.read(&mut stderr)=>{let n=n?;ensure!(n>0, Conflict => "session stderr closed");out.event(json!({"stream":"stderr","session_id":id}),stderr[..n].to_vec()).await?;},
                n=completion.read_u8(),if code.is_none()=>{let b=n?;status.push(b);ensure!(status.len()<=16, Conflict => "invalid shell completion");if b==b'\n'{code=Some(std::str::from_utf8(&status)?.trim().parse::<i32>()?);}},
                _=tokio::time::sleep(Duration::from_millis(5)),if code.is_some()=>break,
            }
        }
        session.last_used=Instant::now();
        out.done(json!({"exit_code":code.unwrap(),"process_id":format!("session:{id}"),"session_id":id})).await
    }).await;
    match result {
        Ok(Ok(())) => Ok(()),
        other => {
            session.kill();
            let _ = session.child.wait().await;
            drop(session);
            agent.sessions.lock().await.remove(id);
            match other {
                Ok(Err(e)) => Err(e),
                Err(e) => Err(e.into()),
                _ => unreachable!(),
            }
        }
    }
}
