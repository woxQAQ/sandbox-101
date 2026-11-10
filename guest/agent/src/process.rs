use crate::{
    Agent, env_map, number,
    server::{Input, Output, next_input},
    string,
    wire::CHUNK,
};
use crate::{Error, ErrorCode, Result, ResultContext};
use serde_json::json;
use std::{
    collections::VecDeque,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::broadcast,
};
use tokio_util::sync::CancellationToken;

const LOG_BYTES: usize = 1024 * 1024;
#[derive(Clone)]
pub struct Log {
    pub sequence: u64,
    pub stream: &'static str,
    pub data: Vec<u8>,
}
pub struct Status {
    pub logs: VecDeque<Log>,
    pub bytes: usize,
    pub sequence: u64,
    pub exit: Option<i32>,
    pub exited: bool,
    pub finished: Option<Instant>,
}
pub struct Process {
    pub id: String,
    pub pid: u32,
    pub pty: Option<Arc<OwnedFd>>,
    pub status: Mutex<Status>,
    pub events: broadcast::Sender<Log>,
    pub changed: tokio::sync::Notify,
}
impl Process {
    pub fn expired(&self) -> bool {
        self.status
            .lock()
            .unwrap()
            .finished
            .is_some_and(|t| t.elapsed() > Duration::from_secs(300))
    }
    pub fn signal(&self, signal: i32) -> Result<()> {
        let status = self.status.lock().unwrap();
        ensure!(!status.exited, Conflict => "process {} already exited", self.id);
        if unsafe { libc::kill(-(self.pid as i32), signal) } < 0 {
            return Err(
                Error::from(std::io::Error::last_os_error()).context(format!(
                    "signal {signal} process {} (pid={})",
                    self.id, self.pid
                )),
            );
        }
        Ok(())
    }
    fn append(&self, stream: &'static str, data: Vec<u8>) -> Log {
        let mut s = self.status.lock().unwrap();
        s.sequence += 1;
        let log = Log {
            sequence: s.sequence,
            stream,
            data,
        };
        s.bytes += log.data.len();
        s.logs.push_back(log.clone());
        while s.bytes > LOG_BYTES {
            if let Some(old) = s.logs.pop_front() {
                s.bytes -= old.data.len();
            }
        }
        let _ = self.events.send(log.clone());
        log
    }
}

pub fn restrict(command: &mut Command, uid: u32, gid: u32, session: bool) {
    unsafe {
        command.pre_exec(move || {
            if session && libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::geteuid() == 0 {
                if libc::setgroups(0, std::ptr::null()) < 0
                    || libc::setgid(gid) < 0
                    || libc::setuid(uid) < 0
                {
                    return Err(std::io::Error::last_os_error());
                }
            } else if libc::geteuid() != uid || libc::getegid() != gid {
                return Err(std::io::Error::from_raw_os_error(libc::EPERM));
            }
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

pub async fn command(agent: &Arc<Agent>, p: &serde_json::Value) -> Result<Command> {
    command_with_environment(agent, p, agent.environment().await?)
}

pub(crate) fn command_with_environment(
    agent: &Arc<Agent>,
    p: &serde_json::Value,
    environment: std::collections::BTreeMap<String, String>,
) -> Result<Command> {
    let command = string(p, "command")?;
    ensure!(!command.is_empty(), "empty command");
    let mut cmd =
        if let Some(argv) = p.get("argv") {
            let argv = argv
                .as_array()
                .ok_or_else(|| Error::new(ErrorCode::Invalid, "argv must be an array"))?;
            let mut cmd = Command::new(command);
            for arg in argv {
                cmd.arg(arg.as_str().ok_or_else(|| {
                    Error::new(ErrorCode::Invalid, "argv entries must be strings")
                })?);
            }
            cmd
        } else {
            let mut cmd = Command::new("/bin/sh");
            cmd.arg("-c").arg(command);
            cmd
        };
    cmd.env_clear()
        .env(
            "PATH",
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        )
        .env("HOME", "/home/sandbox")
        .env("USER", "sandbox");
    cmd.envs(environment).envs(env_map(p)?);
    if let Some(cwd) = p.get("cwd") {
        cmd.current_dir(
            cwd.as_str()
                .ok_or_else(|| Error::new(ErrorCode::Invalid, "cwd must be a string"))?,
        );
    }
    cmd.kill_on_drop(true);
    restrict(&mut cmd, agent.config.uid, agent.config.gid, true);
    Ok(cmd)
}

async fn pump<R: AsyncRead + Unpin>(
    mut reader: R,
    process: Arc<Process>,
    stream: &'static str,
    out: Option<Output>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut buf = vec![0; CHUNK];
    loop {
        let n = tokio::select! { _ = cancel.cancelled() => return Ok(()), result = reader.read(&mut buf) => match result {
            Ok(n) => n,
            Err(e) if process.pty.is_some() && e.raw_os_error() == Some(libc::EIO) => 0,
            Err(e) => {cancel.cancel();return Err(e.into());},
        }};
        if n == 0 {
            return Ok(());
        }
        let log = process.append(stream, buf[..n].to_vec());
        if let Some(out) = &out
            && let Err(e) = out
                .event(
                    json!({"stream":stream,"sequence":log.sequence,"process_id":process.id}),
                    log.data,
                )
                .await
        {
            cancel.cancel();
            return Err(e);
        }
    }
}

pub async fn execute(agent: &Arc<Agent>, out: &Output, input: Input) -> Result<()> {
    let cmd = command(agent, &out.request.params_json()).await?;
    execute_command(agent, out, input, cmd).await
}

pub(crate) async fn execute_command(
    agent: &Arc<Agent>,
    out: &Output,
    mut input: Input,
    mut cmd: Command,
) -> Result<()> {
    agent.reap().await;
    let p = out.request.params_json();
    let pty = out.request.method == "pty_open";
    let keyed = p.get("exec_key").is_some();
    let detach = p.get("detach").and_then(|v| v.as_bool()).unwrap_or(false);
    ensure!(
        !(pty && detach),
        "PTY cannot detach; use persistent sessions"
    );
    let mut table = agent.processes.lock().await;
    ensure!(table.len() < 256, ResourceExhausted => "process table capacity exhausted");
    let mut master = None;
    if pty {
        let (rows, cols) = dimensions(&p)?;
        let (mut m, mut s) = (-1, -1);
        let size = libc::winsize {
            ws_row: rows,
            ws_col: cols,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        if unsafe {
            libc::openpty(
                &mut m,
                &mut s,
                std::ptr::null_mut(),
                std::ptr::null(),
                &size,
            )
        } < 0
        {
            return Err(Error::from(std::io::Error::last_os_error()).context("openpty"));
        }
        let master_fd = unsafe { OwnedFd::from_raw_fd(m) };
        let slave_fd = unsafe { OwnedFd::from_raw_fd(s) };
        for fd in [m, s] {
            if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
                return Err(
                    Error::from(std::io::Error::last_os_error()).context("set PTY FD_CLOEXEC")
                );
            }
        }
        cmd.stdin(Stdio::from(slave_fd.try_clone()?))
            .stdout(Stdio::from(slave_fd.try_clone()?))
            .stderr(Stdio::from(slave_fd));
        unsafe {
            cmd.pre_exec(|| {
                if libc::ioctl(0, libc::TIOCSCTTY, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        master = Some(Arc::new(master_fd));
    } else {
        cmd.stdin(if detach {
            Stdio::null()
        } else {
            Stdio::piped()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    }
    let mut child = cmd
        .spawn()
        .or_context(|| "spawn workload process with restricted identity".into())?;
    drop(cmd); // Close the parent's retained PTY slave handles before waiting for EOF.
    let pid = child.id().unwrap();
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if raw < 0 {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
        let _ = child.wait().await;
        return Err(Error::from(error).context(format!("pidfd_open pid={pid}")));
    }
    let pidfd = unsafe { OwnedFd::from_raw_fd(raw as i32) };
    // The pidfd is only observed for readiness; wait() is deferred until the
    // process group has been cleaned up, so the leader PID cannot be reused.
    let pidfd = unsafe { tokio::io::unix::AsyncFd::register(pidfd) }.map_err(|e| {
        Error::new(
            ErrorCode::Internal,
            format!("register pidfd pid={pid}: {e}"),
        )
    })?;
    let id = format!("p-{pid}-{}", random_id()?);
    let (events, _) = broadcast::channel(32);
    let process = Arc::new(Process {
        id: id.clone(),
        pid,
        pty: master.clone(),
        status: Mutex::new(Status {
            logs: VecDeque::new(),
            bytes: 0,
            sequence: 0,
            exit: None,
            exited: false,
            finished: None,
        }),
        events,
        changed: tokio::sync::Notify::new(),
    });
    table.insert(id.clone(), process.clone());
    drop(table);
    if !detach
        && let Err(error) = out
            .event(json!({"process_id":id,"pid":pid,"started":true}), vec![])
            .await
    {
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
        let _ = child.wait().await;
        agent.processes.lock().await.remove(&id);
        return Err(error);
    }
    let local = if detach {
        CancellationToken::new()
    } else {
        out.cancel.child_token()
    };
    let output = if detach { None } else { Some(out.clone()) };
    let mut pumps = Vec::new();
    let input_task;
    if let Some(master) = master {
        let reader = AsyncPty::new(master.try_clone()?)?;
        let mut writer = AsyncPty::new(master.try_clone()?)?;
        pumps.push(tokio::spawn(pump(
            reader,
            process.clone(),
            "stdout",
            output.clone(),
            local.clone(),
        )));
        let initial = out.request.clone();
        let input_out = out.clone();
        let token = local.clone();
        input_task = Some(tokio::spawn(async move {
            let result: Result<()> = async {
                if !initial.data.is_empty() {
                    writer.write_all(&initial.data).await?;
                }
                if initial.eof {
                    writer.write_all(&[4]).await?;
                    return Ok(());
                }
                loop {
                    let f = next_input(&mut input, &input_out).await?;
                    writer.write_all(&f.data).await?;
                    if f.eof {
                        writer.write_all(&[4]).await?;
                        return Ok(());
                    }
                }
            }
            .await;
            if result.is_err() {
                token.cancel();
            }
        }));
    } else {
        pumps.push(tokio::spawn(pump(
            child.stdout.take().unwrap(),
            process.clone(),
            "stdout",
            output.clone(),
            local.clone(),
        )));
        pumps.push(tokio::spawn(pump(
            child.stderr.take().unwrap(),
            process.clone(),
            "stderr",
            output.clone(),
            local.clone(),
        )));
        input_task = child.stdin.take().map(|mut stdin| {
            let initial = out.request.clone();
            let input_out = out.clone();
            let token = local.clone();
            tokio::spawn(async move {
                let result: Result<()> = async {
                    stdin.write_all(&initial.data).await?;
                    if initial.eof {
                        stdin.shutdown().await?;
                        return Ok(());
                    }
                    loop {
                        let f = next_input(&mut input, &input_out).await?;
                        stdin.write_all(&f.data).await?;
                        if f.eof {
                            stdin.shutdown().await?;
                            return Ok(());
                        }
                    }
                }
                .await;
                if result.is_err() {
                    token.cancel();
                }
            })
        });
    }
    let runner_process = process.clone();
    let runner_out = out.clone();
    let runner = tokio::spawn(async move {
        tokio::select! {_ = local.cancelled()=>{},_ = pidfd.readable()=>{}}
        {
            let mut s = runner_process.status.lock().unwrap();
            s.exited = true;
            // Keep the unreaped leader's PID reserved until group cleanup.
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
        let status = child.wait().await;
        if let Some(task) = input_task {
            task.abort();
        }
        let mut error = if keyed && (status.is_err() || local.is_cancelled()) {
            Some(Error::new(
                ErrorCode::ResultUnknown,
                "keyed execution completion unconfirmed",
            ))
        } else {
            None
        };
        for task in pumps {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => error = Some(e),
                Err(e) => error = Some(e.into()),
            }
        }
        let code = status
            .map(|s| {
                use std::os::unix::process::ExitStatusExt;
                s.code().unwrap_or_else(|| 128 + s.signal().unwrap_or(0))
            })
            .unwrap_or(255);
        {
            let mut s = runner_process.status.lock().unwrap();
            s.exit = Some(code);
            s.finished = Some(Instant::now());
        }
        runner_process.changed.notify_waiters();
        if !detach {
            if let Some(error) = error {
                return Err(error);
            }
            runner_out
                .done(json!({"exit_code":code,"process_id":runner_process.id}))
                .await?;
        }
        Ok(())
    });
    if detach {
        out.done(json!({"process_id":id,"pid":pid,"detached":true}))
            .await
    } else {
        runner.await?
    }
}

pub fn random_id() -> Result<String> {
    use std::io::Read;
    let mut bytes = [0u8; 8];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .or_context(|| "read /dev/urandom for process identity".into())?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
fn dimensions(p: &serde_json::Value) -> Result<(u16, u16)> {
    let rows = if p.get("rows").is_some() {
        number(p, "rows")?
    } else {
        24
    };
    let cols = if p.get("cols").is_some() {
        number(p, "cols")?
    } else {
        80
    };
    ensure!(
        (1..=4096).contains(&rows) && (1..=4096).contains(&cols),
        "invalid terminal dimensions"
    );
    Ok((rows as u16, cols as u16))
}

pub async fn dispatch(agent: &Arc<Agent>, out: &Output) -> Result<()> {
    let p = out.request.params_json();
    if out.request.method == "proc_ps" {
        let records: Vec<_> = agent.processes.lock().await.values().map(|p|json!({"process_id":p.id,"pid":p.pid,"exit_code":p.status.lock().unwrap().exit,"pty":p.pty.is_some()})).collect();
        return out.done(json!({"processes":records})).await;
    }
    let id = string(&p, "process_id")?;
    let process = agent
        .processes
        .lock()
        .await
        .get(id)
        .cloned()
        .ok_or_else(|| Error::new(ErrorCode::NotFound, format!("unknown process {id}")))?;
    match out.request.method.as_str() {
        "proc_kill" => {
            let signal = if p.get("signal").is_some() {
                number(&p, "signal")?
            } else {
                15
            };
            ensure!((1..=64).contains(&signal), "invalid signal");
            process.signal(signal as i32)?;
            out.done(json!({})).await
        }
        "pty_resize" => {
            let master = process.pty.as_ref().ok_or_else(|| {
                Error::new(ErrorCode::Conflict, format!("process {id} has no PTY"))
            })?;
            let (rows, cols) = dimensions(&p)?;
            let size = libc::winsize {
                ws_row: rows,
                ws_col: cols,
                ws_xpixel: 0,
                ws_ypixel: 0,
            };
            if unsafe { libc::ioctl(master.as_raw_fd(), libc::TIOCSWINSZ, &size) } < 0 {
                return Err(Error::from(std::io::Error::last_os_error())
                    .context(format!("resize PTY process {id}")));
            }
            out.done(json!({})).await
        }
        "proc_logs" | "proc_attach" => {
            let mut events = process.events.subscribe();
            let (logs, exit, first, last) = {
                let s = process.status.lock().unwrap();
                (
                    s.logs.iter().cloned().collect::<Vec<_>>(),
                    s.exit,
                    s.logs.front().map(|l| l.sequence).unwrap_or(s.sequence + 1),
                    s.sequence,
                )
            };
            out.event(
                json!({"first_sequence":first,"last_sequence":last,"truncated":first>1}),
                vec![],
            )
            .await?;
            for log in logs {
                out.event(
                    json!({"stream":log.stream,"sequence":log.sequence}),
                    log.data,
                )
                .await?;
            }
            let mut seen = last;
            if out.request.method == "proc_logs" {
                return out.done(json!({"exit_code":exit,"process_id":id})).await;
            }
            loop {
                let notified = process.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let exit = process.status.lock().unwrap().exit;
                if let Some(code) = exit {
                    // Drain all output published before completion.
                    loop {
                        match events.try_recv() {
                            Ok(log) => {
                                if log.sequence > seen {
                                    out.event(
                                        json!({"stream":log.stream,"sequence":log.sequence}),
                                        log.data,
                                    )
                                    .await?;
                                    seen = log.sequence;
                                }
                            }
                            Err(
                                broadcast::error::TryRecvError::Empty
                                | broadcast::error::TryRecvError::Closed,
                            ) => break,
                            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                                bail!(ResourceExhausted => "attach output overflow ({n} chunks); replay proc_logs")
                            }
                        }
                    }
                    return out.done(json!({"exit_code":code,"process_id":id})).await;
                }
                tokio::select! {
                    _ = out.cancel.cancelled() => bail!(Internal => "connection closed"),
                    _ = notified => {},
                    log = events.recv() => {
                        let log = log.map_err(|e| Error::new(ErrorCode::ResourceExhausted, format!("attach output overflow or closed: {e}")))?;
                        if log.sequence > seen { out.event(json!({"stream":log.stream,"sequence":log.sequence}),log.data).await?; seen = log.sequence; }
                    }
                }
            }
        }
        _ => bail!("unknown process method"),
    }
}

// Nonblocking PTY master avoids blocking the async runtime or leaking blocked
// filesystem worker threads when a terminal is idle.
pub struct AsyncPty(tokio::io::unix::AsyncFd<OwnedFd>);
impl AsyncPty {
    fn new(fd: OwnedFd) -> Result<Self> {
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
        if flags < 0
            || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
        {
            return Err(Error::from(std::io::Error::last_os_error()).context("set PTY O_NONBLOCK"));
        }
        // OwnedFd cannot change descriptor while registered.
        Ok(Self(
            unsafe { tokio::io::unix::AsyncFd::register(fd) }
                .map_err(|e| Error::new(ErrorCode::Internal, format!("register PTY: {e}")))?,
        ))
    }
}
impl tokio::io::AsyncRead for AsyncPty {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            let mut ready = std::task::ready!(self.0.poll_read_ready(cx))?;
            let target = buf.initialize_unfilled();
            match ready.try_io(|fd| {
                let n =
                    unsafe { libc::read(fd.as_raw_fd(), target.as_mut_ptr().cast(), target.len()) };
                if n < 0 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            }) {
                Ok(Ok(n)) => {
                    buf.advance(n);
                    return std::task::Poll::Ready(Ok(()));
                }
                Ok(Err(e)) => return std::task::Poll::Ready(Err(e)),
                Err(_) => continue,
            }
        }
    }
}
impl tokio::io::AsyncWrite for AsyncPty {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        loop {
            let mut ready = std::task::ready!(self.0.poll_write_ready(cx))?;
            match ready.try_io(|fd| {
                let n = unsafe { libc::write(fd.as_raw_fd(), buf.as_ptr().cast(), buf.len()) };
                if n < 0 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            }) {
                Ok(result) => return std::task::Poll::Ready(result),
                Err(_) => continue,
            }
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
