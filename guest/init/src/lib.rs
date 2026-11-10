macro_rules! bail {
    ($($arg:tt)*) => { return Err(InitError::new(format!($($arg)*))) };
}
macro_rules! ensure {
    ($condition:expr, $($arg:tt)*) => {
        if !$condition { bail!($($arg)*); }
    };
}
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CString,
    fs,
    os::unix::{fs::MetadataExt, process::CommandExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

pub type Result<T> = std::result::Result<T, InitError>;

#[derive(Debug)]
pub struct InitError {
    frames: Vec<(String, &'static std::panic::Location<'static>)>,
}
impl InitError {
    #[track_caller]
    fn new(message: String) -> Self {
        Self {
            frames: vec![(message, std::panic::Location::caller())],
        }
    }
    #[track_caller]
    fn context(mut self, message: String) -> Self {
        self.frames.push((message, std::panic::Location::caller()));
        self
    }
}
impl std::fmt::Display for InitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, (message, location)) in self.frames.iter().rev().enumerate() {
            if index != 0 {
                write!(f, ": ")?;
            }
            write!(f, "{message} at {location}")?;
        }
        Ok(())
    }
}
impl std::error::Error for InitError {}
macro_rules! init_error_from {
    ($type:ty) => {
        impl From<$type> for InitError {
            #[track_caller]
            fn from(error: $type) -> Self {
                Self::new(error.to_string())
            }
        }
    };
}
init_error_from!(std::io::Error);
init_error_from!(std::ffi::NulError);
init_error_from!(std::num::ParseIntError);
init_error_from!(std::net::AddrParseError);

trait InitContext<T> {
    #[track_caller]
    fn or_context(self, context: impl FnOnce() -> String) -> Result<T>;
}
impl<T, E: Into<InitError>> InitContext<T> for std::result::Result<T, E> {
    #[track_caller]
    fn or_context(self, context: impl FnOnce() -> String) -> Result<T> {
        match self {
            Ok(value) => Ok(value),
            Err(error) => Err(error.into().context(context())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Boot {
    pub lower_serial: String,
    pub upper_serial: String,
    pub data_serials: Vec<String>,
    pub dns: Vec<String>,
    pub timeout: Duration,
}
impl Boot {
    pub fn parse(cmdline: &str) -> Result<Self> {
        let mut args = BTreeMap::new();
        for field in cmdline.split_whitespace() {
            if let Some((key, value)) = field.split_once('=')
                && key.starts_with("sandbox.")
            {
                ensure!(
                    args.insert(key, value).is_none(),
                    "duplicate boot parameter {key}"
                );
            }
        }
        let lower_serial = args
            .get("sandbox.lower_serial")
            .unwrap_or(&"sandbox-root")
            .to_string();
        let upper_serial = args
            .get("sandbox.upper_serial")
            .unwrap_or(&"sandbox-write")
            .to_string();
        let data_serials = args
            .get("sandbox.data_serials")
            .filter(|s| !s.is_empty())
            .map(|s| s.split(',').map(str::to_string).collect())
            .unwrap_or_else(Vec::new);
        let dns = args
            .get("sandbox.dns")
            .filter(|s| !s.is_empty())
            .map(|s| s.split(',').map(str::to_string).collect())
            .unwrap_or_else(Vec::new);
        let timeout = args
            .get("sandbox.device_timeout_ms")
            .map(|s| s.parse::<u64>())
            .transpose()
            .or_context(|| "parse sandbox.device_timeout_ms".into())?
            .unwrap_or(10_000);
        ensure!(
            (1..=120_000).contains(&timeout),
            "invalid block device timeout"
        );
        let boot = Self {
            lower_serial,
            upper_serial,
            data_serials,
            dns,
            timeout: Duration::from_millis(timeout),
        };
        let mut serials = BTreeSet::new();
        for serial in boot.serials() {
            ensure!(
                !serial.is_empty()
                    && serial.len() <= 20
                    && serial
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                "invalid virtio serial {serial}"
            );
            ensure!(serials.insert(serial), "duplicate serial role");
        }
        ensure!(boot.data_serials.len() <= 16, "too many EROFS data devices");
        for dns in &boot.dns {
            dns.parse::<std::net::Ipv4Addr>()
                .or_context(|| format!("parse sandbox.dns address {dns:?}"))?;
        }
        Ok(boot)
    }
    fn serials(&self) -> Vec<&str> {
        std::iter::once(self.lower_serial.as_str())
            .chain(std::iter::once(self.upper_serial.as_str()))
            .chain(self.data_serials.iter().map(String::as_str))
            .collect()
    }
}

// Discover by identity rather than vdX enumeration. Any unexpected nonempty
// virtio serial is a contract failure; no best-effort role guessing is allowed.
pub fn discover(sys_block: &Path, boot: &Boot) -> Result<Option<BTreeMap<String, PathBuf>>> {
    let expected: BTreeSet<_> = boot.serials().into_iter().collect();
    let mut found = BTreeMap::new();
    for entry in fs::read_dir(sys_block)
        .or_context(|| format!("discover block roles in {}", sys_block.display()))?
    {
        let entry =
            entry.or_context(|| format!("enumerate block device in {}", sys_block.display()))?;
        let name = entry.file_name();
        let serial = match fs::read_to_string(entry.path().join("serial")) {
            Ok(serial) => serial,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(InitError::from(error).context(format!(
                    "read block serial {}",
                    entry.path().join("serial").display()
                )));
            }
        };
        let serial = serial.trim();
        ensure!(
            !serial.is_empty(),
            "block device {} has no serial identity",
            name.to_string_lossy()
        );
        ensure!(
            expected.contains(serial),
            "unexpected block serial {serial}"
        );
        let ro = fs::read_to_string(entry.path().join("ro")).or_context(|| {
            format!(
                "read block role {serial} read-only flag at {}",
                entry.path().display()
            )
        })?;
        let expected_ro = if serial == boot.upper_serial {
            "0"
        } else {
            "1"
        };
        ensure!(
            ro.trim() == expected_ro,
            "block role {serial} has incorrect read-only flag"
        );
        ensure!(
            found
                .insert(serial.to_string(), Path::new("/dev").join(name))
                .is_none(),
            "duplicate block serial {serial}"
        );
    }
    if found.len() == expected.len() {
        Ok(Some(found))
    } else {
        Ok(None)
    }
}
fn c(s: &str) -> Result<CString> {
    CString::new(s).or_context(|| format!("encode syscall string {s:?}"))
}
fn mount(
    source: Option<&str>,
    target: &str,
    kind: Option<&str>,
    flags: libc::c_ulong,
    data: Option<&str>,
) -> Result<()> {
    let context = || {
        format!("mount {source:?} -> {target} (fstype={kind:?}, flags={flags:#x}, data={data:?})")
    };
    let source = source.map(c).transpose()?;
    let target = c(target)?;
    let kind = kind.map(c).transpose()?;
    let data = data.map(c).transpose()?;
    let ptr = |v: &Option<CString>| v.as_ref().map(|s| s.as_ptr()).unwrap_or(std::ptr::null());
    if unsafe {
        libc::mount(
            ptr(&source),
            target.as_ptr(),
            ptr(&kind),
            flags,
            ptr(&data).cast(),
        )
    } < 0
    {
        return Err(InitError::from(std::io::Error::last_os_error()).context(context()));
    }
    Ok(())
}
fn dir(path: &str) -> Result<()> {
    fs::create_dir_all(path).or_context(|| format!("create mount target {path}"))?;
    ensure!(
        fs::symlink_metadata(path)
            .or_context(|| format!("inspect mount target {path}"))?
            .is_dir(),
        "mount target {path} is not a directory"
    );
    Ok(())
}
fn loopback() -> Result<()> {
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(InitError::from(std::io::Error::last_os_error())
            .context("open loopback ioctl socket".into()));
    }
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut request: libc::ifreq = unsafe { std::mem::zeroed() };
    request.ifr_name[0] = b'l' as _;
    request.ifr_name[1] = b'o' as _;
    if unsafe { libc::ioctl(fd.as_raw_fd(), libc::SIOCGIFFLAGS as _, &mut request) } < 0 {
        return Err(
            InitError::from(std::io::Error::last_os_error()).context("SIOCGIFFLAGS lo".into())
        );
    }
    unsafe {
        request.ifr_ifru.ifru_flags |= libc::IFF_UP as i16;
    }
    if unsafe { libc::ioctl(fd.as_raw_fd(), libc::SIOCSIFFLAGS as _, &request) } < 0 {
        return Err(InitError::from(std::io::Error::last_os_error())
            .context("SIOCSIFFLAGS lo IFF_UP".into()));
    }
    Ok(())
}

pub fn boot() -> Result<()> {
    ensure!(
        unsafe { libc::getpid() } == 1,
        "sandbox-init must run as PID 1"
    );
    let mut rootfs: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c("/")?.as_ptr(), &mut rootfs) } < 0 {
        return Err(
            InitError::from(std::io::Error::last_os_error()).context("statfs old root /".into())
        );
    }
    ensure!(
        [0x01021994, 0x858458f6].contains(&(rootfs.f_type as u64)),
        "old root is not initramfs tmpfs/ramfs"
    );
    for path in ["/dev", "/proc", "/sys", "/run"] {
        dir(path)?;
    }
    mount(
        Some("devtmpfs"),
        "/dev",
        Some("devtmpfs"),
        libc::MS_NOSUID,
        Some("mode=0755"),
    )?;
    mount(
        Some("proc"),
        "/proc",
        Some("proc"),
        libc::MS_NOSUID | libc::MS_NOEXEC | libc::MS_NODEV,
        None,
    )?;
    mount(
        Some("sysfs"),
        "/sys",
        Some("sysfs"),
        libc::MS_NOSUID | libc::MS_NOEXEC | libc::MS_NODEV,
        None,
    )?;
    mount(
        Some("tmpfs"),
        "/run",
        Some("tmpfs"),
        libc::MS_NOSUID | libc::MS_NODEV,
        Some("mode=0755"),
    )?;
    let config = Boot::parse(
        &fs::read_to_string("/proc/cmdline").or_context(|| "read /proc/cmdline".into())?,
    )?;
    let start = Instant::now();
    let devices = loop {
        if let Some(devices) = discover(Path::new("/sys/block"), &config)? {
            break devices;
        }
        ensure!(
            start.elapsed() < config.timeout,
            "timed out waiting for block serial roles"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    for path in [
        "/run/sandbox-boot/lower",
        "/run/sandbox-boot/writable",
        "/newroot",
    ] {
        dir(path)?;
    }
    let mut options = Vec::new();
    for serial in &config.data_serials {
        options.push(format!("device={}", devices[serial].display()));
    }
    let options = if options.is_empty() {
        None
    } else {
        Some(options.join(","))
    };
    mount(
        Some(devices[&config.lower_serial].to_str().unwrap()),
        "/run/sandbox-boot/lower",
        Some("erofs"),
        libc::MS_RDONLY,
        options.as_deref(),
    )?;
    mount(
        Some(devices[&config.upper_serial].to_str().unwrap()),
        "/run/sandbox-boot/writable",
        Some("ext4"),
        0,
        None,
    )?;
    dir("/run/sandbox-boot/writable/upper")?;
    dir("/run/sandbox-boot/writable/work")?;
    let upper = fs::metadata("/run/sandbox-boot/writable/upper")
        .or_context(|| "stat overlay upper /run/sandbox-boot/writable/upper".into())?;
    let work = fs::metadata("/run/sandbox-boot/writable/work")
        .or_context(|| "stat overlay work /run/sandbox-boot/writable/work".into())?;
    ensure!(
        upper.dev() == work.dev(),
        "upper and work are on different filesystems"
    );
    mount(
        Some("overlay"),
        "/newroot",
        Some("overlay"),
        0,
        Some(
            "lowerdir=/run/sandbox-boot/lower,upperdir=/run/sandbox-boot/writable/upper,workdir=/run/sandbox-boot/writable/work",
        ),
    )?;
    for name in ["dev", "proc", "sys", "run"] {
        dir(&format!("/newroot/{name}"))?;
    }
    ensure!(
        fs::metadata("/newroot/usr/lib/systemd/systemd").is_ok(),
        "image lacks /usr/lib/systemd/systemd"
    );
    if !config.dns.is_empty() {
        // The supported image uses a regular resolver file; reject symlinked
        // resolv.conf rather than following it into the initramfs view.
        let resolver = Path::new("/newroot/etc/resolv.conf");
        if let Ok(meta) = fs::symlink_metadata(resolver) {
            ensure!(
                !meta.file_type().is_symlink(),
                "image resolv.conf must be a regular file"
            );
        }
        fs::write(
            resolver,
            config
                .dns
                .iter()
                .map(|ip| format!("nameserver {ip}\n"))
                .collect::<String>(),
        )
        .or_context(|| format!("write DNS configuration {}", resolver.display()))?;
    }
    loopback()?;
    for name in ["dev", "proc", "sys", "run"] {
        mount(
            Some(&format!("/{name}")),
            &format!("/newroot/{name}"),
            None,
            libc::MS_MOVE,
            None,
        )?;
    }
    switch_root(Path::new("/newroot"))
}

fn clear_old_root(path: &Path, device: u64) -> Result<()> {
    for entry in
        fs::read_dir(path).or_context(|| format!("clear old root directory {}", path.display()))?
    {
        let entry = entry.or_context(|| format!("enumerate old root {}", path.display()))?;
        let meta = fs::symlink_metadata(entry.path())
            .or_context(|| format!("stat old root entry {}", entry.path().display()))?;
        if meta.dev() != device {
            continue;
        }
        if meta.is_dir() {
            clear_old_root(&entry.path(), device)?;
            fs::remove_dir(entry.path())
                .or_context(|| format!("remove old root directory {}", entry.path().display()))?;
        } else {
            fs::remove_file(entry.path())
                .or_context(|| format!("unlink old root entry {}", entry.path().display()))?;
        }
    }
    Ok(())
}
fn switch_root(newroot: &Path) -> Result<()> {
    let old_device = fs::metadata("/")
        .or_context(|| "stat old root /".into())?
        .dev();
    ensure!(
        fs::metadata(newroot)
            .or_context(|| format!("stat new root {}", newroot.display()))?
            .dev()
            != old_device,
        "new root is not a separate mount"
    );
    std::env::set_current_dir(newroot)
        .or_context(|| format!("chdir new root {}", newroot.display()))?;
    // Release all initramfs files while still able to identify the old mount.
    // Nested mounts, including the overlay root and moved /run, are skipped.
    clear_old_root(Path::new("/"), old_device)?;
    mount(Some("."), "/", None, libc::MS_MOVE, None)?;
    if unsafe { libc::chroot(c(".")?.as_ptr()) } < 0 {
        return Err(InitError::from(std::io::Error::last_os_error())
            .context(format!("chroot . from {}", newroot.display())));
    }
    std::env::set_current_dir("/").or_context(|| "chdir / after chroot".into())?;
    let error = std::process::Command::new("/usr/lib/systemd/systemd")
        .arg("--system")
        .env_clear()
        .env("PATH", "/usr/sbin:/usr/bin:/sbin:/bin")
        .exec();
    bail!("exec /usr/lib/systemd/systemd --system failed: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roles_and_order() {
        let boot = Boot::parse("sandbox.data_serials=layer-a,layer-b sandbox.dns=1.1.1.1,8.8.8.8")
            .unwrap();
        assert_eq!(boot.data_serials, vec!["layer-a", "layer-b"]);
        assert!(Boot::parse("sandbox.lower_serial=same sandbox.upper_serial=same").is_err());
        for (cmdline, target) in [
            ("sandbox.dns=evil", "sandbox.dns"),
            (
                "sandbox.device_timeout_ms=invalid",
                "sandbox.device_timeout_ms",
            ),
        ] {
            let error = Boot::parse(cmdline).unwrap_err().to_string();
            assert!(error.contains(target), "{error}");
            assert!(error.contains("guest/init/src/lib.rs:"), "{error}");
        }
        let tmp = tempfile::tempdir().unwrap();
        for (name, serial) in [
            ("vdb", "sandbox-root"),
            ("vda", "sandbox-write"),
            ("vdz", "layer-b"),
            ("vdc", "layer-a"),
        ] {
            fs::create_dir(tmp.path().join(name)).unwrap();
            fs::write(tmp.path().join(name).join("serial"), serial).unwrap();
            fs::write(
                tmp.path().join(name).join("ro"),
                if serial == "sandbox-write" { "0" } else { "1" },
            )
            .unwrap();
        }
        let found = discover(tmp.path(), &boot).unwrap().unwrap();
        assert_eq!(found["sandbox-root"], PathBuf::from("/dev/vdb"));
        assert_eq!(found["sandbox-write"], PathBuf::from("/dev/vda"));
        fs::write(tmp.path().join("vda/serial"), "bad").unwrap();
        let error = discover(tmp.path(), &boot).unwrap_err().to_string();
        assert!(error.contains("unexpected block serial bad"), "{error}");
        assert!(error.contains("guest/init/src/lib.rs:"), "{error}");
        std::fs::remove_file(tmp.path().join("vda/serial")).unwrap();
        std::fs::remove_file(tmp.path().join("vdb/ro")).unwrap();
        let error = discover(tmp.path(), &boot).unwrap_err().to_string();
        assert!(
            error.contains("sandbox-root") && error.contains("vdb"),
            "{error}"
        );
        assert!(error.contains("guest/init/src/lib.rs:"), "{error}");
    }
    #[test]
    fn requires_pid_one() {
        let error = boot().unwrap_err().to_string();
        assert!(error.contains("PID 1"), "{error}");
        assert!(error.contains("guest/init/src/lib.rs:"), "{error}");
    }
}
