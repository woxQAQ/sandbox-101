use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use sandbox_agent::{
    Agent, Config,
    server::{Portal, serve_connection},
    wire::{CHUNK, Frame, read_frame, write_frame},
};
use serde_json::json;
use std::{
    ffi::CString,
    fs::Permissions,
    io::Read,
    os::unix::{ffi::OsStrExt, fs::MetadataExt, fs::PermissionsExt, fs::symlink},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::net::UnixStream;

fn setup() -> (tempfile::TempDir, Arc<Agent>) {
    let tmp = tempfile::tempdir().unwrap();
    let agent = Agent::new(Config {
        env_file: tmp.path().join("sandbox.env"),
        machine_id: tmp.path().join("machine-id"),
        ledger_dir: tmp.path().join("ledger"),
        ledger_max_entries: sandbox_agent::ledger::MAX_ENTRIES,
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
    })
    .unwrap();
    (tmp, agent)
}
fn connect(agent: Arc<Agent>) -> UnixStream {
    let (client, server) = UnixStream::pair().unwrap();
    tokio::spawn(async move {
        let _ = serve_connection(agent, server, Portal::Workload).await;
    });
    client
}
fn request(method: &str, path: &Path) -> Frame {
    Frame {
        id: "tree".into(),
        method: method.into(),
        ..Default::default()
    }
    .reply(json!({"path":path}), vec![], true)
}
async fn reply(stream: &mut UnixStream) -> (Vec<u8>, String) {
    let mut data = Vec::new();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(10), read_frame(stream))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(frame.data.len() <= CHUNK);
        data.extend_from_slice(&frame.data);
        if frame.eof {
            if !frame.error.is_empty() {
                assert!(frame.params_json()["error_code"].is_string(), "{frame:?}");
                assert!(frame.error.contains("guest/agent/src/"), "{frame:?}");
            }
            return (data, frame.error);
        }
    }
}
async fn pull(agent: &Arc<Agent>, path: &Path) -> (Vec<u8>, String) {
    let mut stream = connect(agent.clone());
    write_frame(&mut stream, &request("tree_pull", path))
        .await
        .unwrap();
    reply(&mut stream).await
}
async fn push(agent: &Arc<Agent>, path: &Path, archive: &[u8]) -> String {
    let mut stream = connect(agent.clone());
    let mut initial = request("tree_push", path);
    initial.eof = archive.is_empty();
    write_frame(&mut stream, &initial).await.unwrap();
    for (i, chunk) in archive.chunks(CHUNK).enumerate() {
        write_frame(
            &mut stream,
            &Frame {
                id: initial.id.clone(),
                data: chunk.to_vec(),
                eof: (i + 1) * CHUNK >= archive.len(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    reply(&mut stream).await.1
}
fn times(path: &Path, seconds: i64, nanos: i64) {
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let times = [libc::timespec {
        tv_sec: seconds,
        tv_nsec: nanos,
    }; 2];
    assert_eq!(
        unsafe {
            libc::utimensat(
                libc::AT_FDCWD,
                path.as_ptr(),
                times.as_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        },
        0,
        "{}",
        std::io::Error::last_os_error()
    );
}
fn set_xattr(path: &Path, name: &str, value: &[u8]) {
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let name = CString::new(name).unwrap();
    assert_eq!(
        unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
            )
        },
        0,
        "{}",
        std::io::Error::last_os_error()
    );
}
fn xattr(path: &Path, name: &str) -> Vec<u8> {
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let name = CString::new(name).unwrap();
    let mut value = vec![0u8; 65536];
    let n = unsafe {
        libc::getxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_mut_ptr().cast(),
            value.len(),
        )
    };
    assert!(n >= 0, "{name:?}: {}", std::io::Error::last_os_error());
    value.truncate(n as usize);
    value
}
fn xattrs(path: &Path) -> Vec<String> {
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let mut names = vec![0u8; 65536];
    let n = unsafe { libc::listxattr(path.as_ptr(), names.as_mut_ptr().cast(), names.len()) };
    assert!(n >= 0, "{}", std::io::Error::last_os_error());
    names[..n as usize]
        .split(|b| *b == 0)
        .filter(|n| !n.is_empty())
        .map(|n| String::from_utf8(n.to_vec()).unwrap())
        .collect()
}
fn assert_metadata(a: &Path, b: &Path) {
    let a = std::fs::symlink_metadata(a).unwrap();
    let b = std::fs::symlink_metadata(b).unwrap();
    assert_eq!(a.mode() & 0o7777, b.mode() & 0o7777);
    assert_eq!((a.mtime(), a.mtime_nsec()), (b.mtime(), b.mtime_nsec()));
    assert_eq!((b.uid(), b.gid()), unsafe {
        (libc::geteuid(), libc::getegid())
    });
}
fn same_inode(a: &Path, b: &Path) -> bool {
    let a = std::fs::metadata(a).unwrap();
    let b = std::fs::metadata(b).unwrap();
    (a.dev(), a.ino()) == (b.dev(), b.ino())
}
fn header(kind: tar::EntryType, size: u64) -> tar::Header {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(kind);
    header.set_size(size);
    header.set_mode(0o640);
    header.set_uid(12345);
    header.set_gid(54321);
    header.set_mtime(123);
    header.set_cksum();
    header
}
fn archive(build: impl FnOnce(&mut tar::Builder<Vec<u8>>)) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    build(&mut builder);
    builder.into_inner().unwrap()
}
fn regular(builder: &mut tar::Builder<Vec<u8>>, path: &str, data: &[u8]) {
    builder
        .append_data(
            &mut header(tar::EntryType::Regular, data.len() as u64),
            path,
            data,
        )
        .unwrap();
}
fn link(builder: &mut tar::Builder<Vec<u8>>, path: &str, target: &str, hard: bool) {
    builder
        .append_link(
            &mut header(
                if hard {
                    tar::EntryType::Link
                } else {
                    tar::EntryType::Symlink
                },
                0,
            ),
            path,
            target,
        )
        .unwrap();
}

#[tokio::test]
async fn tree_roundtrip_preserves_metadata_links_and_binary_user_xattrs() {
    let (tmp, agent) = setup();
    let source = tmp.path().join("source");
    std::fs::create_dir_all(source.join("dir")).unwrap();
    let data = vec![b'x'; CHUNK * 3 + 7];
    std::fs::write(source.join("dir/file"), &data).unwrap();
    std::fs::hard_link(source.join("dir/file"), source.join("alias")).unwrap();
    symlink("dir/file", source.join("link")).unwrap();
    symlink("../alias", source.join("dir/relative")).unwrap();
    symlink("missing", source.join("dangling")).unwrap();
    let long_name = "long".repeat(50);
    std::fs::write(source.join(&long_name), b"long GNU name").unwrap();
    std::fs::set_permissions(source.join("dir/file"), Permissions::from_mode(0o6751)).unwrap();
    std::fs::set_permissions(source.join("dir"), Permissions::from_mode(0o1750)).unwrap();
    std::fs::set_permissions(&source, Permissions::from_mode(0o750)).unwrap();
    set_xattr(
        &source.join("dir/file"),
        "user.binary % name",
        b"a\0b\n\xff",
    );
    set_xattr(&source.join("dir"), "user.directory", b"directory");
    set_xattr(&source, "user.root", b"root");
    times(&source.join("dir/file"), -1, 900_000_001);
    times(&source.join("link"), 1_700_000_002, 123_456_789);
    times(&source.join("dir"), 1_700_000_003, 987_654_321);
    times(&source, 1_700_000_004, 99);
    let (tar, error) = pull(&agent, &source).await;
    assert!(error.is_empty(), "{error}");
    let destination = tmp.path().join("destination");
    assert_eq!(push(&agent, &destination, &tar).await, "");
    assert_eq!(std::fs::read(destination.join("dir/file")).unwrap(), data);
    assert!(same_inode(
        &destination.join("dir/file"),
        &destination.join("alias")
    ));
    for name in ["", "dir", "dir/file", "link"] {
        assert_metadata(&source.join(name), &destination.join(name));
    }
    assert_eq!(
        std::fs::read_link(destination.join("dir/relative")).unwrap(),
        Path::new("../alias")
    );
    assert_eq!(
        std::fs::read_link(destination.join("dangling")).unwrap(),
        Path::new("missing")
    );
    assert_eq!(
        std::fs::read(destination.join(long_name)).unwrap(),
        b"long GNU name"
    );
    assert_eq!(
        xattr(&destination.join("dir/file"), "user.binary % name"),
        b"a\0b\n\xff"
    );
    assert_eq!(
        xattr(&destination.join("dir"), "user.directory"),
        b"directory"
    );
    assert_eq!(xattr(&destination, "user.root"), b"root");
    for entry in tar::Archive::new(tar.as_slice()).entries().unwrap() {
        let entry = entry.unwrap();
        assert_eq!(entry.header().uid().unwrap(), unsafe { libc::geteuid() }
            as u64);
        assert_eq!(entry.header().gid().unwrap(), unsafe { libc::getegid() }
            as u64);
    }
}

#[tokio::test]
async fn merging_keeps_existing_hardlinks_and_replacing_one_name_severs_only_that_name() {
    let (tmp, agent) = setup();
    let target = tmp.path().join("target");
    std::fs::create_dir(&target).unwrap();
    std::fs::write(target.join("original"), b"old").unwrap();
    std::fs::hard_link(target.join("original"), target.join("alias")).unwrap();
    symlink("alias", target.join("link")).unwrap();
    set_xattr(&target.join("alias"), "user.kept", b"yes\n\0");
    times(&target.join("alias"), 10, 20);
    let first = archive(|b| regular(b, "extra", b"first"));
    assert_eq!(push(&agent, &target, &first).await, "");
    assert!(same_inode(&target.join("original"), &target.join("alias")));
    let second = archive(|b| regular(b, "original", b"new"));
    assert_eq!(push(&agent, &target, &second).await, "");
    assert!(!same_inode(&target.join("original"), &target.join("alias")));
    assert_eq!(std::fs::read(target.join("alias")).unwrap(), b"old");
    assert_eq!(std::fs::read(target.join("link")).unwrap(), b"old");
    assert_eq!(xattr(&target.join("alias"), "user.kept"), b"yes\n\0");
    let m = std::fs::metadata(target.join("alias")).unwrap();
    assert_eq!((m.mtime(), m.mtime_nsec()), (10, 20));
    assert_eq!(std::fs::read(target.join("extra")).unwrap(), b"first");
    let forward = archive(|b| {
        link(b, "first", "second", true);
        link(b, "second", "third", true);
        regular(b, "third", b"forward");
    });
    assert_eq!(push(&agent, &target, &forward).await, "");
    assert!(same_inode(&target.join("first"), &target.join("third")));
    assert!(same_inode(&target.join("second"), &target.join("third")));
    assert_eq!(
        std::fs::metadata(target.join("third")).unwrap().uid(),
        unsafe { libc::geteuid() }
    );
}

#[tokio::test]
async fn traversal_links_special_files_and_conflicting_metadata_roll_back() {
    let (tmp, agent) = setup();
    let target = tmp.path().join("target");
    std::fs::create_dir(&target).unwrap();
    std::fs::write(target.join("kept"), b"kept").unwrap();
    let outside = tmp.path().join("outside");
    std::fs::write(&outside, b"untouched").unwrap();
    let outside_name = outside.to_str().unwrap();
    let attacks = vec![
        archive(|b| {
            link(b, "escape", outside_name, false);
            regular(b, "escape", b"overwrite");
        }),
        archive(|b| {
            link(b, "escape", "../outside", false);
        }),
        archive(|b| {
            link(b, "escape", "../outside", true);
        }),
        archive(|b| {
            link(b, "escape", outside_name, true);
        }),
        archive(|b| {
            link(b, "dangling", "kept", false);
            link(b, "hard", "dangling", true);
        }),
        archive(|b| {
            link(b, "loop1", "loop2", true);
            link(b, "loop2", "loop1", true);
        }),
        archive(|b| {
            link(b, "loop1", "loop2", false);
            link(b, "loop2", "loop1", false);
        }),
        archive(|b| {
            link(b, "jump", ".", false);
            regular(b, "jump/child", b"bad");
        }),
        archive(|b| {
            b.append_data(&mut header(tar::EntryType::Directory, 0), "dir", &b""[..])
                .unwrap();
            link(b, "dir/up", "..", false);
            link(b, "chain", "dir/up/../outside", false);
        }),
        archive(|b| {
            b.append_data(&mut header(tar::EntryType::Fifo, 0), "fifo", &b""[..])
                .unwrap();
        }),
        archive(|b| {
            b.append_pax_extensions([("path", &b"../outside"[..])])
                .unwrap();
            regular(b, "innocent", b"bad");
        }),
        archive(|b| {
            let mut h = header(tar::EntryType::Regular, 3);
            h.as_mut_bytes()[..14].copy_from_slice(b"nested//escape");
            h.set_cksum();
            b.append(&h, &b"bad"[..]).unwrap();
        }),
        archive(|b| {
            b.append_pax_extensions([("LIBARCHIVE.xattr.user.test", &b"%%%"[..])])
                .unwrap();
            regular(b, "metadata", b"bad");
        }),
        archive(|b| {
            b.append_pax_extensions([("LIBARCHIVE.xattr.user.bad%zz", &b"eA"[..])])
                .unwrap();
            regular(b, "metadata", b"bad");
        }),
        archive(|b| {
            b.append_pax_extensions([("SCHILY.xattr.security.capability", &b"bad"[..])])
                .unwrap();
            regular(b, "metadata", b"bad");
        }),
        archive(|b| {
            b.append_pax_extensions([
                ("SCHILY.xattr.user.test", &b"one"[..]),
                ("SCHILY.xattr.user.test", &b"two"[..]),
            ])
            .unwrap();
            regular(b, "metadata", b"bad");
        }),
        archive(|b| {
            b.append_pax_extensions([
                ("SCHILY.xattr.user.test", &b"one"[..]),
                ("LIBARCHIVE.xattr.user.test", &b"dHdv"[..]),
            ])
            .unwrap();
            regular(b, "metadata", b"bad");
        }),
        archive(|b| {
            b.append_pax_extensions([(
                "LIBARCHIVE.xattr.user.huge",
                STANDARD_NO_PAD.encode(vec![b'a'; 65537]).as_bytes(),
            )])
            .unwrap();
            regular(b, "metadata", b"bad");
        }),
        archive(|b| {
            b.append_pax_extensions([("comment", vec![b'x'; 128 * 1024].as_slice())])
                .unwrap();
            regular(b, "metadata", b"bad");
        }),
        archive(|b| {
            regular(b, "duplicate", b"one");
            regular(b, "duplicate", b"two");
        }),
    ];
    for (i, tar) in attacks.iter().enumerate() {
        let error = push(&agent, &target, tar).await;
        assert!(!error.is_empty(), "attack {i} was accepted");
        assert_eq!(std::fs::read(target.join("kept")).unwrap(), b"kept");
        assert_eq!(std::fs::read(&outside).unwrap(), b"untouched");
        assert_eq!(
            std::fs::read_dir(&target).unwrap().count(),
            1,
            "attack {i}: {error}"
        );
        assert_eq!(
            std::fs::read_dir(tmp.path()).unwrap().count(),
            3,
            "staging leak after attack {i}: {error}"
        );
    }
}

#[tokio::test]
async fn interrupted_and_truncated_transfers_never_publish_and_remove_temporary_files() {
    let (tmp, agent) = setup();
    let target = tmp.path().join("target");
    std::fs::create_dir(&target).unwrap();
    std::fs::write(target.join("kept"), b"kept").unwrap();
    let tar = archive(|b| regular(b, "added", b"complete payload"));
    let mut stream = connect(agent.clone());
    let mut first = request("tree_push", &target);
    first.eof = false;
    first.data = tar.clone();
    write_frame(&mut stream, &first).await.unwrap();
    // Wait until the worker is admitted; disconnect with a complete tar but no
    // protocol EOF, which must still leave the destination untouched.
    tokio::time::timeout(Duration::from_secs(5), async {
        while agent.requests.available_permits() == 128 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(stream);
    tokio::time::timeout(Duration::from_secs(5), async {
        while agent.requests.available_permits() != 128 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read(target.join("kept")).unwrap(), b"kept");
    assert!(!target.join("added").exists());
    assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 2);
    let mut truncated = archive(|b| {
        regular(b, "first", b"first");
        regular(b, "second", &[b'x'; 1024]);
    });
    truncated.truncate(1024 + 512 + 10);
    assert!(!push(&agent, &target, &truncated).await.is_empty());
    assert!(!target.join("first").exists());
    let absent = tmp.path().join("absent");
    assert!(!push(&agent, &absent, &truncated).await.is_empty());
    assert!(!absent.exists());
    assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 2);
}

#[tokio::test]
async fn expanded_data_path_depth_and_aggregate_metadata_limits_roll_back() {
    let (tmp, agent) = setup();
    let target = tmp.path().join("target");
    std::fs::create_dir(&target).unwrap();
    std::fs::write(target.join("kept"), b"kept").unwrap();
    let oversized = archive(|b| {
        b.append_data(
            &mut header(tar::EntryType::Regular, 1024 * 1024 * 1024 + 1),
            "huge",
            &b""[..],
        )
        .unwrap();
    });
    assert!(
        push(&agent, &target, &oversized)
            .await
            .contains("expanded tree exceeds")
    );
    let too_deep = archive(|b| regular(b, &vec!["d"; 65].join("/"), b"x"));
    assert!(
        push(&agent, &target, &too_deep)
            .await
            .contains("tree depth limit")
    );
    let metadata = archive(|b| {
        let comment = vec![b'a'; 100 * 1024];
        for i in 0..170 {
            b.append_pax_extensions([("comment", comment.as_slice())])
                .unwrap();
            regular(b, &format!("file-{i}"), b"");
        }
    });
    assert!(
        push(&agent, &target, &metadata)
            .await
            .contains("tree metadata limit")
    );
    assert_eq!(std::fs::read(target.join("kept")).unwrap(), b"kept");
    assert_eq!(std::fs::read_dir(&target).unwrap().count(), 1);
    assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 2);
}

#[tokio::test]
async fn zero_permissions_hardlinks_and_zero_mode_directories_commit_without_reopening() {
    let (tmp, agent) = setup();
    let target = tmp.path().join("target");
    let tar = archive(|b| {
        let mut h = header(tar::EntryType::Directory, 0);
        h.set_mode(0);
        b.append_data(&mut h, "restricted", &b""[..]).unwrap();
        let mut h = header(tar::EntryType::Regular, 1);
        h.set_mode(0);
        b.append_data(&mut h, "first", &b"x"[..]).unwrap();
        link(b, "second", "first", true);
        regular(b, "restricted/leaf", b"inside");
    });
    assert_eq!(push(&agent, &target, &tar).await, "");
    assert!(same_inode(&target.join("first"), &target.join("second")));
    assert_eq!(
        std::fs::metadata(target.join("first")).unwrap().mode() & 0o7777,
        0
    );
    assert_eq!(
        std::fs::metadata(target.join("restricted")).unwrap().mode() & 0o7777,
        0
    );
    std::fs::set_permissions(target.join("restricted"), Permissions::from_mode(0o700)).unwrap();
    assert_eq!(
        std::fs::read(target.join("restricted/leaf")).unwrap(),
        b"inside"
    );
}

#[tokio::test]
async fn pulls_omit_private_ledger_and_aliases_and_reject_outside_links() {
    let (tmp, agent) = setup();
    std::fs::write(tmp.path().join("public"), b"visible").unwrap();
    symlink(
        agent.ledger.directory.join("exec.sqlite3"),
        tmp.path().join("alias"),
    )
    .unwrap();
    let (tar, error) = pull(&agent, tmp.path()).await;
    assert!(error.is_empty(), "{error}");
    let mut paths = Vec::new();
    for entry in tar::Archive::new(tar.as_slice()).entries().unwrap() {
        let mut entry = entry.unwrap();
        paths.push(entry.path().unwrap().into_owned());
        let mut data = Vec::new();
        entry.read_to_end(&mut data).unwrap();
    }
    assert!(paths.contains(&"public".into()));
    assert!(
        !paths
            .iter()
            .any(|p| p.starts_with("ledger") || p.starts_with("alias"))
    );
    let bad = tmp.path().join("bad");
    std::fs::create_dir(&bad).unwrap();
    symlink("../../outside", bad.join("escape")).unwrap();
    let (data, error) = pull(&agent, &bad).await;
    assert!(!error.is_empty());
    assert!(
        data.is_empty(),
        "unsafe archives must not expose partial output"
    );
}

#[tokio::test]
async fn restricted_directory_modes_and_equivalent_xattrs_are_applied_after_contents() {
    let (tmp, agent) = setup();
    let target = tmp.path().join("target");
    let tar = archive(|b| {
        let mut h = header(tar::EntryType::Directory, 0);
        h.set_mode(0o500);
        b.append_data(&mut h, "nested", &b""[..]).unwrap();
        b.append_pax_extensions([
            ("SCHILY.xattr.user.test", &b"yes"[..]),
            ("LIBARCHIVE.xattr.user.test", &b"eWVz"[..]),
        ])
        .unwrap();
        regular(b, "nested/leaf", b"data");
    });
    assert_eq!(push(&agent, &target, &tar).await, "");
    assert_eq!(
        std::fs::metadata(target.join("nested")).unwrap().mode() & 0o7777,
        0o500
    );
    assert_eq!(
        std::fs::metadata(target.join("nested")).unwrap().mtime(),
        123
    );
    assert_eq!(xattr(&target.join("nested/leaf"), "user.test"), b"yes");
    // EXCHANGE must also clean the old destination with a read-only directory.
    assert_eq!(
        push(&agent, &target, &archive(|b| regular(b, "other", b"ok"))).await,
        ""
    );
    assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 2);
    std::fs::set_permissions(target.join("nested"), Permissions::from_mode(0o700)).unwrap();
}

#[tokio::test]
async fn schily_xattr_names_preserve_literal_percent() {
    let (tmp, agent) = setup();
    let target = tmp.path().join("target");
    let tar = archive(|b| {
        b.append_pax_extensions([
            // A literal percent that is not an escape sequence stays as-is.
            ("SCHILY.xattr.user.100% legit", &b"literal\0\xff"[..]),
            // A name merely resembling an escape sequence is not decoded.
            ("SCHILY.xattr.user.a%2Fb", &b"escape-like"[..]),
        ])
        .unwrap();
        regular(b, "file", b"content");
    });
    assert_eq!(push(&agent, &target, &tar).await, "");
    let file = target.join("file");
    assert_eq!(std::fs::read(&file).unwrap(), b"content");
    assert_eq!(xattr(&file, "user.100% legit"), b"literal\0\xff");
    assert_eq!(xattr(&file, "user.a%2Fb"), b"escape-like");
    let names = xattrs(&file);
    for expected in ["user.100% legit", "user.a%2Fb"] {
        assert_eq!(
            names.iter().filter(|n| *n == expected).count(),
            1,
            "{names:?}"
        );
    }
    // Decoding would have produced this alias instead of the literal name.
    assert!(!names.contains(&"user.a/b".to_owned()), "{names:?}");
}

#[tokio::test]
async fn xattr_format_equivalence_and_conflicts_preserve_names() {
    let (tmp, agent) = setup();
    let target = tmp.path().join("target");
    std::fs::create_dir(&target).unwrap();
    std::fs::write(target.join("kept"), b"kept").unwrap();
    set_xattr(&target.join("kept"), "user.original", b"before");
    let entries = || std::fs::read_dir(tmp.path()).unwrap().count();
    // The same percent-named attribute expressed in both formats dedupes when
    // the values agree. A literal SCHILY name resembling an escape sequence is
    // a distinct attribute from the one the LIBARCHIVE form decodes to.
    let same = archive(|b| {
        b.append_pax_extensions([
            ("SCHILY.xattr.user.50%", &b"half"[..]),
            (
                "LIBARCHIVE.xattr.user.50%25",
                STANDARD_NO_PAD.encode(b"half").as_bytes(),
            ),
            ("SCHILY.xattr.user.a%20b", &b"raw"[..]),
            (
                "LIBARCHIVE.xattr.user.a%20b",
                STANDARD_NO_PAD.encode(b"raw").as_bytes(),
            ),
        ])
        .unwrap();
        regular(b, "file", b"data");
    });
    assert_eq!(push(&agent, &target, &same).await, "");
    let file = target.join("file");
    assert_eq!(std::fs::read(&file).unwrap(), b"data");
    assert_eq!(xattr(&file, "user.50%"), b"half");
    assert_eq!(xattr(&file, "user.a%20b"), b"raw");
    assert_eq!(xattr(&file, "user.a b"), b"raw");
    let names = xattrs(&file);
    for expected in ["user.50%", "user.a%20b", "user.a b"] {
        assert_eq!(
            names.iter().filter(|n| *n == expected).count(),
            1,
            "{names:?}"
        );
    }
    assert_eq!(names.len(), 3, "{names:?}");
    // A 255-byte final name is accepted in both formats even though the
    // LIBARCHIVE spelling is longer on the wire; both spellings dedupe.
    let longest = format!("user.{}%", "n".repeat(249));
    assert_eq!(longest.len(), 255);
    let encoded = format!("user.{}%25", "n".repeat(249));
    let edge = archive(|b| {
        b.append_pax_extensions([
            (format!("SCHILY.xattr.{longest}").as_str(), &b"edge"[..]),
            (
                format!("LIBARCHIVE.xattr.{encoded}").as_str(),
                STANDARD_NO_PAD.encode(b"edge").as_bytes(),
            ),
        ])
        .unwrap();
        regular(b, "edge", b"edge");
    });
    assert_eq!(push(&agent, &target, &edge).await, "");
    assert_eq!(xattr(&target.join("edge"), &longest), b"edge");
    assert_eq!(xattrs(&target.join("edge")).len(), 1);
    let too_long = format!("user.{}", "m".repeat(251));
    assert_eq!(too_long.len(), 256);
    let rejections = [
        // Conflicting values for the same final name across formats.
        archive(|b| {
            b.append_pax_extensions([
                ("SCHILY.xattr.user.50%", &b"one"[..]),
                (
                    "LIBARCHIVE.xattr.user.50%25",
                    STANDARD_NO_PAD.encode(b"two").as_bytes(),
                ),
            ])
            .unwrap();
            regular(b, "bad", b"bad");
        }),
        // A 256-byte final name exceeds the name limit.
        archive(|b| {
            b.append_pax_extensions([(format!("SCHILY.xattr.{too_long}").as_str(), &b"bad"[..])])
                .unwrap();
            regular(b, "bad", b"bad");
        }),
        // Malformed LIBARCHIVE escape sequence.
        archive(|b| {
            b.append_pax_extensions([("LIBARCHIVE.xattr.user.bad%zz", &b"eA"[..])])
                .unwrap();
            regular(b, "bad", b"bad");
        }),
        // Non-user namespaces stay unsupported.
        archive(|b| {
            b.append_pax_extensions([("SCHILY.xattr.security.capability", &b"bad"[..])])
                .unwrap();
            regular(b, "bad", b"bad");
        }),
    ];
    let before = entries();
    for (i, tar) in rejections.iter().enumerate() {
        let error = push(&agent, &target, tar).await;
        assert!(!error.is_empty(), "rejection {i} was accepted");
        assert!(!target.join("bad").exists(), "rejection {i} committed");
        assert_eq!(std::fs::read(target.join("kept")).unwrap(), b"kept");
        assert_eq!(xattr(&target.join("kept"), "user.original"), b"before");
        assert_eq!(xattr(&file, "user.50%"), b"half");
        assert_eq!(xattr(&target.join("edge"), &longest), b"edge");
        assert_eq!(
            entries(),
            before,
            "staging leak after rejection {i}: {error}"
        );
    }
}
