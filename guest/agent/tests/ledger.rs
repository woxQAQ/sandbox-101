use sandbox_agent::ledger::{Admission, Ledger, Saved};
use std::{
    os::unix::fs::PermissionsExt,
    sync::{Arc, Barrier},
};

#[test]
fn simultaneous_admission_is_serialized_and_unfinished_intent_is_never_evicted() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("ledger");
    let ledger = Arc::new(Ledger::open(&path, 1).unwrap());
    let barrier = Arc::new(Barrier::new(8));
    let results = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let ledger = ledger.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    ledger.admit("", "shared-key", &[7; 32]).unwrap()
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Admission::New(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Admission::InProgress))
            .count(),
        7
    );
    assert!(ledger.set_identity("fork", false).is_err());
    assert!(
        Ledger::open(&path, 1).is_err(),
        "another agent cannot recover a live owner's intents"
    );
    drop(ledger);
    let ledger = Ledger::open(&path, 1).unwrap();
    assert!(matches!(
        ledger.admit("", "shared-key", &[7; 32]).unwrap(),
        Admission::Unknown
    ));
    assert!(matches!(
        ledger.admit("", "shared-key", &[8; 32]).unwrap(),
        Admission::Conflict
    ));
    assert!(matches!(
        ledger.admit("", "different-key", &[7; 32]).unwrap(),
        Admission::Exhausted
    ));
    ledger.set_identity("fork", false).unwrap();
    let Admission::New(key) = ledger.admit("fork", "shared-key", &[7; 32]).unwrap() else {
        panic!("new identity must have a fresh scope")
    };
    ledger
        .complete(
            &key,
            &Saved {
                exit_code: 19,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(matches!(
        ledger.admit("fork", "shared-key", &[7; 32]).unwrap(),
        Admission::Completed(Saved { exit_code: 19, .. })
    ));
}

#[test]
fn unsafe_permissions_and_symlinked_database_fail_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("ledger");
    std::fs::create_dir(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(Ledger::open(&path, 1).is_err());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let target = tmp.path().join("unrelated-file");
    std::fs::write(&target, b"unchanged").unwrap();
    std::os::unix::fs::symlink(&target, path.join("exec.sqlite3")).unwrap();
    assert!(Ledger::open(&path, 1).is_err());
    assert_eq!(std::fs::read(target).unwrap(), b"unchanged");
}
