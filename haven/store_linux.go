package haven

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"

	"github.com/woxQAQ/haven/loom"
)

const (
	storeFormat        = "haven-single-control"
	storeVersion       = 9
	storeApplicationID = 0x4841564e
)

var (
	errStoreRecovery = errors.New("haven store requires manual recovery")
	errStoreConflict = errors.New("haven store already owned or initialized")
)

type storeIdentity struct {
	Format        string `json:"format"`
	Version       int    `json:"version"`
	Identity      string `json:"identity"`
	NodeKeySHA256 string `json:"node_key_sha256"`
}

type storeHead struct {
	Format   string `json:"format"`
	Identity string `json:"identity"`
	Epoch    int64  `json:"epoch"`
}

type store struct {
	reviewed  bool
	headMu    sync.Mutex
	db        *statementDB
	lock      *os.File
	directory string
	key       []byte
	identity  storeIdentity
	ready     bool
}

// openStore owns one writer lock and one database. It performs no dispatch,
// reconciliation, resource discovery, metadata migration or recovery replay.
func openStore(directory string, initialize bool) (*store, error) {
	directory, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	if initialize {
		parent, err := loom.OpenCaptureDir(filepath.Dir(directory))
		if err != nil {
			return nil, err
		}
		_ = parent.Close()
		if err = os.Mkdir(directory, 0o700); err != nil && !os.IsExist(err) {
			return nil, err
		}
	}
	root, err := loom.OpenCaptureDir(directory)
	if err != nil {
		return nil, fmt.Errorf("%w: private state directory unavailable: %v", errStoreRecovery, err)
	}
	info, err := root.Stat()
	_ = root.Close()
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm() != 0o700 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("%w: state directory must be owned by this writer and mode 0700", errStoreRecovery)
	}
	state := &store{
		directory: directory,
	}
	if initialize {
		entries, err := os.ReadDir(directory)
		if err != nil {
			return nil, err
		}
		if len(entries) != 0 {
			return nil, fmt.Errorf("%w: initialization requires an empty private directory", errStoreRecovery)
		}
	} else {
		if _, err := os.Lstat(filepath.Join(directory, "RECOVERY_REQUIRED")); err == nil {
			return nil, fmt.Errorf("%w: explicit recovery review is required", errStoreRecovery)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
		for _, name := range []string{
			"haven.db",
			"writer.lock",
			"node.key",
			"store.json",
			"HEAD.json",
		} {
			if err = privateStoreFile(filepath.Join(directory, name)); err != nil {
				return nil, fmt.Errorf("%w: incomplete store: %v", errStoreRecovery, err)
			}
		}
		if err = loom.ReadJSON(filepath.Join(directory, "store.json"), &state.identity); err != nil {
			return nil, fmt.Errorf("%w: invalid format identity: %v", errStoreRecovery, err)
		}
		if state.identity.Format != storeFormat || state.identity.Version != storeVersion || !loom.ValidResourceID(state.identity.Identity, "store-") {
			return nil, fmt.Errorf("%w: foreign or unsupported format identity", errStoreRecovery)
		}
		state.key, err = os.ReadFile(filepath.Join(directory, "node.key"))
		if err != nil || len(state.key) != 32 || keyHash(state.key) != state.identity.NodeKeySHA256 {
			return nil, fmt.Errorf("%w: node key missing or inconsistent", errStoreRecovery)
		}
		if err = privateStoreFile(filepath.Join(directory, "RECOVERY_REVIEWED.json")); err == nil {
			var review recoveryReview
			if err = loom.ReadJSON(filepath.Join(directory, "RECOVERY_REVIEWED.json"), &review); err != nil {
				return nil, err
			}
			if review.Format != 1 || review.Backup.Store != state.identity || review.Backup.Format != 1 {
				return nil, fmt.Errorf("%w: recovery review identity inconsistent", errStoreRecovery)
			}
			state.reviewed = true
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}
	flags := unix.O_RDWR | unix.O_NOFOLLOW | unix.O_CLOEXEC
	if initialize {
		flags |= unix.O_CREAT | unix.O_EXCL
	}
	fd, err := unix.Open(filepath.Join(directory, "writer.lock"), flags, 0o600)
	if err != nil {
		return nil, err
	}
	state.lock = os.NewFile(uintptr(fd), "writer.lock")
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = state.lock.Close()
		return nil, fmt.Errorf("%w: another writer holds this store", errStoreConflict)
	}
	fail := func(err error) (*store, error) {
		return nil, errors.Join(err, state.close())
	}
	path := filepath.Join(directory, "haven.db")
	if initialize {
		state.key = make([]byte, 32)
		if _, err = rand.Read(state.key); err != nil {
			return fail(err)
		}
		identityBytes := make([]byte, 16)
		if _, err = rand.Read(identityBytes); err != nil {
			return fail(err)
		}
		state.identity = storeIdentity{
			Format:        storeFormat,
			Version:       storeVersion,
			Identity:      "store-" + hex.EncodeToString(identityBytes),
			NodeKeySHA256: keyHash(state.key),
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			return fail(err)
		}
		if err = file.Close(); err != nil {
			return fail(err)
		}
	} else {
		// Initialization checkpoints the static format and schema into the DB
		// before publishing store.json. This probe never creates WAL/SHM files.
		probe, err := sql.Open("sqlite", storeDSN(path, "ro", true))
		if err != nil {
			return fail(err)
		}
		probe.SetMaxOpenConns(1)
		err = state.validate(probe)
		err = errors.Join(err, probe.Close())
		if err != nil {
			return fail(fmt.Errorf("%w: unsupported or corrupt metadata: %v", errStoreRecovery, err))
		}
		reader, closeProbe, err := state.readOnlyProbe()
		if err != nil {
			return fail(err)
		}
		err = state.validate(reader)
		if err == nil {
			err = state.validateHead(reader)
		}
		if err == nil {
			err = state.validateWorkflows(reader)
		}
		err = errors.Join(err, closeProbe())
		if err != nil {
			return fail(fmt.Errorf("%w: existing store validation failed: %v", errStoreRecovery, err))
		}
	}
	database, err := sql.Open("sqlite", storeDSN(path, "rw", false))
	if err != nil {
		return fail(err)
	}
	state.db = &statementDB{
		DB:         database,
		statements: map[string]*sql.Stmt{},
	}
	state.db.SetMaxOpenConns(1)
	if initialize {
		if err = state.initialize(); err != nil {
			return fail(err)
		}
		if err = loom.AtomicWrite(filepath.Join(directory, "node.key"), state.key, 0o600); err != nil {
			return fail(err)
		}
		if err = loom.WriteJSON(filepath.Join(directory, "store.json"), state.identity); err != nil {
			return fail(err)
		}
	} else if err = state.validate(state.db.DB); err != nil {
		return fail(fmt.Errorf("%w: metadata integrity failed: %v", errStoreRecovery, err))
	}
	if !initialize {
		if err = state.validateHead(state.db.DB); err != nil {
			return fail(err)
		}
	}
	state.ready = true
	if initialize {
		if err = state.checkpoint(); err != nil {
			state.ready = false
			return fail(err)
		}
	}
	return state, nil
}

func privateStoreFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("private regular file required: %s", path)
	}
	return nil
}

func keyHash(key []byte) string {
	hash := sha256.Sum256(key)
	return hex.EncodeToString(hash[:])
}

func storeDSN(path, mode string, immutable bool) string {
	uri := url.URL{
		Scheme: "file",
		Path:   path,
	}
	query := uri.Query()
	query.Set("mode", mode)
	if immutable {
		query.Set("immutable", "1")
	} else if mode == "rw" {
		for _, pragma := range []string{
			"busy_timeout(5000)",
			"journal_mode(WAL)",
			"synchronous(FULL)",
			"foreign_keys(ON)",
		} {
			query.Add("_pragma", pragma)
		}
	}
	uri.RawQuery = query.Encode()
	return uri.String()
}

func (state *store) initialize() error {
	tx, err := state.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = tx.Exec(storeSchema); err != nil {
		return err
	}
	if _, err = tx.Exec("INSERT INTO store_metadata VALUES(1,?,?,?,?,0)", state.identity.Format, state.identity.Version, state.identity.Identity, state.identity.NodeKeySHA256); err != nil {
		return err
	}
	if _, err = tx.Exec(fmt.Sprintf("PRAGMA application_id=%d; PRAGMA user_version=%d", storeApplicationID, storeVersion)); err != nil {
		return err
	}
	for _, table := range epochTables {
		for _, event := range []string{
			"INSERT",
			"UPDATE",
			"DELETE",
		} {
			statement := fmt.Sprintf("CREATE TRIGGER epoch_%s_%s AFTER %s ON %s BEGIN UPDATE store_metadata SET epoch=epoch+1 WHERE singleton=1; END", table, event, event, table)
			if _, err = tx.Exec(statement); err != nil {
				return err
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	var busy, frames, checkpointed int
	if err = state.db.QueryRow("PRAGMA wal_checkpoint(FULL)").Scan(&busy, &frames, &checkpointed); err != nil {
		return err
	}
	if busy != 0 || frames != checkpointed {
		return fmt.Errorf("initial schema checkpoint incomplete")
	}
	return nil
}

func (state *store) validate(database *sql.DB) error {
	var integrity string
	if err := database.QueryRow("PRAGMA quick_check").Scan(&integrity); err != nil || integrity != "ok" {
		return fmt.Errorf("integrity check failed: %v %s", err, integrity)
	}
	var application, version int
	if err := database.QueryRow("PRAGMA application_id").Scan(&application); err != nil {
		return err
	}
	if err := database.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if application != storeApplicationID || version != storeVersion {
		return fmt.Errorf("foreign application identity or schema version")
	}
	var actual storeIdentity
	var epoch int64
	if err := database.QueryRow("SELECT format,version,identity,node_key_sha256,epoch FROM store_metadata WHERE singleton=1").Scan(&actual.Format, &actual.Version, &actual.Identity, &actual.NodeKeySHA256, &epoch); err != nil {
		return err
	}
	if actual != state.identity || epoch < 0 {
		return fmt.Errorf("inconsistent store identity")
	}
	for _, table := range epochTables {
		var found int
		if err := database.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&found); err != nil || found != 1 {
			return fmt.Errorf("required table missing: %s: %v", table, err)
		}
	}
	for _, trigger := range []string{
		"credential_identity_immutable",
		"credential_history_retained",
		"operation_identity_immutable",
		"operation_terminal_immutable",
		"operation_history_retained",
		"step_identity_immutable",
		"step_terminal_immutable",
		"step_history_retained",
		"store_identity_immutable",
		"deletion_identity_immutable",
		"snapshot_owner_immutable",
		"snapshot_owner_retained",
		"operation_clock_insert",
		"operation_clock_finish",
		"operation_clock_identity_immutable",
		"operation_clock_terminal_immutable",
		"operation_clock_retained",
		"publication_immutable",
		"publication_retained",
		"journal_immutable",
		"journal_retained",
	} {
		var found int
		if err := database.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=?", trigger).Scan(&found); err != nil || found != 1 {
			return fmt.Errorf("required persistence guard missing: %s: %v", trigger, err)
		}
	}
	violations, err := database.Query("PRAGMA foreign_key_check")
	if err != nil {
		return err
	}
	invalid := violations.Next()
	err = errors.Join(violations.Err(), violations.Close())
	if err != nil {
		return err
	}
	if invalid {
		return fmt.Errorf("dangling resource or operation reference")
	}
	var missing int
	if err := database.QueryRow(`SELECT COUNT(*) FROM operations o LEFT JOIN operation_publications p ON p.operation_id=o.id WHERE o.status IN ('succeeded','failed') AND p.operation_id IS NULL AND (EXISTS(SELECT 1 FROM operation_credentials c WHERE c.operation_id=o.id) OR EXISTS(SELECT 1 FROM operation_steps s WHERE s.operation_id=o.id AND s.action='activate'))`).Scan(&missing); err != nil {
		return err
	}
	if missing != 0 {
		return fmt.Errorf("terminal public operation lacks frozen result")
	}
	if err := state.validatePublications(database); err != nil {
		return err
	}
	return state.validateJournal(database)
}

func (state *store) validateHead(database *sql.DB) error {
	var head storeHead
	if err := loom.ReadJSON(filepath.Join(state.directory, "HEAD.json"), &head); err != nil {
		return fmt.Errorf("%w: unreadable durable HEAD", errStoreRecovery)
	}
	var epoch int64
	if err := database.QueryRow("SELECT epoch FROM store_metadata WHERE singleton=1").Scan(&epoch); err != nil {
		return err
	}
	if head.Format != storeFormat || head.Identity != state.identity.Identity || head.Epoch < 0 || epoch < head.Epoch {
		return fmt.Errorf("%w: database identity or epoch is behind durable HEAD", errStoreRecovery)
	}
	return nil
}

func (state *store) checkpoint() error {
	state.headMu.Lock()
	defer state.headMu.Unlock()
	head := storeHead{
		Format:   storeFormat,
		Identity: state.identity.Identity,
	}
	if err := state.db.QueryRowContext(context.Background(), "SELECT epoch FROM store_metadata WHERE singleton=1").Scan(&head.Epoch); err != nil {
		return err
	}
	return loom.WriteJSON(filepath.Join(state.directory, "HEAD.json"), head)
}

func (state *store) close() error {
	var result error
	if state.db != nil {
		if state.ready {
			result = state.checkpoint()
		}
		result = errors.Join(result, state.db.Close())
	}
	if state.lock != nil {
		// Close alone can leave the open-file-description lock held by a
		// concurrent fork until that child execs, even with CLOEXEC. The
		// database and workers are already closed before releasing ownership.
		unlockErr := unix.Flock(int(state.lock.Fd()), unix.LOCK_UN)
		result = errors.Join(result, unlockErr, state.lock.Close())
		state.lock = nil
	}
	state.ready = false
	return result
}
