package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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
	storeVersion       = Version
	storeApplicationID = ApplicationID
)

var (
	errStoreRecovery = errors.New("haven store requires manual recovery")
	errStoreConflict = errors.New("haven store already owned or initialized")
)

type Identity struct {
	Format        string `json:"format"`
	Version       int    `json:"version"`
	Identity      string `json:"identity"`
	NodeKeySHA256 string `json:"node_key_sha256"`
}

// UnmarshalJSON requires an explicit version in store, backup and review files.
func (identity *Identity) UnmarshalJSON(data []byte) error {
	type fields Identity
	var decoded struct {
		fields
		Version *int `json:"version"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Version == nil {
		return errors.New("store identity is missing its version")
	}
	*identity = Identity(decoded.fields)
	identity.Version = *decoded.Version
	return nil
}

type storeHead struct {
	Format   string `json:"format"`
	Identity string `json:"identity"`
	Epoch    int64  `json:"epoch"`
}

// State owns the writer lock, database, node key and durable HEAD.
type State struct {
	validation Validation
	reviewed   bool
	headMu     sync.Mutex
	db         *Database
	lock       *os.File
	directory  string
	key        []byte
	identity   Identity
	ready      bool
}

// Open owns one writer lock and one database. It performs no dispatch,
// reconciliation, resource discovery, metadata migration or recovery replay.
func Open(directory string, initialize bool, validation Validation) (*State, error) {
	if err := validation.check(); err != nil {
		return nil, err
	}
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
	state := &State{
		validation: validation,
		directory:  directory,
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
	fail := func(err error) (*State, error) {
		return nil, errors.Join(err, state.Close())
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
		state.identity = Identity{
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
			err = state.validation.Workflows(context.Background(), reader, Origin{
				Directory: state.directory,
				Reviewed:  state.reviewed,
			})
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
	state.db = &Database{
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
		if err = state.Checkpoint(); err != nil {
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

func (state *State) initialize() error {
	err := Migrate(context.Background(), state.db.DB, func(tx *sql.Tx) error {
		if err := initializeMetadata(tx, state.identity); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
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

func (state *State) validate(database *sql.DB) error {
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
	if err := validateMigrationVersion(context.Background(), database, Version); err != nil {
		return err
	}
	if err := validateMetadata(database, state.identity); err != nil {
		return err
	}
	// The schema carries no triggers; any present one is foreign tampering.
	var triggers int
	if err := database.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'").Scan(&triggers); err != nil || triggers != 0 {
		return fmt.Errorf("foreign trigger present in store schema: %d: %v", triggers, err)
	}
	invalid, err := HasForeignKeyViolations(context.Background(), database)
	if err != nil {
		return err
	}
	if invalid {
		return fmt.Errorf("dangling resource or operation reference")
	}
	if err := state.validation.Publications(context.Background(), database); err != nil {
		return err
	}
	return state.validateJournal(database)
}

func (state *State) validateHead(database *sql.DB) error {
	var head storeHead
	if err := loom.ReadJSON(filepath.Join(state.directory, "HEAD.json"), &head); err != nil {
		return fmt.Errorf("%w: unreadable durable HEAD", errStoreRecovery)
	}
	epoch, err := readEpoch(context.Background(), database)
	if err != nil {
		return err
	}
	if head.Format != storeFormat || head.Identity != state.identity.Identity || head.Epoch < 0 || epoch < head.Epoch {
		return fmt.Errorf("%w: database identity or epoch is behind durable HEAD", errStoreRecovery)
	}
	return nil
}

func (state *State) Checkpoint() error {
	state.headMu.Lock()
	defer state.headMu.Unlock()
	head := storeHead{
		Format:   storeFormat,
		Identity: state.identity.Identity,
	}
	var err error
	head.Epoch, err = state.advanceEpoch(context.Background())
	if err != nil {
		return err
	}
	return loom.WriteJSON(filepath.Join(state.directory, "HEAD.json"), head)
}

// advanceEpoch bumps the write counter and returns the new value. Every
// workflow checkpoints after committing, so the epoch and the durable HEAD
// move together and a restored store cannot pass the head check.
func (state *State) advanceEpoch(ctx context.Context) (int64, error) {
	tx, err := state.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	result, err := tx.ExecContext(ctx, "UPDATE store_metadata SET epoch=epoch+1 WHERE singleton=1")
	if err != nil {
		return 0, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return 0, fmt.Errorf("epoch advance affected %d rows: %v", changed, err)
	}
	var epoch int64
	if err = tx.QueryRowContext(ctx, "SELECT epoch FROM store_metadata WHERE singleton=1").Scan(&epoch); err != nil {
		return 0, err
	}
	return epoch, tx.Commit()
}

func (state *State) Close() error {
	var result error
	if state.db != nil {
		if state.ready {
			result = state.Checkpoint()
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
