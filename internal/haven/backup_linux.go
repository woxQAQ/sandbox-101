package haven

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/haven/loom"
)

type BackupPoint struct {
	Format    int               `json:"format"`
	ID        string            `json:"id"`
	CreatedAt time.Time         `json:"created_at"`
	Store     storeIdentity     `json:"store"`
	Epoch     int64             `json:"epoch"`
	Files     map[string]string `json:"files"`
}
type recoveryReview struct {
	Format     int         `json:"format"`
	Backup     BackupPoint `json:"backup"`
	Note       string      `json:"note"`
	ReviewedAt time.Time   `json:"reviewed_at"`
}

var backupComponents = []string{"haven.db", "node.key", "store.json", "HEAD.json"}

// BackupState takes the normal exclusive writer lock after the daemon exits.
// Runtime disks, RAM and physical custody require separate resource recovery.
func BackupState(ctx context.Context, directory, destination string) (BackupPoint, error) {
	state, err := openStore(directory, false)
	if err != nil {
		return BackupPoint{}, err
	}
	point, err := state.backup(ctx, destination)
	return point, errors.Join(err, state.close())
}

func (state *store) backup(ctx context.Context, destination string) (BackupPoint, error) {
	point := BackupPoint{Format: 1, ID: newID("backup-"), CreatedAt: time.Now().UTC(), Store: state.identity, Files: map[string]string{}}
	if err := os.Mkdir(destination, 0o700); err != nil {
		return point, err
	}
	database := filepath.Join(destination, "haven.db")
	if _, err := state.db.ExecContext(ctx, "VACUUM INTO ?", database); err != nil {
		return point, err
	}
	if err := syncBackupFile(database); err != nil {
		return point, err
	}
	copyDB, err := sql.Open("sqlite", storeDSN(database, "ro", true))
	if err != nil {
		return point, err
	}
	copyDB.SetMaxOpenConns(1)
	err = state.validate(copyDB)
	if err == nil {
		err = copyDB.QueryRowContext(ctx, "SELECT epoch FROM store_metadata WHERE singleton=1").Scan(&point.Epoch)
	}
	err = errors.Join(err, copyDB.Close())
	if err != nil {
		return point, err
	}
	for name, payload := range map[string][]byte{"node.key": state.key, "store.json": encode(state.identity), "HEAD.json": encode(storeHead{Format: storeFormat, Identity: state.identity.Identity, Epoch: point.Epoch})} {
		if err = loom.AtomicWrite(filepath.Join(destination, name), payload, 0o600); err != nil {
			return point, err
		}
	}
	for _, name := range backupComponents {
		point.Files[name], err = loom.DigestFileContext(ctx, filepath.Join(destination, name))
		if err != nil {
			return point, err
		}
	}
	if err = loom.WriteJSON(filepath.Join(destination, "backup.json"), point); err != nil {
		return point, err
	}
	return point, loom.SyncDir(filepath.Dir(destination))
}

func syncBackupFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err = file.Chmod(0o600); err != nil {
		return errors.Join(err, file.Close())
	}
	return errors.Join(file.Sync(), file.Close())
}

func copyBackupComponent(ctx context.Context, source, destination string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = io.Copy(output, input)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = output.Sync()
	}
	return errors.Join(err, output.Close())
}

func readBackup(ctx context.Context, source string) (BackupPoint, error) {
	var point BackupPoint
	directory, err := loom.OpenCaptureDir(source)
	if err != nil {
		return point, err
	}
	_ = directory.Close()
	if err = privateStoreFile(filepath.Join(source, "backup.json")); err != nil {
		return point, err
	}
	if err = loom.ReadJSON(filepath.Join(source, "backup.json"), &point); err != nil {
		return point, err
	}
	if point.Format != 1 || !loom.ValidResourceID(point.ID, "backup-") || point.Store.Format != storeFormat || point.Store.Version != storeVersion || point.Epoch < 0 || len(point.Files) != len(backupComponents) {
		return point, loom.Fail(loom.Incompatible, "unsupported metadata backup")
	}
	for _, name := range backupComponents {
		if err = privateStoreFile(filepath.Join(source, name)); err != nil {
			return point, err
		}
		hash, err := loom.DigestFileContext(ctx, filepath.Join(source, name))
		if err != nil {
			return point, err
		}
		if hash != point.Files[name] {
			return point, loom.Fail(loom.Incompatible, "backup component checksum mismatch")
		}
	}
	var identity storeIdentity
	if err = loom.ReadJSON(filepath.Join(source, "store.json"), &identity); err != nil {
		return point, err
	}
	key, err := os.ReadFile(filepath.Join(source, "node.key"))
	if err != nil {
		return point, err
	}
	if identity != point.Store || len(key) != 32 || keyHash(key) != identity.NodeKeySHA256 {
		return point, loom.Fail(loom.Incompatible, "backup DB/node-key identity mismatch")
	}
	state := &store{directory: source, identity: identity}
	database, err := sql.Open("sqlite", storeDSN(filepath.Join(source, "haven.db"), "ro", true))
	if err != nil {
		return point, err
	}
	database.SetMaxOpenConns(1)
	err = state.validate(database)
	if err == nil {
		err = state.validateHead(database)
	}
	var epoch int64
	if err == nil {
		err = database.QueryRowContext(ctx, "SELECT epoch FROM store_metadata WHERE singleton=1").Scan(&epoch)
		if err == nil && epoch != point.Epoch {
			err = fmt.Errorf("backup epoch differs from recorded SQLite point")
		}
	}
	return point, errors.Join(err, database.Close())
}

// RestoreBackup installs only a new metadata directory. The recovery fence is
// published before any component, and ordinary startup cannot replay this state.
func RestoreBackup(ctx context.Context, destination, source string) (BackupPoint, error) {
	point, err := readBackup(ctx, source)
	if err != nil {
		return point, err
	}
	if err = os.Mkdir(destination, 0o700); err != nil {
		return point, err
	}
	if err = loom.WriteJSON(filepath.Join(destination, "RECOVERY_REQUIRED"), point); err != nil {
		return point, err
	}
	for _, name := range backupComponents {
		if err = copyBackupComponent(ctx, filepath.Join(source, name), filepath.Join(destination, name)); err != nil {
			return point, err
		}
	}
	lock, err := os.OpenFile(filepath.Join(destination, "writer.lock"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return point, err
	}
	if err = errors.Join(lock.Sync(), lock.Close()); err != nil {
		return point, err
	}
	if err = loom.SyncDir(destination); err != nil {
		return point, err
	}
	return point, loom.SyncDir(filepath.Dir(destination))
}

// AcknowledgeRecovery records review without granting effect replay or GC
// authority. A reviewed rollback remains available for metadata diagnostics.
func AcknowledgeRecovery(ctx context.Context, directory, note string) error {
	if len(strings.TrimSpace(note)) < 16 || len(note) > 16384 {
		return loom.Fail(loom.Invalid, "a bounded review note describing missing ownership history is required")
	}
	fd, err := unix.Open(filepath.Join(directory, "writer.lock"), unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	lock := os.NewFile(uintptr(fd), "writer.lock")
	defer func() { _ = lock.Close() }()
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return loom.Fail(loom.Conflict, "Haven store is active")
	}
	defer func() { _ = unix.Flock(fd, unix.LOCK_UN) }()
	if err = privateStoreFile(filepath.Join(directory, "RECOVERY_REQUIRED")); err != nil {
		return err
	}
	var point BackupPoint
	if err = loom.ReadJSON(filepath.Join(directory, "RECOVERY_REQUIRED"), &point); err != nil {
		return err
	}
	if point.Format != 1 || point.Store.Format != storeFormat || point.Store.Version != storeVersion {
		return loom.Fail(loom.Incompatible, "unsupported recovery review")
	}
	for _, name := range backupComponents {
		if err = privateStoreFile(filepath.Join(directory, name)); err != nil {
			return err
		}
		hash, err := loom.DigestFileContext(ctx, filepath.Join(directory, name))
		if err != nil {
			return err
		}
		if hash != point.Files[name] {
			return loom.Fail(loom.ManualRecovery, "restored metadata changed before review")
		}
	}
	if err = loom.WriteJSON(filepath.Join(directory, "RECOVERY_REVIEWED.json"), recoveryReview{Format: 1, Backup: point, Note: note, ReviewedAt: time.Now().UTC()}); err != nil {
		return err
	}
	if err = os.Remove(filepath.Join(directory, "RECOVERY_REQUIRED")); err != nil {
		return err
	}
	return loom.SyncDir(directory)
}
