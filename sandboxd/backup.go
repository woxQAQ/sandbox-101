package sandboxd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/sandbox-engine/api"
)

type BackupPoint struct {
	Format         int       `json:"format"`
	ID             string    `json:"id"`
	CreatedAt      time.Time `json:"created_at"`
	DatabaseSHA256 string    `json:"database_sha256"`
	NodeKeySHA256  string    `json:"node_key_sha256"`
}

// Backup captures a consistent SQLite snapshot and the immutable token/audit key.
// Guest snapshots belong to engine and must be backed up separately.
func (s *Service) Backup(
	ctx context.Context,
	destination string,
) (BackupPoint, error) {
	return s.store.backup(ctx, destination)
}

func (s *store) backup(
	ctx context.Context,
	destination string,
) (BackupPoint, error) {
	point := BackupPoint{
		Format:    1,
		ID:        newID("product-backup_"),
		CreatedAt: time.Now().UTC(),
	}
	if err := os.Mkdir(destination, 0o700); err != nil {
		return point, err
	}
	database := filepath.Join(destination, "sandboxd.db")
	if _, err := s.db.ExecContext(ctx, "VACUUM INTO ?", database); err != nil {
		return point, err
	}
	if err := syncPrivateFile(database); err != nil {
		return point, err
	}
	if err := atomicFile(
		filepath.Join(destination, "node.key"),
		s.key,
	); err != nil {
		return point, err
	}
	var err error
	point.DatabaseSHA256, err = hashFile(database)
	if err != nil {
		return point, err
	}
	point.NodeKeySHA256, err = hashFile(filepath.Join(destination, "node.key"))
	if err != nil {
		return point, err
	}
	if err = atomicFile(
		filepath.Join(destination, "backup.json"),
		encode(point),
	); err != nil {
		return point, err
	}
	return point, nil
}

func BackupState(
	ctx context.Context,
	state, destination string,
) (BackupPoint, error) {
	st, err := openStore(state, false)
	if err != nil {
		return BackupPoint{}, err
	}
	defer func() { _ = st.close() }()
	return st.backup(ctx, destination)
}

func hashFile(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", api.Fail(
			api.Invalid,
			"backup component is not a regular file",
		)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func syncPrivateFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err = f.Chmod(0o600); err != nil {
		return errors.Join(err, f.Close())
	}
	return errors.Join(f.Sync(), f.Close())
}

func copyBackupFile(source, destination string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(
		destination,
		os.O_CREATE|os.O_EXCL|os.O_WRONLY,
		0o600,
	)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err == nil {
		err = out.Sync()
	}
	return errors.Join(err, out.Close())
}

// RestoreStateBackup creates a new metadata directory and blocks ordinary startup
// until an operator has reviewed this known rollback against the engine inventory.
func RestoreStateBackup(state, source string) (BackupPoint, error) {
	var point BackupPoint
	raw, err := os.ReadFile(filepath.Join(source, "backup.json"))
	if err != nil {
		return point, err
	}
	if err = json.Unmarshal(raw, &point); err != nil {
		return point, err
	}
	if point.Format != 1 || point.ID == "" {
		return point, api.Fail(api.Incompatible, "unsupported product backup")
	}
	for name, want := range map[string]string{"sandboxd.db": point.DatabaseSHA256, "node.key": point.NodeKeySHA256} {
		got, err := hashFile(filepath.Join(source, name))
		if err != nil {
			return point, err
		}
		if got != want {
			return point, api.Fail(
				api.Invalid,
				"backup checksum mismatch for %s",
				name,
			)
		}
	}
	if err = os.Mkdir(state, 0o700); err != nil {
		return point, err
	}
	// Install the recovery fence first: a partial restoration cannot become a new node.
	if err = atomicFile(
		filepath.Join(state, "RECOVERY_REQUIRED"),
		raw,
	); err != nil {
		return point, err
	}
	for _, name := range []string{"sandboxd.db", "node.key"} {
		if err = copyBackupFile(
			filepath.Join(source, name),
			filepath.Join(state, name),
		); err != nil {
			return point, err
		}
	}
	if err = atomicFile(
		filepath.Join(state, "initialized"),
		[]byte("sandboxd schema 1\n"),
	); err != nil {
		return point, err
	}
	return point, nil
}

func AcknowledgeStateRecovery(state, note string) error {
	if len(strings.TrimSpace(note)) < 16 {
		return api.Fail(
			api.Invalid,
			"a review note describing engine inventory and missing history is required",
		)
	}
	lock, err := os.OpenFile(
		filepath.Join(state, "writer.lock"),
		os.O_CREATE|os.O_RDWR,
		0o600,
	)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return api.Fail(api.Conflict, "sandboxd is active")
	}
	point, err := os.ReadFile(filepath.Join(state, "RECOVERY_REQUIRED"))
	if err != nil {
		return err
	}
	if err = atomicFile(
		filepath.Join(state, "recovery-review.json"),
		encode(
			map[string]any{
				"backup":      json.RawMessage(point),
				"note":        note,
				"reviewed_at": time.Now().UTC(),
			},
		),
	); err != nil {
		return err
	}
	if err = os.Remove(filepath.Join(state, "RECOVERY_REQUIRED")); err != nil {
		return err
	}
	dir, err := os.Open(state)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
