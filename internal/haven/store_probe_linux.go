package haven

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/woxQAQ/haven/loom"
)

// readOnlyProbe never creates or updates SQLite sidecars in the owned state.
// Existing WAL/SHM are copied under the writer lock for isolated interpretation.
func (state *store) readOnlyProbe() (*sql.DB, func() error, error) {
	path := filepath.Join(state.directory, "haven.db")
	walErr := privateStoreFile(path + "-wal")
	shmErr := privateStoreFile(path + "-shm")
	absentWAL, absentSHM := os.IsNotExist(walErr), os.IsNotExist(shmErr)
	temporary := ""
	immutable := true
	if !absentWAL || !absentSHM {
		if walErr != nil || shmErr != nil {
			return nil, nil, fmt.Errorf("%w: incomplete or untrusted SQLite sidecars: %v", errStoreRecovery, errors.Join(walErr, shmErr))
		}
		var err error
		temporary, err = os.MkdirTemp("", "haven-readonly-")
		if err != nil {
			return nil, nil, err
		}
		target := filepath.Join(temporary, "haven.db")
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if err = loom.CopyFile(path+suffix, target+suffix, 0o600); err != nil {
				return nil, nil, errors.Join(err, os.RemoveAll(temporary))
			}
		}
		path = target
		immutable = false
	}
	database, err := sql.Open("sqlite", storeDSN(path, "ro", immutable))
	if err != nil {
		if temporary != "" {
			err = errors.Join(err, os.RemoveAll(temporary))
		}
		return nil, nil, err
	}
	database.SetMaxOpenConns(1)
	closeProbe := func() error {
		err := database.Close()
		if temporary != "" {
			err = errors.Join(err, os.RemoveAll(temporary))
		}
		return err
	}
	return database, closeProbe, nil
}
