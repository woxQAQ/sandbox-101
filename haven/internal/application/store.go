package application

import (
	"context"

	"github.com/woxQAQ/haven/internal/recovery"
	"github.com/woxQAQ/haven/store"
)

func storeValidation() store.Validation {
	return recovery.NewRecovery().Validation()
}

func openStore(directory string, initialize bool) (*store.State, error) {
	return store.Open(directory, initialize, storeValidation())
}

func BackupState(ctx context.Context, directory, destination string) (store.BackupPoint, error) {
	return store.BackupState(ctx, directory, destination, storeValidation())
}

func RestoreBackup(ctx context.Context, destination, source string) (store.BackupPoint, error) {
	return store.RestoreBackup(ctx, destination, source, storeValidation())
}

func AcknowledgeRecovery(ctx context.Context, directory, note string) error {
	return store.AcknowledgeRecovery(ctx, directory, note)
}
