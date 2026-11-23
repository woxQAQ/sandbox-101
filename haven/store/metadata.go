package store

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/store/internal/dbgen"
)

func initializeMetadata(database DBTX, identity Identity) error {
	return dbgen.New(database).InitializeMetadata(context.Background(), dbgen.InitializeMetadataParams{
		Format:        identity.Format,
		Version:       int64(identity.Version),
		Identity:      identity.Identity,
		NodeKeySha256: identity.NodeKeySHA256,
	})
}

func validateMetadata(database DBTX, identity Identity) error {
	actual, err := dbgen.New(database).GetMetadata(context.Background())
	if err != nil {
		return err
	}
	if actual.Format != identity.Format || actual.Version != int64(identity.Version) || actual.Identity != identity.Identity || actual.NodeKeySha256 != identity.NodeKeySHA256 || actual.Epoch < 0 {
		return fmt.Errorf("inconsistent store identity")
	}
	return nil
}

// Epoch reads the current metadata epoch through the existing writer connection.
func (state *State) Epoch(ctx context.Context) (int64, error) {
	return readEpoch(ctx, state.db)
}

func readEpoch(ctx context.Context, database DBTX) (int64, error) {
	return dbgen.New(database).GetEpoch(ctx)
}
