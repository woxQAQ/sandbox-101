package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/store/internal/dbgen"
)

func receiptHash(intent loom.DeletionIntent) (string, error) {
	payload, err := json.Marshal(intent)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(payload)), nil
}

// RecordDeletionReceipt binds the tree observed before removal to the operation step in the caller's transaction.
func RecordDeletionReceipt(ctx context.Context, tx *sql.Tx, operationID, stepKey string, intent loom.DeletionIntent) error {
	hash, err := receiptHash(intent)
	if err != nil {
		return err
	}
	q := dbgen.New(tx)
	if err = RequireOne(q.InsertDeletionReceipt(ctx, dbgen.InsertDeletionReceiptParams{
		Path:        intent.Path,
		OperationID: operationID,
		StepKey:     stepKey,
		Format:      int64(intent.Format),
		Directory:   intent.Dir,
		ReceiptHash: hash,
	})); err != nil {
		return err
	}
	if err = RequireOne(q.ClaimDeletion(ctx, dbgen.ClaimDeletionParams{
		Path:        intent.Path,
		OperationID: operationID,
		StepKey:     stepKey,
		ReceiptHash: hash,
	})); err != nil {
		return err
	}
	for name, node := range intent.Nodes {
		if err = RequireOne(q.InsertDeletionNode(ctx, dbgen.InsertDeletionNodeParams{
			OperationID:  operationID,
			StepKey:      stepKey,
			ReceiptPath:  intent.Path,
			RelativePath: name,
			Device:       int64(node.Device),
			Inode:        int64(node.Inode),
			Mode:         int64(node.Mode),
		})); err != nil {
			return err
		}
	}
	return nil
}

// LoadDeletionReceipt reads the complete tree in one SQLite snapshot and verifies it against the preparation digest.
func LoadDeletionReceipt(ctx context.Context, query DBTX, operationID, stepKey, path string) (loom.DeletionIntent, error) {
	var intent loom.DeletionIntent
	err := ReadSnapshot(ctx, query, func(tx *sql.Tx) error {
		hash, err := dbgen.New(tx).LiveDeletionHash(ctx, dbgen.LiveDeletionHashParams{
			Path:        path,
			OperationID: operationID,
			StepKey:     stepKey,
		})
		if err != nil {
			return err
		}
		intent, err = ReadDeletionHistory(ctx, tx, operationID, stepKey, path)
		if err != nil {
			return err
		}
		observed, err := receiptHash(intent)
		if err != nil {
			return err
		}
		if observed != hash {
			return fmt.Errorf("deletion claim differs from original observation")
		}
		return nil
	})
	return intent, err
}

// ReadDeletionHistory reconstructs the original observation independently of the current path claim.
func ReadDeletionHistory(ctx context.Context, query *sql.Tx, operationID, stepKey, path string) (loom.DeletionIntent, error) {
	rows, err := dbgen.New(query).ReadDeletionReceipt(ctx, dbgen.ReadDeletionReceiptParams{
		Path:        path,
		OperationID: operationID,
		StepKey:     stepKey,
	})
	if err != nil {
		return loom.DeletionIntent{}, err
	}
	if len(rows) == 0 {
		return loom.DeletionIntent{}, sql.ErrNoRows
	}
	intent := loom.DeletionIntent{
		Format: int(rows[0].Format),
		Path:   path,
		Dir:    rows[0].Directory,
		Nodes:  map[string]loom.FileIdentity{},
	}
	for _, row := range rows {
		if row.RelativePath.Valid {
			intent.Nodes[row.RelativePath.String] = loom.FileIdentity{
				Device: uint64(row.Device.Int64),
				Inode:  uint64(row.Inode.Int64),
				Mode:   uint32(row.Mode.Int64),
			}
		}
	}
	hash, err := receiptHash(intent)
	if err != nil {
		return loom.DeletionIntent{}, err
	}
	if hash != rows[0].ReceiptHash {
		return loom.DeletionIntent{}, fmt.Errorf("deletion receipt node set changed")
	}
	return intent, nil
}

// RetireDeletionReceipt releases the exact verified tree in the same transaction as its physical charge.
func RetireDeletionReceipt(ctx context.Context, tx *sql.Tx, operationID, stepKey string, intent loom.DeletionIntent) error {
	retained, err := LoadDeletionReceipt(ctx, tx, operationID, stepKey, intent.Path)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(retained, intent) {
		return fmt.Errorf("deletion receipt changed before retirement")
	}
	hash, err := receiptHash(intent)
	if err != nil {
		return err
	}
	return RequireOne(dbgen.New(tx).DeleteMatchingReceipt(ctx, dbgen.DeleteMatchingReceiptParams{
		Path:        intent.Path,
		OperationID: operationID,
		StepKey:     stepKey,
		ReceiptHash: hash,
	}))
}
