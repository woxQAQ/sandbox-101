package recovery

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// Recovery validates frozen workflows and publications for all store entrypoints.
// It owns no running service, backend, worker, or mutable store connection.
type Recovery struct{}

func NewRecovery() *Recovery {
	return &Recovery{}
}

func (r *Recovery) Validation() store.Validation {
	if r == nil {
		return store.Validation{}
	}
	return store.Validation{
		Publications: r.publications,
		Workflows:    r.workflows,
	}
}

func (r *Recovery) workflows(ctx context.Context, database *sql.DB, origin store.Origin) error {
	entries, err := operation.New(database).RecoverySteps(ctx)
	if err != nil {
		return err
	}
	for _, item := range entries {
		hash := sha256.Sum256(item.Input)
		if hex.EncodeToString(hash[:]) != item.Hash || !json.Valid(item.Input) || !json.Valid(item.Body) {
			return fmt.Errorf("invalid durable step encoding: %s/%s", item.OperationID, item.Key)
		}
		if err := validateWorkflowProof(ctx, database, origin, item); err != nil {
			return fmt.Errorf("durable step %s/%s requires recovery: %w", item.OperationID, item.Key, err)
		}
	}
	return nil
}

// publications checks immutable admission/terminal identities. Current sandbox
// state cannot recreate a historical delivery or repair a missing publication.
func (r *Recovery) publications(ctx context.Context, database *sql.DB) error {
	repository := operation.New(database)
	missing, err := repository.MissingPublications(ctx)
	if err != nil {
		return err
	}
	if missing != 0 {
		return fmt.Errorf("terminal public operation lacks frozen result")
	}
	proofs, err := repository.PublicationProofs(ctx)
	if err != nil {
		return err
	}
	for _, proof := range proofs {
		var view types.Operation
		var result operation.Result
		if err = json.Unmarshal(proof.Publication, &view); err != nil {
			return err
		}
		if err = json.Unmarshal(proof.Result, &result); err != nil {
			return err
		}
		command, err := operation.AcceptedCommand(proof.Request)
		if err != nil {
			return err
		}
		start, err := time.Parse(time.RFC3339Nano, proof.CreatedAt)
		if err != nil {
			return err
		}
		end, err := time.Parse(time.RFC3339Nano, proof.FinishedAt)
		if err != nil {
			return err
		}
		ids := result.SandboxIDs
		if len(ids) == 0 && result.SandboxID != "" {
			ids = []string{
				result.SandboxID,
			}
		}
		terminal := proof.Status == "succeeded" || proof.Status == "failed"
		if proof.ScopeKind != "tenant" || !terminal || view.ID != proof.ID || view.Key != proof.Key || view.TenantID != proof.ScopeID || view.Status != proof.Status || view.Result.OperationID != proof.ID || view.Result.Tokens != nil || view.Error != nil || !view.CreatedAt.Equal(start) || !view.FinishedAt.Equal(end) || !reflect.DeepEqual(view.Command, command) || !slices.Equal(view.IDs, ids) {
			return fmt.Errorf("frozen delivery differs from accepted operation %s", proof.ID)
		}
		if view.Result.Checkpoint != nil && view.Result.Checkpoint.ID != result.CheckpointID {
			return fmt.Errorf("frozen checkpoint delivery differs from terminal result")
		}
		if view.Result.Template != nil && view.Result.Template.ID != result.TemplateID {
			return fmt.Errorf("frozen template delivery differs from terminal result")
		}
		sandboxes := view.Result.Sandboxes
		if view.Result.Sandbox != nil {
			sandboxes = append(append([]types.Sandbox(nil), sandboxes...), *view.Result.Sandbox)
		}
		for _, sandbox := range sandboxes {
			if !slices.Contains(ids, sandbox.ID) || sandbox.Epoch == "" || sandbox.CreatedAt.IsZero() {
				return fmt.Errorf("frozen sandbox delivery identity is incomplete")
			}
		}
	}
	return nil
}
