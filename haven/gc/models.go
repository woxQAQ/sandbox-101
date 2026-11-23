package gc

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/gc/internal/dbgen"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

type input struct {
	StoreID         string          `json:"store_id"`
	RequestTenantID string          `json:"request_tenant_id"`
	Options         types.GCOptions `json:"options"`
	Deadline        time.Time       `json:"deadline"`
	Report          types.GCReport  `json:"report"`
}

type progress struct {
	Next   int            `json:"next"`
	Report types.GCReport `json:"report"`
}

type Step struct {
	OperationID string
	Hash        string
	Status      string
	Input       input
	State       progress
}

func Load(ctx context.Context, database store.DBTX, id string) (Step, error) {
	var step Step
	err := store.ReadSnapshot(ctx, database, func(tx *sql.Tx) error {
		var err error
		step, err = readStep(ctx, tx, id)
		return err
	})
	return step, err
}

func readStep(ctx context.Context, database *sql.Tx, id string) (Step, error) {
	step := Step{
		OperationID: id,
	}
	record, err := dbgen.New(database).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "gc",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	payload := record.Input
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(payload) != step.Hash {
		return step, fmt.Errorf("GC input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	queries := dbgen.New(database)
	next, err := queries.GetGCProgress(ctx, id)
	if err != nil {
		return step, fmt.Errorf("cannot read GC progress for %s: %v", id, err)
	}
	step.State.Next = int(next)
	step.State.Report = step.Input.Report
	if record.CompletedAt != "" {
		step.State.Report.FinishedAt, err = time.Parse(time.RFC3339Nano, record.CompletedAt)
		if err != nil {
			return step, err
		}
	}
	rows, err := queries.ListGCOutcomes(ctx, id)
	if err != nil {
		return step, err
	}
	for _, row := range rows {
		item := types.GCItem{
			Kind:         row.Kind,
			ID:           row.ResourceID,
			TenantID:     row.TenantID,
			CacheBuilder: row.CacheBuilder,
			CacheDiffID:  row.CacheDiffID,
			Proof:        row.Proof,
			OperationID:  row.ChildOperationID,
			Reason:       row.Reason,
		}
		items := &step.State.Report.Collected
		if row.Disposition == "deferred" {
			items = &step.State.Report.Deferred
		}
		if int64(len(*items)) != row.Ordinal {
			return step, fmt.Errorf("GC outcome sequence changed")
		}
		*items = append(*items, item)
	}
	if !operation.Terminal(step.Status) && len(rows) != step.State.Next {
		return step, fmt.Errorf("GC cursor differs from its durable outcomes")
	}
	if action != "gc" || candidate != step.Input.StoreID || !loom.ValidResourceID(candidate, "store-") || expected != "" || step.State.Next < 0 || step.State.Next > len(step.Input.Report.Candidates) {
		return step, fmt.Errorf("GC durable progress identity changed")
	}
	return step, nil
}
