package gc

import (
	"context"
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
	payload, body := record.Input, record.Body
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
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != "gc" || candidate != step.Input.StoreID || !loom.ValidResourceID(candidate, "store-") || expected != "" || step.State.Next < 0 || step.State.Next > len(step.Input.Report.Candidates) {
		return step, fmt.Errorf("GC durable progress identity changed")
	}
	return step, nil
}
