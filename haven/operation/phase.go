package operation

import (
	"context"
	"database/sql"

	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// PhaseIntent binds frozen workflow input to its initial progress.
type PhaseIntent struct {
	OperationID         string
	StepKey             string
	Action              string
	CandidateResourceID string
	ExpectedInstance    string
	Status              string
	Phase               string
	SnapshotID          string
	Input               []byte
}

func InsertPhaseStep(ctx context.Context, tx *sql.Tx, intent PhaseIntent) error {
	return store.RequireOne(dbgen.New(tx).InsertPhaseStep(ctx, dbgen.InsertPhaseStepParams{
		OperationID:         intent.OperationID,
		StepKey:             intent.StepKey,
		Action:              intent.Action,
		InputHash:           Digest(intent.Input),
		Input:               intent.Input,
		CandidateResourceID: intent.CandidateResourceID,
		ExpectedInstance:    intent.ExpectedInstance,
		Status:              intent.Status,
		Phase: sql.NullString{
			String: intent.Phase,
			Valid:  true,
		},
		ResultSnapshotID: intent.SnapshotID,
	}))
}

// PhaseChange binds a successor to the exact live predecessor.
type PhaseChange struct {
	OperationID      string
	StepKey          string
	InputHash        string
	PreviousStatus   string
	PreviousPhase    string
	PreviousSnapshot string
	NextStatus       string
	NextPhase        string
	NextSnapshot     string
}

func AdvancePhase(ctx context.Context, query store.DBTX, change PhaseChange) error {
	return store.RequireOne(dbgen.New(query).AdvancePhase(ctx, dbgen.AdvancePhaseParams{
		OperationID:    change.OperationID,
		StepKey:        change.StepKey,
		InputHash:      change.InputHash,
		PreviousStatus: change.PreviousStatus,
		PreviousPhase: sql.NullString{
			String: change.PreviousPhase,
			Valid:  true,
		},
		PreviousSnapshot: change.PreviousSnapshot,
		NextStatus:       change.NextStatus,
		NextPhase: sql.NullString{
			String: change.NextPhase,
			Valid:  true,
		},
		NextSnapshot: change.NextSnapshot,
	}))
}
