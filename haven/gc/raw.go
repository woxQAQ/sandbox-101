package gc

import (
	"context"
	"database/sql"
	"errors"

	"github.com/woxQAQ/haven/gc/internal/dbgen"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/types"
)

func (service *Collector) rawCandidates(ctx context.Context) ([]types.GCItem, []types.GCItem, error) {
	queries := dbgen.New(service.store.Database)
	ids, err := queries.ListPendingCleanupOperations(ctx, "capture")
	if err != nil {
		return nil, nil, err
	}
	var candidates, deferred []types.GCItem
	for _, id := range ids {
		capture, err := machine.LoadCapture(ctx, service.store.Database, id)
		if err != nil {
			return nil, nil, err
		}
		if capture.Status != "succeeded" {
			continue
		}
		source := capture.Input.Source
		current, err := machine.LoadOwnership(ctx, service.store.Database, source.Candidate)
		if errors.Is(err, sql.ErrNoRows) {
			deferred = append(deferred, types.GCItem{
				Kind:   "raw_capture",
				ID:     id,
				Reason: "source lacks independent successful reclamation/stop proof",
			})
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		if current.State != "stopped" || current.Owned || current.Instance != source.State.Witness {
			deferred = append(deferred, types.GCItem{
				Kind:   "raw_capture",
				ID:     id,
				Reason: "source remains active or owned",
			})
			continue
		}
		stops, err := queries.ListSuccessfulStops(ctx, dbgen.ListSuccessfulStopsParams{
			CandidateResourceID: source.Input.SandboxID,
			ExpectedInstance:    source.State.Witness,
		})
		if err != nil {
			return nil, nil, err
		}
		selected := ""
		for _, stop := range stops {
			proof, err := machine.LoadStop(ctx, service.store.Database, stop)
			if err != nil {
				return nil, nil, err
			}
			if proof.Input.SourceOperation == source.OperationID && proof.Input.SourceHash == source.Hash && proof.Phase == "complete" {
				selected = stop
				break
			}
		}
		if selected == "" {
			deferred = append(deferred, types.GCItem{
				Kind:   "raw_capture",
				ID:     id,
				Reason: "original stopped generation witness missing",
			})
			continue
		}
		candidates = append(candidates, types.GCItem{
			Kind:        "raw_capture",
			ID:          id,
			TenantID:    capture.Input.Request.TenantID,
			Proof:       capture.Hash,
			OperationID: selected,
		})
	}
	return candidates, deferred, nil
}
