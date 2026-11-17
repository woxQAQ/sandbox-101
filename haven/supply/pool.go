package supply

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
	"github.com/woxQAQ/haven/types"
)

func LoadPoolReservation(ctx context.Context, query store.DBTX, id string) (PoolReservation, string, error) {
	var reservation PoolReservation
	row, err := dbgen.New(query).GetReservation(ctx, id)
	if err != nil {
		return reservation, row.Status, err
	}
	if err = json.Unmarshal(row.Body, &reservation); err != nil {
		return reservation, row.Status, err
	}
	if reservation.Template.ID != row.TemplateID || reservation.Template.Version != row.TemplateVersion || reservation.VMID != row.VmID {
		return reservation, row.Status, fmt.Errorf("pool reservation frozen identity changed")
	}
	return reservation, row.Status, nil
}

// ValidateWarmCatalog checks every ready allocation against its original refill
// and permanent template owner before the caller selects a candidate.
func ValidateWarmCatalog(proofs machine.ResourceProofs, ctx context.Context, tx *sql.Tx, selectedTemplate TemplateRecord) error {
	queries := dbgen.New(tx)
	ids, err := queries.ListWarmVMs(ctx, selectedTemplate.ID)
	if err != nil {
		return err
	}
	for _, id := range ids {
		poolID, lookup := queries.GetReservationForVM(ctx, id)
		if lookup != nil {
			return fmt.Errorf("%w: ready warm pool provenance missing", errs.ErrManualRecovery)
		}
		selected, _, err := VerifyReadyWarm(proofs, ctx, tx, poolID)
		if err != nil {
			return err
		}
		if selected.Template.ID != selectedTemplate.ID || selected.Template.TenantID != selectedTemplate.TenantID {
			return fmt.Errorf("%w: ready warm permanent owner changed", errs.ErrManualRecovery)
		}
	}
	return nil
}

// ReadyWarm distinguishes an empty pool from a selected entry whose retained
// publication cannot be verified. Only the former permits cold fallback.
func ReadyWarm(proofs machine.ResourceProofs, ctx context.Context, tx *sql.Tx, template TemplateRecord) (string, PoolReservation, machine.ResourceStep, bool, error) {
	id, err := dbgen.New(tx).FirstReady(ctx, dbgen.FirstReadyParams{
		TemplateID:      template.ID,
		SnapshotID:      template.SnapshotID,
		TemplateVersion: template.Version,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return "", PoolReservation{}, machine.ResourceStep{}, false, nil
	}
	if err != nil {
		return "", PoolReservation{}, machine.ResourceStep{}, false, err
	}
	reservation, source, err := VerifyReadyWarm(proofs, ctx, tx, id)
	return id, reservation, source, true, err
}

// SelectWarm transfers the exact ready row and warm holder in the caller's claim
// transaction. It cannot commit or dispatch the accepted activation.
func SelectWarm(ctx context.Context, tx *sql.Tx, id string, reservation PoolReservation) error {
	queries := dbgen.New(tx)
	if err := store.RequireOne(queries.ConsumeWarm(ctx, dbgen.ConsumeWarmParams{
		VmID:       reservation.VMID,
		TemplateID: reservation.Template.ID,
		SnapshotID: reservation.Template.SnapshotID,
		CreatedAt:  reservation.CreatedAt.UnixNano(),
	})); err != nil {
		return err
	}
	if err := quota.New(tx).RemoveResourceReference(ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         reservation.VMID,
		HolderKind: "warm",
		HolderID:   id,
	}); err != nil {
		return err
	}
	return store.RequireOne(queries.SelectReservation(ctx, dbgen.SelectReservationParams{
		OperationID: id,
		Body:        operation.Encode(reservation),
	}))
}

func RetireSelectedReservation(ctx context.Context, tx *sql.Tx, id string, expected []byte) error {
	return store.RequireOne(dbgen.New(tx).DeleteReservation(ctx, dbgen.DeleteReservationParams{
		OperationID: id,
		Status:      "selected",
		Body:        expected,
	}))
}

func RequireRetiredWarm(ctx context.Context, tx *sql.Tx, id string) error {
	count, lookup := dbgen.New(tx).CountReservation(ctx, id)
	if lookup != nil || count != 0 {
		return fmt.Errorf("fallback warm pool slot still occupied: %v", lookup)
	}
	return nil
}

// ObsoleteWarmCandidates preserves ready-row order and returns prior candidates
// with an error when a later publication is unreadable. It opens no new snapshot.
func ObsoleteWarmCandidates(ctx context.Context, query store.DBTX) ([]types.GCItem, error) {
	ids, err := dbgen.New(query).ListReady(ctx)
	if err != nil {
		return nil, err
	}
	var candidates []types.GCItem
	for _, id := range ids {
		reservation, _, err := LoadPoolReservation(ctx, query, id)
		if err != nil {
			return candidates, err
		}
		current, err := CurrentTemplate(ctx, query, reservation.Template.ID)
		if err != nil {
			return candidates, err
		}
		if current == nil || current.Version != reservation.Template.Version || current.TargetWarm == 0 {
			original, err := LoadSupply(ctx, query, id)
			if err != nil {
				return candidates, err
			}
			candidates = append(candidates, types.GCItem{
				Kind:     "supply",
				ID:       id,
				TenantID: reservation.Template.TenantID,
				Proof:    original.Hash,
			})
		}
	}
	return candidates, nil
}
