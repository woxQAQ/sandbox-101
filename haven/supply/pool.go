package supply

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/ownership"
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
	template, err := loadTemplateVersion(ctx, query, row.TemplateVersion)
	if err != nil {
		return reservation, row.Status, err
	}
	if template.ID != row.TemplateID {
		return reservation, row.Status, fmt.Errorf("pool reservation frozen template changed")
	}
	return PoolReservation{
		Template:  template,
		VMID:      row.VmID,
		SandboxID: row.SandboxID,
		Instance:  row.Instance,
		CreatedAt: time.Unix(0, row.CreatedAtUnixNs).UTC(),
		Source: ownership.GenerationRef{
			OperationID: row.SourceOperationID,
			InputHash:   row.SourceInputHash,
		},
	}, row.Status, nil
}

// ValidateWarmCatalog checks every ready allocation against its original refill
// and permanent template owner before the caller selects a candidate.
func ValidateWarmCatalog(ctx context.Context, proofs machine.ResourceProofs, tx *sql.Tx, selectedTemplate TemplateRecord) error {
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
		selected, _, err := VerifyReadyWarm(ctx, proofs, tx, poolID)
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
func ReadyWarm(ctx context.Context, proofs machine.ResourceProofs, tx *sql.Tx, template TemplateRecord) (string, PoolReservation, machine.ResourceStep, bool, error) {
	id, err := dbgen.New(tx).FirstReady(ctx, dbgen.FirstReadyParams{
		TemplateID:      template.ID,
		TemplateVersion: template.Version,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return "", PoolReservation{}, machine.ResourceStep{}, false, nil
	}
	if err != nil {
		return "", PoolReservation{}, machine.ResourceStep{}, false, err
	}
	reservation, source, err := VerifyReadyWarm(ctx, proofs, tx, id)
	return id, reservation, source, true, err
}

// SelectWarm transfers the exact ready row and warm holder in the caller's claim
// transaction. It cannot commit or dispatch the accepted activation.
func SelectWarm(ctx context.Context, tx *sql.Tx, id string, reservation PoolReservation) error {
	queries := dbgen.New(tx)
	if err := quota.RemoveResourceReference(ctx, tx, quota.ResourceReference{
		Kind:       "vm",
		ID:         reservation.VMID,
		HolderKind: "warm",
		HolderID:   id,
	}); err != nil {
		return err
	}
	return store.RequireOne(queries.SelectReservation(ctx, dbgen.SelectReservationParams{
		OperationID:     id,
		VmID:            reservation.VMID,
		Instance:        reservation.Instance,
		SourceInputHash: reservation.Source.InputHash,
		TemplateVersion: reservation.Template.Version,
	}))
}

func RetireSelectedReservation(ctx context.Context, tx *sql.Tx, id string, expected PoolReservation) error {
	return store.RequireOne(dbgen.New(tx).DeleteReservation(ctx, dbgen.DeleteReservationParams{
		OperationID:     id,
		Status:          "selected",
		Instance:        expected.Instance,
		SourceInputHash: expected.Source.InputHash,
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

// VerifyReadyWarm verifies the original refill publication, generation and zero-slot
// funding before a selection or collector can transfer the pool responsibility.
func VerifyReadyWarm(ctx context.Context, proofs machine.ResourceProofs, tx *sql.Tx, id string) (PoolReservation, machine.ResourceStep, error) {
	queries := dbgen.New(tx)
	reservation, status, err := LoadPoolReservation(ctx, tx, id)
	if err != nil {
		return reservation, machine.ResourceStep{}, err
	}
	if status != "ready" || reservation.Instance == "" {
		return reservation, machine.ResourceStep{}, fmt.Errorf("pool entry is not ready")
	}
	parent, err := LoadSupply(ctx, tx, id)
	if err != nil {
		return reservation, machine.ResourceStep{}, err
	}
	if parent.Status != "succeeded" || parent.State.Phase != "complete" || parent.Input.Command.Action != "refill" || !reflect.DeepEqual(parent.Input.Template, reservation.Template) || parent.Input.SandboxID != reservation.SandboxID {
		return reservation, machine.ResourceStep{}, fmt.Errorf("warm refill publication changed")
	}
	source, err := machine.LoadResourceStep(ctx, tx, reservation.Source.OperationID)
	if err != nil {
		return reservation, source, err
	}
	if source.Hash != reservation.Source.InputHash || source.Status != "succeeded" {
		return reservation, source, fmt.Errorf("warm resource step differs from the successful refill source")
	}
	if source.State.Witness != reservation.Instance || source.Candidate != reservation.VMID {
		return reservation, source, fmt.Errorf("warm VM or instance differs from reservation")
	}
	input := source.Input
	if input.SandboxID != reservation.SandboxID || input.TenantID != reservation.Template.TenantID {
		return reservation, source, fmt.Errorf("warm resource sandbox or tenant differs from reservation")
	}
	warmBudget := quota.SlotBudget{
		Kind:  "sandbox_slot",
		Units: 0,
	}
	if input.SlotBudget != warmBudget {
		return reservation, source, fmt.Errorf("warm resource requires a zero-unit sandbox slot budget")
	}
	if input.Restore == nil || input.Restore.Snapshot.ID != reservation.Template.SnapshotID {
		return reservation, source, fmt.Errorf("warm restore source differs from template snapshot")
	}
	if !input.Resource.Config.Equal(reservation.Template.Config) {
		return reservation, source, fmt.Errorf("warm resource configuration differs from template")
	}
	link, linked, err := machine.LoadResourceLink(ctx, tx, id, "clone")
	if err != nil {
		return reservation, source, err
	}
	if linked != "succeeded" || link.ChildID != source.OperationID {
		return reservation, source, fmt.Errorf("warm clone link changed")
	}
	if err = proofs.VerifyGeneration(ctx, tx, source); err != nil {
		return reservation, source, err
	}
	currentMachine, instance, owner, err := machine.LoadVM(ctx, tx, source)
	if err != nil {
		return reservation, source, err
	}
	if owner != "" || instance != reservation.Instance || !currentMachine.Verified || !currentMachine.AgentReady || currentMachine.Deleting {
		return reservation, source, fmt.Errorf("warm current resource ownership changed")
	}
	warm, err := queries.CountWarmIdentity(ctx, dbgen.CountWarmIdentityParams{
		VmID:       reservation.VMID,
		TemplateID: reservation.Template.ID,
		SnapshotID: reservation.Template.SnapshotID,
	})
	if err != nil || warm != 1 {
		return reservation, source, fmt.Errorf("warm responsibility incomplete: %v", err)
	}
	checks := []struct {
		read func() (int64, error)
		want int64
	}{
		{func() (int64, error) {
			return quota.CountResourceHolders(ctx, tx, "vm", reservation.VMID)
		}, 2},
		{func() (int64, error) {
			return quota.CountResourceReference(ctx, tx, quota.ResourceReference{
				Kind:       "vm",
				ID:         reservation.VMID,
				HolderKind: "warm",
				HolderID:   id,
			})
		}, 1},
		{func() (int64, error) {
			return quota.CountMatchingCost(ctx, tx, quota.Cost{
				CostIdentity: quota.CostIdentity{
					TenantID:        reservation.Template.TenantID,
					Kind:            "sandbox_slot",
					Key:             reservation.SandboxID,
					SourceOperation: source.Input.SlotOperation,
				},
			})
		}, 1},
		{func() (int64, error) {
			return quota.CountCostReference(ctx, tx, quota.CostReference{
				TenantID:   reservation.Template.TenantID,
				Kind:       "sandbox_slot",
				Key:        reservation.SandboxID,
				HolderKind: "active_use",
				HolderID:   "sandbox:" + reservation.SandboxID,
			})
		}, 1},
	}
	for _, check := range checks {
		if count, err := check.read(); err != nil || count != check.want {
			return reservation, source, fmt.Errorf("warm responsibility incomplete: %v", err)
		}
	}

	if count, err := machine.RetainedInstanceCount(ctx, tx, reservation.VMID); err != nil || count != 1 {
		return reservation, source, fmt.Errorf("warm responsibility incomplete: %v", err)
	}
	if count, err := machine.RetainedVersionCount(ctx, tx, reservation.VMID); err != nil || count != 1 {
		return reservation, source, fmt.Errorf("warm responsibility incomplete: %v", err)
	}
	return reservation, source, nil
}
