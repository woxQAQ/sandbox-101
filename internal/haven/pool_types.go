package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

type poolReservation struct {
	Template  templateRecord `json:"template"`
	SandboxID string         `json:"sandbox_id"`
	VMID      string         `json:"vm_id"`
	Source    generationRef  `json:"source"`
	Instance  string         `json:"instance,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

func loadPoolReservation(ctx context.Context, q queryer, id string) (poolReservation, string, error) {
	var reservation poolReservation
	var body []byte
	var template, version, vm, status string
	err := q.QueryRowContext(ctx, "SELECT template_id,template_version,vm_id,status,body FROM pool_slots WHERE operation_id=?", id).Scan(&template, &version, &vm, &status, &body)
	if err != nil {
		return reservation, status, err
	}
	if err = json.Unmarshal(body, &reservation); err != nil {
		return reservation, status, err
	}
	if reservation.Template.ID != template || reservation.Template.Version != version || reservation.VMID != vm {
		return reservation, status, fmt.Errorf("pool reservation frozen identity changed")
	}
	return reservation, status, nil
}

func (service *Service) verifyReadyWarm(ctx context.Context, tx *sql.Tx, id string) (poolReservation, resourceStep, error) {
	pool, status, err := loadPoolReservation(ctx, tx, id)
	if err != nil {
		return pool, resourceStep{}, err
	}
	if status != "ready" || pool.Instance == "" {
		return pool, resourceStep{}, fmt.Errorf("pool entry is not ready")
	}
	parent, err := loadSupply(ctx, tx, id)
	if err != nil {
		return pool, resourceStep{}, err
	}
	if parent.Status != "succeeded" || parent.State.Phase != "complete" || parent.Input.Command.Action != "refill" || !reflect.DeepEqual(parent.Input.Template, pool.Template) || parent.Input.SandboxID != pool.SandboxID {
		return pool, resourceStep{}, fmt.Errorf("warm refill publication changed")
	}
	source, err := loadStep(ctx, tx, pool.Source.OperationID)
	if err != nil {
		return pool, source, err
	}
	if source.Hash != pool.Source.InputHash || source.Status != "succeeded" || source.State.Witness != pool.Instance || source.Candidate != pool.VMID || source.Input.SandboxID != pool.SandboxID || source.Input.TenantID != pool.Template.TenantID || source.Input.SlotBudget != (slotBudget{Kind: "sandbox_slot", Units: 0}) || source.Input.Restore == nil || source.Input.Restore.Snapshot.ID != pool.Template.SnapshotID || !sameConfig(source.Input.Resource.Config, pool.Template.Config) {
		return pool, source, fmt.Errorf("warm resource birth proof differs from refill")
	}
	link, linked, err := loadResourceLink(ctx, tx, id, "clone")
	if err != nil {
		return pool, source, err
	}
	if linked != "succeeded" || link.ChildID != source.OperationID {
		return pool, source, fmt.Errorf("warm clone link changed")
	}
	if err = service.verifyGeneration(ctx, tx, source); err != nil {
		return pool, source, err
	}
	vm, instance, owner, err := service.loadVM(ctx, tx, source)
	if err != nil {
		return pool, source, err
	}
	if owner != "" || instance != pool.Instance || !vm.Verified || !vm.AgentReady || vm.Deleting {
		return pool, source, fmt.Errorf("warm current resource ownership changed")
	}
	var count int
	checks := []struct {
		q    string
		args []any
		want int
	}{
		{"SELECT COUNT(*) FROM warm WHERE vm_id=? AND template_id=? AND snapshot_id=?", []any{pool.VMID, pool.Template.ID, pool.Template.SnapshotID}, 1},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=?", []any{pool.VMID}, 2},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='warm' AND holder_id=?", []any{pool.VMID, id}, 1},
		{"SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind='sandbox_slot' AND resource_key=? AND sandbox_slots=0 AND storage_mib=0 AND source_operation_id=?", []any{pool.Template.TenantID, pool.SandboxID, source.Input.SlotOperation}, 1},
		{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind='sandbox_slot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", []any{pool.Template.TenantID, pool.SandboxID, "sandbox:" + pool.SandboxID}, 1},
		{"SELECT COUNT(*) FROM instances WHERE vm_id=?", []any{pool.VMID}, 1},
		{"SELECT COUNT(*) FROM resource_versions WHERE vm_id=?", []any{pool.VMID}, 1},
	}
	for _, check := range checks {
		if err = tx.QueryRowContext(ctx, check.q, check.args...).Scan(&count); err != nil || count != check.want {
			return pool, source, fmt.Errorf("warm responsibility incomplete: %v", err)
		}
	}
	return pool, source, nil
}
