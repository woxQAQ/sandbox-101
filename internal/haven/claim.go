package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

func (service *Service) existingSandboxOperation(ctx context.Context, principal Principal, key string, admission *claimAdmission) (Operation, bool, error) {
	var id string
	err := service.store.db.QueryRowContext(ctx, "SELECT id FROM operations WHERE scope_kind='tenant' AND scope_id=? AND idem_key=?", principal.TenantID, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, false, nil
	}
	if err != nil {
		return Operation{}, false, err
	}
	operation, err := service.Operation(ctx, principal, id)
	if err != nil {
		return operation, true, err
	}
	if err = service.credentialsMatch(id, admission); err != nil {
		return operation, true, err
	}
	if !terminal(operation.Status) {
		service.mu.Lock()
		service.opSecrets[id] = admission.Secrets
		service.mu.Unlock()
		service.dispatch(id)
	}
	return operation, true, nil
}

func (service *Service) AdmitClaim(ctx context.Context, principal Principal, key string, command Command) (Operation, error) {
	var empty Operation
	if service.opts.Guest == nil || principal.TenantID == "" || principal.SandboxID != "" || command.Action != "claim" || command.TemplateID == "" || key == "" || len(key) > 256 {
		return empty, loom.Fail(loom.Invalid, "claim requires tenant authority, template, key and guest effects")
	}
	requestGuard := service.vmLock("request:" + string(encode([]string{principal.TenantID, key})))
	requestGuard.Lock()
	defer requestGuard.Unlock()
	admission, err := service.prepareClaim(command)
	if err != nil {
		return empty, err
	}
	admission.MAC = service.store.commandMAC(principal, command)
	if operation, found, err := service.existingSandboxOperation(ctx, principal, key, admission); found || err != nil {
		return operation, err
	}
	service.mu.Lock()
	draining := service.draining
	service.mu.Unlock()
	if draining {
		return empty, loom.Fail(loom.Exhausted, "node is draining")
	}
	template, err := loadOwnedTemplate(ctx, service.store.db, principal.TenantID, command.TemplateID)
	if err != nil {
		return empty, err
	}
	if err = host.CheckConfig(&template.Config); err != nil {
		return empty, err
	}
	if err = service.opts.Backend.Check(ctx, template.Config); err != nil {
		return empty, err
	}
	image, err := loadImage(ctx, service.store.db, template.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = verifyImageFiles(image, filepath.Join(service.store.directory, "images", image.Digest[7:])); err != nil {
		return empty, err
	}
	var source *restoreSource
	var frozen []byte
	if template.SnapshotID != "" {
		snapshot, err := service.loadRestoreSource(ctx, service.store.db, principal.TenantID, template.SnapshotID)
		if err != nil {
			return empty, err
		}
		if !sameConfig(template.Config, snapshot.Snapshot.Manifest.Config) {
			return empty, loom.Fail(loom.Conflict, "template snapshot shape/image differs")
		}
		if err = verifyRestoreSourceFiles(snapshot); err != nil {
			return empty, err
		}
		source = &snapshot
		frozen = encode(struct {
			Action  string
			Request CloneRequest
			Claim   Command
		}{"clone", CloneRequest{TenantID: principal.TenantID, Key: key, SnapshotID: template.SnapshotID}, admission.Command})
	} else {
		frozen = encode(struct {
			Action  string
			Request ColdRequest
			Claim   Command
		}{"claim", ColdRequest{TenantID: principal.TenantID, Key: key, TemplateID: template.ID}, admission.Command})
	}
	if source != nil {
		if warm, found, err := service.admitWarmClaim(ctx, principal, key, *admission, template, *source); found || err != nil {
			return warm, err
		}
	}
	return service.admitProvision(ctx, principal.TenantID, key, frozen, digest(frozen), template, image, source, admission, nil)
}

func sameConfig(first, second loom.Config) bool {
	return string(encode(first)) == string(encode(second))
}

func (service *Service) admitActivation(tx *sql.Tx, operation Operation, resource stepInput, claim claimAdmission) error {
	claim.Settings.Generation = operation.ID
	input := activationInput{RequestTenantID: resource.TenantID, TenantID: resource.TenantID, SandboxID: resource.SandboxID, ResourceOperation: operation.ID, ResourceHash: digest(encode(resource)), ExpectedInstance: resource.Resource.Instance, Settings: claim.Settings, ResetIdentity: true}
	payload := encode(input)
	if claim.MAC != "" {
		if err := requireWrite(tx.ExecContext(service.ctx, "INSERT INTO operation_credentials VALUES(?,?)", operation.ID, claim.MAC)); err != nil {
			return err
		}
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO sandbox_settings VALUES(?,?)", []any{resource.SandboxID, encode(claim.Settings)}},
		{"INSERT INTO configuration_fences VALUES(?,?)", []any{resource.SandboxID, operation.ID}},
		{"INSERT INTO operation_steps VALUES(?,'activation','activate',?,?,?,?,'pending',?)", []any{operation.ID, digest(payload), payload, resource.Resource.VMID, resource.Resource.Instance, encode("admitted")}},
	} {
		if err := requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	return nil
}

func loadSettings(ctx context.Context, q queryer, id string) (sandboxSettings, error) {
	var settings sandboxSettings
	var body []byte
	if err := q.QueryRowContext(ctx, "SELECT body FROM sandbox_settings WHERE sandbox_id=?", id).Scan(&body); err != nil {
		return settings, err
	}
	return settings, json.Unmarshal(body, &settings)
}
