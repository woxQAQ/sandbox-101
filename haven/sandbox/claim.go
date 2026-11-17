package sandbox

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/supply"
)

func (service *Sandboxes) AdmitClaim(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	var empty operation.Record
	if service.opts.Guest == nil || principal.TenantID == "" || principal.SandboxID != "" || command.Action != "claim" || command.TemplateID == "" || key == "" || len(key) > 256 {
		return empty, fmt.Errorf("%w: claim requires tenant authority, template, key and guest effects", errs.ErrInvalid)
	}
	requestGuard := service.guards.TenantRequest(principal.TenantID, key)
	requestGuard.Lock()
	defer requestGuard.Unlock()
	admission, err := service.commands.Prepare(command)
	if err != nil {
		return empty, err
	}
	admission.MAC = service.commands.Identity.CommandMAC(principal, command)
	if operation, found, err := service.existing(ctx, principal, key, admission); found || err != nil {
		return operation, err
	}
	draining := service.availability.IsDraining()
	if draining {
		return empty, fmt.Errorf("%w: node is draining", errs.ErrExhausted)
	}
	template, err := supply.NewTemplateCatalog(service.store.Database).Owned(ctx, principal.TenantID, command.TemplateID)
	if err != nil {
		return empty, err
	}
	if err = host.CheckConfig(&template.Config); err != nil {
		return empty, err
	}
	if err = service.opts.Backend.Check(ctx, template.Config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, service.store.Database, template.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(service.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	var source *machine.Source
	var frozen []byte
	if template.SnapshotID != "" {
		snapshot, err := service.history.LoadRestoreSource(ctx, service.store.Database, principal.TenantID, template.SnapshotID)
		if err != nil {
			return empty, err
		}
		if string(operation.Encode(template.Config)) != string(operation.Encode(snapshot.Snapshot.Manifest.Config)) {
			return empty, fmt.Errorf("%w: template snapshot shape/image differs", errs.ErrConflict)
		}
		if err = machine.VerifyRestoreSourceFiles(snapshot); err != nil {
			return empty, err
		}
		source = &snapshot
		frozen = operation.Encode(struct {
			Action  string
			Request machine.CloneRequest
			Claim   operation.Command
		}{"clone", machine.CloneRequest{
			TenantID:   principal.TenantID,
			Key:        key,
			SnapshotID: template.SnapshotID,
		}, admission.Command})
	} else {
		frozen = operation.Encode(struct {
			Action  string
			Request machine.ColdRequest
			Claim   operation.Command
		}{"claim", machine.ColdRequest{
			TenantID:   principal.TenantID,
			Key:        key,
			TemplateID: template.ID,
		}, admission.Command})
	}
	if source != nil {
		if warm, found, err := service.warmClaims.Admit(ctx, principal, key, *admission, template, *source); found || err != nil {
			return warm, err
		}
	}
	return service.provisioning.Admit(ctx, principal.TenantID, key, frozen, operation.Digest(frozen), template, manifest, source, admission, nil)
}
