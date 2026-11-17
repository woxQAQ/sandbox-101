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

func (sb *Sandboxes) AdmitClaim(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	var empty operation.Record
	if sb.opts.Guest == nil || principal.TenantID == "" || principal.SandboxID != "" || command.Action != "claim" || command.TemplateID == "" || key == "" || len(key) > 256 {
		return empty, fmt.Errorf("%w: claim requires tenant authority, template, key and guest effects", errs.ErrInvalid)
	}
	requestGuard := sb.guards.TenantRequest(principal.TenantID, key)
	requestGuard.Lock()
	defer requestGuard.Unlock()
	admission, err := sb.commands.Prepare(command)
	if err != nil {
		return empty, err
	}
	admission.MAC = sb.commands.Identity.CommandMAC(principal, command)
	if operation, found, err := sb.existing(ctx, principal, key, admission); found || err != nil {
		return operation, err
	}
	draining := sb.availability.IsDraining()
	if draining {
		return empty, fmt.Errorf("%w: node is draining", errs.ErrExhausted)
	}
	template, err := supply.NewTemplateCatalog(sb.store.Database).Owned(ctx, principal.TenantID, command.TemplateID)
	if err != nil {
		return empty, err
	}
	if err = host.CheckConfig(&template.Config); err != nil {
		return empty, err
	}
	if err = sb.opts.Backend.Check(ctx, template.Config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, sb.store.Database, template.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(sb.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	var source *machine.SnapshotSource
	var frozen []byte
	if template.SnapshotID != "" {
		snapshot, err := sb.history.LoadRestoreSource(ctx, sb.store.Database, principal.TenantID, template.SnapshotID)
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
			Request machine.FreshRequest
			Claim   operation.Command
		}{"claim", machine.FreshRequest{
			TenantID:   principal.TenantID,
			Key:        key,
			TemplateID: template.ID,
		}, admission.Command})
	}
	if source != nil {
		if warm, found, err := sb.warmClaims.Admit(ctx, WarmClaimRequest{
			Principal: principal,
			Key:       key,
			Admission: *admission,
			Template:  template,
			Restore:   *source,
		}); found || err != nil {
			return warm, err
		}
	}
	return sb.provisioning.Admit(ctx, ProvisionAdmission{
		Request: supply.ProvisionRequest{
			TenantID: principal.TenantID,
			Key:      key,
			Frozen:   frozen,
			Template: template,
			Image:    manifest,
			Restore:  source,
		},
		Claim: admission,
	})
}
