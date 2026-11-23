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

// ClaimAdmission carries the prepared command and its identity for claim retries.
type ClaimAdmission struct {
	PreparedCommand
	// MAC is the message authentication code that binds the caller and original
	// command, including host secret values, for comparison on idempotent retries.
	MAC string
}

// AdmitClaim accepts one tenant claim and returns the operation responsible for delivery.
// A matching retry resumes the original operation, including while the node is draining.
func (sb *Sandboxes) AdmitClaim(ctx context.Context, principal authorization.Principal, key string, command operation.Intent) (operation.State, error) {
	var empty operation.State
	if sb.guest == nil {
		return empty, fmt.Errorf("%w: claim requires guest effects", errs.ErrInvalid)
	}
	if principal.TenantID == "" || principal.SandboxID != "" {
		return empty, fmt.Errorf("%w: claim requires tenant authority without sandbox scope", errs.ErrInvalid)
	}
	if command.Action != "claim" || command.TemplateID == "" {
		return empty, fmt.Errorf("%w: claim requires the claim action and a template", errs.ErrInvalid)
	}
	if key == "" || len(key) > 256 {
		return empty, fmt.Errorf("%w: claim requires a key of 1..256 bytes", errs.ErrInvalid)
	}
	// Validate settings before taking the per-key guard. Only redacted values may be frozen.
	prepared, err := sb.admissionPolicy.PrepareCommand(command)
	if err != nil {
		return empty, err
	}
	admission := ClaimAdmission{
		PreparedCommand: *prepared,
		MAC:             sb.identity.CommandMAC(principal, command),
	}
	unlock := sb.tenantIdempotencyLocks.Lock(principal.TenantID, key)
	defer unlock()
	if operation, found, err := sb.existing(ctx, principal, key, operation.CommandCredentials{
		MAC:     admission.MAC,
		Secrets: prepared.Secrets,
	}); found || err != nil {
		return operation, err
	}
	// Drain closes new admissions after the replay check.
	if sb.availability.IsDraining() {
		return empty, fmt.Errorf("%w: node is draining", errs.ErrExhausted)
	}
	// Verify the owned template and its local image before reserving resources.
	template, err := supply.NewTemplateCatalog(sb.store.Database).Owned(ctx, principal.TenantID, command.TemplateID)
	if err != nil {
		return empty, err
	}
	if err = host.CheckConfig(&template.Config); err != nil {
		return empty, err
	}
	if err = sb.backend.ValidateHostConfig(ctx, template.Config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, sb.store.Database, template.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(sb.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	// Prefer a ready warm VM from this snapshot. Otherwise freeze the clone or fresh request.
	request := supply.ProvisionRequest{
		TenantID: principal.TenantID,
		Key:      key,
		Template: template,
		Image:    manifest,
	}
	if template.SnapshotID != "" {
		verifiedSource, err := sb.snapshotProofs.LoadSnapshotSource(ctx, sb.store.Database, principal.TenantID, template.SnapshotID)
		if err != nil {
			return empty, err
		}
		if string(operation.Encode(template.Config)) != string(operation.Encode(verifiedSource.Snapshot.Manifest.Config)) {
			return empty, fmt.Errorf("%w: template snapshot shape/image differs", errs.ErrConflict)
		}
		if err = machine.VerifyRestoreSourceFiles(verifiedSource); err != nil {
			return empty, err
		}
		if warm, found, err := sb.warmClaims.Admit(ctx, WarmClaimRequest{
			Principal: principal,
			Key:       key,
			Admission: admission,
			Template:  template,
			Restore:   verifiedSource,
		}); found || err != nil {
			return warm, err
		}
		request.Restore = &verifiedSource
		request.Frozen = machine.FreezeProvisionRequest(machine.CloneRequest{
			TenantID:   principal.TenantID,
			Key:        key,
			SnapshotID: template.SnapshotID,
		}, &admission.Command)
	} else {
		request.Frozen = machine.FreezeProvisionRequest(machine.FreshRequest{
			TenantID:   principal.TenantID,
			Key:        key,
			TemplateID: template.ID,
		}, &admission.Command)
	}
	return sb.provisioning.Admit(ctx, ProvisionAdmission{
		Request: request,
		Claim:   &admission,
	})
}
