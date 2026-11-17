package sandbox

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/supply"
)

// verifyProvisionSources rechecks preflight facts inside the allocation
// transaction before writing any candidate, ownership or budget records.
func (service *VMProvisioning) verifyProvisionSources(ctx context.Context, tx *sql.Tx, admission ProvisionAdmission) error {
	request := admission.Request
	selected, restore := request.Template, request.Restore
	if admission.Claim != nil {
		current, err := supply.NewTemplateCatalog(tx).Owned(ctx, request.TenantID, selected.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, selected) {
			return fmt.Errorf("%w: template publication changed during preflight", errs.ErrConflict)
		}
	}
	provisional := false
	if admission.Target != nil {
		if declared, ok := provisionalTemplate(admission.Target.Parent); ok {
			if !reflect.DeepEqual(declared, selected) {
				return fmt.Errorf("%w: provisional resource template differs from supply intent", errs.ErrConflict)
			}
			provisional = true
		}
	}
	if restore == nil && !provisional {
		catalog := supply.NewTemplateCatalog(tx)
		loader := catalog.Cold
		if admission.Claim != nil {
			loader = catalog.Owned
		}
		current, err := loader(ctx, request.TenantID, selected.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, selected) {
			return fmt.Errorf("%w: template publication changed during preflight", errs.ErrConflict)
		}
	} else if restore != nil {
		current, err := service.proofs.Snapshots().LoadRestoreSource(ctx, tx, request.TenantID, restore.Snapshot.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, *restore) {
			return fmt.Errorf("%w: snapshot publication changed during preflight", errs.ErrConflict)
		}
	}
	currentImage, err := image.LoadManifest(ctx, tx, selected.Config.Image)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(currentImage, request.Image) {
		return fmt.Errorf("%w: image publication changed during preflight", errs.ErrConflict)
	}
	return nil
}
