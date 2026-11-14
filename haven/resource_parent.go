package haven

import (
	"context"
	"database/sql"
)

// resourceParent joins a control workflow to a resource admission transaction.
// Implementations belong to Haven; Loom never receives this control interface.
type resourceParent interface {
	admitResource(context.Context, *Service, *sql.Tx, Operation, string, string, string) error
}

func (parent *lifecycleAdmission) admitResource(ctx context.Context, service *Service, tx *sql.Tx, child Operation, kind, candidate, hash string) error {
	return service.admitLifecycleLink(ctx, tx, parent, child, kind, candidate, hash)
}

func lifecycleParent(parent resourceParent) *lifecycleAdmission {
	lifecycle, _ := parent.(*lifecycleAdmission)
	return lifecycle
}

func (service *Service) admitResourceParent(ctx context.Context, tx *sql.Tx, parent resourceParent, child Operation, kind, candidate, hash string) error {
	if parent == nil {
		return nil
	}
	return parent.admitResource(ctx, service, tx, child, kind, candidate, hash)
}
