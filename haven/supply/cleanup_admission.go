package supply

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

type SupplyCleanupAdmission struct {
	Parent SupplyCleanupStep
	Role   string
}

func (parent *SupplyCleanupAdmission) SupplyCleanupOperation() (string, bool) {
	if parent == nil {
		return "", false
	}
	return parent.Parent.OperationID, true
}

func (parent *SupplyCleanupAdmission) AdmitResource(ctx context.Context, tx *sql.Tx, request machine.ResourceAdmission) error {
	current, err := LoadSupplyCleanup(ctx, tx, parent.Parent.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(current.Status) || current.Hash != parent.Parent.Hash || !reflect.DeepEqual(current.Input, parent.Parent.Input) {
		return fmt.Errorf("%w: supply cleanup admission changed", errs.ErrConflict)
	}
	return machine.InsertResourceLink(ctx, tx, machine.ResourceLinkTarget{
		OperationID:      current.OperationID,
		Role:             parent.Role,
		ExpectedInstance: current.Input.Source.Input.Resource.Instance,
	}, request)
}
