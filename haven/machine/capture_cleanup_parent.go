package machine

import (
	"context"
	"database/sql"
)

// CaptureCleanupParentAuthority verifies a frozen rollback's raw capture and
// original stop child before resource cleanup admission. It performs no effects.
type CaptureCleanupParentAuthority interface {
	VerifyCaptureCleanup(context.Context, *sql.Tx, CaptureStep, string) error
}
