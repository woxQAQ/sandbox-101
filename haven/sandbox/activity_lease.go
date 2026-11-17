package sandbox

import (
	"context"
	"time"

	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// RenewActivityLease extends the verified configuration under the caller's
// continuous configuration guard. Admission, binding checks and checkpointing
// remain with the data caller; the original settings body is the CAS witness.
func RenewActivityLease(ctx context.Context, query store.DBTX, id string, previous Settings) (time.Time, error) {
	next := previous
	next.Deadline = time.Now().UTC().Add(time.Duration(next.TTLSeconds) * time.Second)
	err := store.RequireOne(dbgen.New(query).ReplaceSettings(ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    id,
		PreviousBody: operation.Encode(previous),
		NextBody:     operation.Encode(next),
	}))
	return next.Deadline, err
}
