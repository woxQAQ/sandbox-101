package sandbox_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/sandbox"
)

func TestHavenCapabilityReadersBorrowReadOnlySnapshot(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _ := testapp.ColdService(t, nil)
	accepted := testfixture.ColdClaim(t, service, "readonly-proof")
	source := testutil.AwaitColdWorker(t, service, accepted.ID)
	if source.Status != "succeeded" {
		t.Fatal(source)
	}
	ctx := context.Background()
	tx, err := service.State.Database().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := tx.ExecContext(ctx, "PRAGMA query_only=OFF"); err != nil {
			t.Error(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Error(err)
		}
	}()
	if _, err = tx.ExecContext(ctx, "PRAGMA query_only=ON"); err != nil {
		t.Fatal(err)
	}
	expected, err := sandbox.LoadSandbox(ctx, tx, source.Input.SandboxID, source.Input.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	for _, reader := range []machine.SandboxOwnershipReader{
		sandbox.OwnershipHistory{},
		sandbox.StopHistory{},
		sandbox.CaptureDiscardHistory{},
		sandbox.DeletionHistory{},
		sandbox.SupplyCleanupHistory{},
	} {
		actual, err := reader.ReadSandbox(ctx, tx, expected.ID, expected.TenantID)
		if err != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatalf("%T did not consume the supplied snapshot: %+v %v", reader, actual, err)
		}
		if _, err = reader.ReadSandbox(ctx, tx, expected.ID, "another-tenant"); err == nil {
			t.Fatalf("%T accepted foreign ownership", reader)
		}
	}
	if _, err = (sandbox.ActivationHistory{}).ReadActivationProof(ctx, tx, accepted.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("raw generation acquired an inferred activation proof: %v", err)
	}
	if _, err = tx.ExecContext(ctx, "UPDATE sandboxes SET body=body WHERE id=?", expected.ID); err == nil {
		t.Fatal("proof reader fixture did not enforce a read-only SQLite connection")
	}
}
