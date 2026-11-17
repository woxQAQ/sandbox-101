package component

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
)

func TestHavenCaptureDiscardRequiresOwnPendingResult(t *testing.T) {
	parallelLifecycle(t)
	for _, field := range []string{
		"sandbox_id",
		"vm_id",
		"snapshot_id",
	} {
		t.Run(field, func(t *testing.T) {
			service, _, request, failed := failedCaptureFixtureWithFault(t, true, func(point string) error {
				if point == "capture_discard_admitted" {
					return errs.ErrInterrupted
				}
				return nil
			})
			ctx := context.Background()
			accepted, err := service.CaptureDiscard.AdmitCaptureDiscard(ctx, request)
			if !errors.Is(err, errs.ErrInterrupted) || accepted.ID == "" {
				t.Fatal("fixture did not stop after committed admission", accepted, err)
			}
			var body []byte
			if err = service.State.Database().QueryRowContext(ctx, "SELECT body FROM operations WHERE id=?", accepted.ID).Scan(&body); err != nil {
				t.Fatal(err)
			}
			var result map[string]json.RawMessage
			if err = json.Unmarshal(body, &result); err != nil {
				t.Fatal(err)
			}
			if _, found := result[field]; !found {
				t.Fatal("fixture result lacks the original field", field)
			}
			delete(result, field)
			body, err = json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.State.Database().ExecContext(ctx, "UPDATE operations SET body=? WHERE id=? AND status='pending'", body, accepted.ID); err != nil {
				t.Fatal(err)
			}
			if handled, err := service.CaptureDiscard.Resume(accepted.ID); !handled || err == nil {
				t.Fatal("discard inherited missing identity from the failed capture", handled, err)
			}
			step, err := loadCaptureDiscard(ctx, service.State.Database(), accepted.ID)
			if err != nil || step.Phase != "admitted" || terminal(step.Status) {
				t.Fatal("incomplete result advanced the discard", step, err)
			}
			var receipts int
			if err = service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM deletion_intents WHERE operation_id=?", accepted.ID).Scan(&receipts); err != nil || receipts != 0 {
				t.Fatal("incomplete result authorized deletion receipts", receipts, err)
			}
			captureDiscardLiability(t, service, failed, 192)
			for _, path := range []string{
				failed.State.Boundary.Path,
				failed.Input.Snapshot.Dir,
			} {
				if _, err = os.Stat(path); err != nil {
					t.Fatal("incomplete discard result removed original material", path, err)
				}
			}
		})
	}
}
