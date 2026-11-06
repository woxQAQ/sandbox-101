package sandboxd

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestProductBackupRestorationRequiresReview(t *testing.T) {
	s, _, _, p := fixture(t)
	r := claimTest(
		t,
		s,
		p,
		"claim",
		map[string]EnvVar{"VISIBLE": {Value: "preserved"}},
	)
	destination := filepath.Join(t.TempDir(), "backup")
	point, err := s.Backup(context.Background(), destination)
	if err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	got, err := RestoreStateBackup(restored, destination)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != point.ID {
		t.Fatal("backup point changed")
	}
	if _, err = openStore(restored, false); !isCode(err, api.ManualRecovery) {
		t.Fatal("restored metadata silently started", err)
	}
	if err = AcknowledgeStateRecovery(
		restored,
		"Reviewed engine inventory and operation history at this backup point",
	); err != nil {
		t.Fatal(err)
	}
	st, err := openStore(restored, false)
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	b, err := getSandbox(context.Background(), st.db, r.Sandbox.ID)
	if err != nil || b.Env["VISIBLE"] != "preserved" {
		t.Fatalf("consistent backup lost state: %+v %v", b, err)
	}
	if st.token(b) != s.store.token(*r.Sandbox) {
		t.Fatal("restored node key changed sandbox token")
	}
	if _, err = RestoreStateBackup(restored, destination); err == nil {
		t.Fatal("restore overwrote an existing metadata directory")
	}
}
