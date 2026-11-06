package sandboxd

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestSnapshotOwnershipBackupRoundTrip(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	source := claimTest(t, s, p, "source", nil)
	cp, err := s.Execute(
		s.ctx,
		p,
		"checkpoint",
		Command{Action: "checkpoint", SandboxID: source.Sandbox.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	shape := source.Sandbox.Shape
	f.mu.Lock()
	f.snaps["external"] = api.Snapshot{
		ID:        "external",
		Config:    api.Config{Shape: shape},
		Published: true,
	}
	f.mu.Unlock()
	if _, err = s.Execute(
		s.ctx,
		Principal{Root: true, TenantID: p.TenantID},
		"import",
		Command{
			Action: "template_put",
			Template: &Template{
				ID:         "golden",
				Shape:      shape,
				SnapshotID: "external",
			},
		},
	); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "backup")
	if _, err = s.Backup(s.ctx, destination); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if _, err = RestoreStateBackup(restored, destination); err != nil {
		t.Fatal(err)
	}
	if err = AcknowledgeStateRecovery(
		restored,
		"Reviewed test engine inventory and ownership receipts",
	); err != nil {
		t.Fatal(err)
	}
	opts := s.opts
	opts.StateDir = restored
	opts.Initialize = false
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if _, err = reopened.Execute(
		reopened.ctx,
		p,
		"own-import",
		Command{
			Action: "template_put",
			Template: &Template{
				ID:         "own",
				Shape:      shape,
				SnapshotID: cp.Checkpoint.SnapshotID,
			},
		},
	); err != nil {
		t.Fatal("captured owner lost", err)
	}
	if _, err = reopened.Execute(
		reopened.ctx,
		p,
		"claim-golden",
		Command{Action: "claim", TemplateID: "golden"},
	); err != nil {
		t.Fatal("external grant lost", err)
	}
	if _, err = reopened.Execute(
		reopened.ctx,
		Principal{TenantID: "other"},
		"foreign",
		Command{
			Action: "template_put",
			Template: &Template{
				ID:         "foreign",
				Shape:      shape,
				SnapshotID: cp.Checkpoint.SnapshotID,
			},
		},
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatal("foreign capture accepted", err)
	}
}

func TestLegacySnapshotOwnershipStateRequiresReview(t *testing.T) {
	dir := t.TempDir()
	st, err := openStore(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.db.Exec(
		"UPDATE version SET version=1; DROP TABLE snapshot_owners",
	); err != nil {
		t.Fatal(err)
	}
	if err = st.close(); err != nil {
		t.Fatal(err)
	}
	f := newFakeEngine()
	if _, err = Open(
		Options{
			StateDir:  dir,
			Engine:    f,
			Agent:     &fakeAgent{},
			RootToken: "root-secret",
		},
	); !isCode(
		err,
		api.Incompatible,
	) {
		t.Fatal("legacy state accepted", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "sandboxd.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version, owners int
	if err = db.QueryRow("SELECT version FROM version").
		Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='snapshot_owners'").
		Scan(&owners); err != nil {
		t.Fatal(err)
	}
	if version != 1 || owners != 0 || f.count("delete") != 0 ||
		f.count("delete_snapshot") != 0 {
		t.Fatal("legacy state mutated", version, owners)
	}
}

func TestSandboxdBackupRestorationRequiresReview(t *testing.T) {
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
