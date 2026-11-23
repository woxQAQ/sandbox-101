package testfixture

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// SeedPendingImageImport freezes a private import whose source vanished before staging.
func SeedPendingImageImport(t *testing.T, state *store.State) (string, imageimport.ImageImportOptions) {
	t.Helper()
	ctx := context.Background()
	options := imageimport.ImageImportOptions{
		Layout:          t.TempDir(),
		Digest:          "sha256:" + strings.Repeat("b", 64),
		Materialization: host.LayeredEROFSLayout,
		MaxBytes:        4 << 20,
		MaxFiles:        2048,
	}
	tools := map[string]string{
		"mkfs.erofs": "mkfs.erofs 1.8.10",
		"fsck.erofs": "fsck.erofs 1.8.10",
	}
	plan := imageimport.ImageImportPlan{
		Options:      options,
		ConfigDigest: "sha256:" + strings.Repeat("c", 64),
		ConfigSize:   128,
		Tools:        tools,
		Builder:      host.ImageBuilder(tools),
		Layers: []imageimport.ImageImportLayer{
			{
				Digest:    "sha256:" + strings.Repeat("d", 64),
				DiffID:    "sha256:" + strings.Repeat("e", 64),
				MediaType: "application/vnd.oci.image.layer.v1.tar",
				Size:      1024,
			},
		},
	}
	id := operation.NewID("op-")
	request := operation.Request{
		Action: "image_import",
		Import: &options,
	}
	// Intent retirement compares the original bytes, including field order.
	input := fmt.Appendf(nil, `{"request_tenant_id":"root","options":%s,"plan":%s,"work":%s,"target":%s,"reservation":%d}`, operation.Encode(options), operation.Encode(plan), operation.Encode(filepath.Join(state.Directory(), "imports", id)), operation.Encode(filepath.Join(state.Directory(), "images", options.Digest[7:])), int64(64<<20))
	tx, err := state.Database().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	operations := operation.New(tx)
	if err = operations.RecordAdmission(ctx, operation.Admission{
		ID:        id,
		ScopeKind: "node",
		ScopeID:   state.Identity().Identity,
		Key:       "original",
		Request:   request,
		Status:    "pending",
	}); err != nil {
		t.Fatal(err)
	}
	if err = operation.InsertPhaseStep(ctx, tx, operation.PhaseIntent{
		OperationID:         id,
		StepKey:             "image_import",
		Action:              "image_import",
		Input:               input,
		CandidateResourceID: options.Digest,
		Status:              "pending",
		Phase:               "admitted",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO image_import_progress(operation_id) VALUES(?)", id); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO image_intents(digest,operation_id,layout,input_hash) VALUES(?,?,?,?)`, options.Digest, id, options.Materialization, operation.Digest(input)); err != nil {
		t.Fatal(err)
	}
	if err = quota.Reserve(ctx, tx, quota.Reservation{
		Owner: "import:" + id,
		Bytes: 64 << 20,
	}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = state.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	return id, options
}
