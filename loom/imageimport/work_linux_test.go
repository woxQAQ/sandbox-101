//go:build linux

package imageimport_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
)

// This filesystem scenario exercises prepared work custody, independently of
// native image construction or the control plane's publication authorization.
func TestImportWorkDeletionKeepsOutputCustodySeparate(t *testing.T) {
	for _, layout := range []string{
		host.LayeredEROFSLayout,
		host.MergedOCIEROFSLayout,
	} {
		t.Run(layout, func(t *testing.T) {
			state := t.TempDir()
			operation := "op-" + strings.Repeat("1", 32)
			boundary, err := imageimport.PrepareImageImport(state, operation)
			if err != nil {
				t.Fatal(err)
			}
			work := imageimport.ImportWork{
				StateRoot:   state,
				OperationID: operation,
				Boundary:    boundary,
				Plan: imageimport.ImageImportPlan{
					Options: imageimport.ImageImportOptions{
						Digest:          "sha256:" + strings.Repeat("2", 64),
						Materialization: layout,
					},
					ConfigDigest: "sha256:" + strings.Repeat("3", 64),
				},
			}
			output := filepath.Join(boundary.Path, "output")
			if err = os.WriteFile(filepath.Join(output, "kernel"), []byte("private prepared output"), 0o400); err != nil {
				t.Fatal(err)
			}
			receipt, err := work.InspectUnpublishedWorkDeletion(context.Background())
			if err != nil || receipt.Nodes["output"] != boundary.Output || receipt.Nodes["output/kernel"].Inode == 0 {
				t.Fatal("unpublished work lost its original output boundary", receipt, err)
			}
			if _, err = work.InspectPublishedWorkDeletion(context.Background()); err == nil {
				t.Fatal("published cleanup accepted an output still held by the work directory")
			}
			held := filepath.Join(state, "held-output")
			if err = os.Rename(output, held); err != nil {
				t.Fatal(err)
			}
			if err = os.Mkdir(output, 0o700); err != nil {
				t.Fatal(err)
			}
			if _, err = work.InspectUnpublishedWorkDeletion(context.Background()); err == nil {
				t.Fatal("unpublished cleanup adopted a replacement output inode")
			}
			if err = os.Remove(output); err != nil {
				t.Fatal(err)
			}
			receipt, err = work.InspectPublishedWorkDeletion(context.Background())
			if err != nil || receipt.Nodes["."] != boundary.Work {
				t.Fatal("published cleanup lost the original input work boundary", receipt, err)
			}
			if _, found := receipt.Nodes["output"]; found {
				t.Fatal("published work receipt retained output custody")
			}
			unknown := filepath.Join(boundary.Path, "input", "unknown")
			if err = os.WriteFile(unknown, []byte("retained"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err = work.InspectPublishedWorkDeletion(context.Background()); err == nil {
				t.Fatal("published cleanup adopted an unknown input artifact")
			}
			if data, err := os.ReadFile(filepath.Join(held, "kernel")); err != nil || string(data) != "private prepared output" {
				t.Fatal("work inspection touched independently held output", err)
			}
		})
	}
}
