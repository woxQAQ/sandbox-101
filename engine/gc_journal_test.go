package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestGCDeletionJournalRetiresAfterExplicitVMDeletion(t *testing.T) {
	interrupt := false
	e, _ := openGCTest(t, func(step string) error {
		if interrupt && step == "gc_delete_intent" {
			return ErrInterrupted
		}
		return nil
	})
	vm, _, _ := gcStoppedRestore(t, e)
	interrupt = true
	if _, err := e.CollectGarbage(context.Background(), GCOptions{}); !errors.Is(err, ErrInterrupted) {
		t.Fatal("missing deletion intent interruption", err)
	}
	interrupt = false
	wait(t, e, api.Request{Key: "delete-after-gc-interruption", Action: "delete", VMID: vm.VMID})
	gcPass(t, e, GCOptions{})
	var n int
	if err := e.db.QueryRow("SELECT COUNT(*) FROM gc_deletions").Scan(&n); err != nil || n != 0 {
		t.Fatal("completed explicit deletion left a GC journal", n, err)
	}
}

func TestGCActualPartialImportAndCacheDeletionSurviveRestart(t *testing.T) {
	e, layout, _, image := importFixture(t)
	interrupt := func(step string) error {
		if step == "gc_unlinked" {
			return ErrInterrupted
		}
		return nil
	}
	// This fixture has no VM workers. The artifact guard also synchronizes with
	// any background collector while installing this test crash boundary.
	e.artifacts.Lock()
	e.opts.Fault = interrupt
	e.artifacts.Unlock()
	if _, err := e.CollectGarbage(context.Background(), GCOptions{MaxArtifacts: 1}); !errors.Is(err, ErrInterrupted) {
		t.Fatal("import cleanup did not interrupt after unlink", err)
	}
	work := filepath.Join(e.opts.StateDir, "imports", image.Digest[7:])
	// Intent removal after its files is another possible partial-unlink boundary.
	if err := os.Remove(filepath.Join(work, "intent.json")); err != nil {
		t.Fatal(err)
	}
	reopen := func(old *Engine, fault func(string) error) *Engine {
		t.Helper()
		if err := old.Close(); err != nil {
			t.Fatal(err)
		}
		next, err := Open(Options{StateDir: old.opts.StateDir, Backend: newTestBackend(), SafetyBytes: 1, MaxMemoryMiB: 8192, MaxVCPUs: 64, ReconcileInterval: time.Hour, GCInterval: time.Hour, Fault: fault})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { next.Close() })
		return next
	}
	e = reopen(e, nil)
	gcPass(t, e, GCOptions{})
	gcExists(t, work, false)
	orphan := putOCILayer(t, layout, []tarEntry{fileEntry("orphan-restart", "data")}, false)
	input, err := blob(layout, orphan.descriptor.Digest.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = buildLayerCache(context.Background(), e.opts.StateDir, input, orphan.descriptor.MediaType, image.Builder, orphan.diff.String(), 10240, 1); err != nil {
		t.Fatal(err)
	}
	e = reopen(e, interrupt)
	cache := filepath.Join(e.opts.StateDir, filepath.Dir(layerRelativePath(image.Builder, orphan.diff.String())))
	if _, err = e.CollectGarbage(context.Background(), GCOptions{}); !errors.Is(err, ErrInterrupted) {
		t.Fatal("cache cleanup did not interrupt after unlink", err)
	}
	gcExists(t, filepath.Join(cache, "layer.erofs"), false)
	gcExists(t, filepath.Join(cache, "layer.json"), true)
	e = reopen(e, nil)
	gcPass(t, e, GCOptions{})
	gcExists(t, cache, false)
	if err = image.Verify(filepath.Join(e.opts.StateDir, "images", image.Digest[7:]), image.Digest); err != nil {
		t.Fatal("deletion replay broke the published image", err)
	}
}
