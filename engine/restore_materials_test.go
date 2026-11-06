package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

// The fixture uses lifecycle capture, then stops a clone at a durable boundary.
func restoreCandidateFixture(
	t *testing.T,
	boundary string,
	actions ...string,
) (string, *testBackend, api.Operation, opRecord, snapshotRecord) {
	t.Helper()
	dir := t.TempDir()
	b := newTestBackend()
	var armed atomic.Bool
	e := openTest(t, dir, b, true, func(step string) error {
		if armed.Load() && step == boundary {
			return ErrInterrupted
		}
		return nil
	})
	vm := wait(
		t,
		e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	wait(t, e, api.Request{Key: "start", Action: "start", VMID: vm.VMID})
	snap := wait(
		t,
		e,
		api.Request{Key: "snapshot", Action: "snapshot", VMID: vm.VMID},
	)
	request := api.Request{
		Key:        "clone",
		Action:     "clone",
		SnapshotID: snap.SnapshotID,
	}
	if len(actions) != 0 && actions[0] == "restore" {
		wait(
			t,
			e,
			api.Request{Key: "stop-source", Action: "stop", VMID: vm.VMID},
		)
		request.Action, request.VMID = "restore", vm.VMID
	}
	armed.Store(true)
	op, err := e.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	awaitStep(t, e, op.ID, boundary)
	// Read under the lifecycle lock after the faulted worker has returned.
	lock := e.vmLock(op.VMID)
	lock.Lock()
	record, err := e.loadOp(context.Background(), op.ID)
	if err != nil {
		t.Fatal(err)
	}
	source, err := e.loadSnapshot(context.Background(), snap.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	lock.Unlock()
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, b, op, record, source
}

func damageRestoreCandidate(t *testing.T, path, damage string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	switch damage {
	case "truncate":
		err = os.Truncate(path, 1)
	case "rewrite":
		err = os.WriteFile(path, bytes.Repeat([]byte{'x'}, len(data)), 0o600)
	case "remove":
		err = os.Remove(path)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestRestoreCandidateInterruptedCopy(t *testing.T) {
	for _, boundary := range []string{"restore_candidate_prepared", "restore_candidate_copied", "restore_candidate_renamed", "restore_candidate_synced"} {
		t.Run(boundary, func(t *testing.T) {
			dir, b, op, _, _ := restoreCandidateFixture(t, boundary)
			e := openTest(t, dir, b, false, nil)
			defer e.Close()
			if final := awaitStatus(t, e, op.ID); final.Status != "failed" {
				t.Fatalf("incomplete proof accepted: %+v", final)
			}
			record, err := e.loadOp(context.Background(), op.ID)
			if err != nil || !record.Journal.Lease ||
				!record.Journal.RestoreBlocked {
				t.Fatalf(
					"incomplete candidate lost protection: %+v %v",
					record.Journal,
					err,
				)
			}
		})
	}
	for _, name := range []string{"config.json", "state.json", "disk.raw"} {
		for _, damage := range []string{"truncate", "rewrite", "remove"} {
			t.Run(name+"/"+damage, func(t *testing.T) {
				dir, b, op, record, source := restoreCandidateFixture(
					t,
					"restore_prepare_intent",
				)
				r := *record.Journal.Resource
				if err := os.MkdirAll(r.RestoreDir, 0o700); err != nil {
					t.Fatal(err)
				}
				// Explicitly construct an interrupted destination, independent of reflink support.
				for _, file := range []string{"config.json", "state.json", "disk.raw"} {
					src, dst := filepath.Join(
						source.Dir,
						"ch",
						file,
					), filepath.Join(
						r.RestoreDir,
						file,
					)
					if file == "disk.raw" {
						src, dst = filepath.Join(source.Dir, file), r.Disk
					}
					if err := copyFile(src, dst, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				path := filepath.Join(r.RestoreDir, name)
				if name == "disk.raw" {
					path = r.Disk
				}
				damageRestoreCandidate(t, path, damage)
				e := openTest(t, dir, b, false, nil)
				defer e.Close()
				final := awaitStatus(t, e, op.ID)
				if final.Status == "succeeded" {
					for _, file := range []string{"config.json", "state.json", "disk.raw"} {
						src, dst := filepath.Join(
							source.Dir,
							"ch",
							file,
						), filepath.Join(
							r.RestoreDir,
							file,
						)
						if file == "disk.raw" {
							src, dst = filepath.Join(source.Dir, file), r.Disk
						}
						want, x := digestFile(src)
						got, y := digestFile(dst)
						if x != nil || y != nil || want != got {
							t.Fatalf(
								"damaged candidate handed off: %s (%v %v)",
								file,
								x,
								y,
							)
						}
					}
				}
			})
		}
	}
}

func TestRestoreCandidateDamageBeforeHandoff(t *testing.T) {
	for _, boundary := range []string{"restore_materials_complete", "restore_resources_ready"} {
		for _, name := range []string{"config.json", "state.json", "disk.raw"} {
			for _, damage := range []string{"truncate", "rewrite", "remove"} {
				t.Run(boundary+"/"+name+"/"+damage, func(t *testing.T) {
					dir, b, op, record, _ := restoreCandidateFixture(
						t,
						boundary,
					)
					r := *record.Journal.Resource
					path := filepath.Join(r.RestoreDir, name)
					if name == "disk.raw" {
						path = r.Disk
					}
					damageRestoreCandidate(t, path, damage)
					e := openTest(t, dir, b, false, nil)
					defer e.Close()
					if final := awaitStatus(
						t,
						e,
						op.ID,
					); final.Status != "failed" {
						t.Fatalf("proved candidate damage accepted: %+v", final)
					}
					after, err := e.loadOp(context.Background(), op.ID)
					if err != nil || !after.Journal.RestoreBlocked ||
						!bytes.Equal(
							encode(record.Journal.RestoreMaterials),
							encode(after.Journal.RestoreMaterials),
						) {
						t.Fatalf(
							"proof re-certified or fence missing: %+v %v",
							after.Journal,
							err,
						)
					}
				})
			}
		}
	}
}

func TestRestoreCandidateHandoffSurvivesSourceDeletion(t *testing.T) {
	dir, b, op, record, _ := restoreCandidateFixture(
		t,
		"restore_resources_ready",
	)
	e := openTest(t, dir, b, false, func(step string) error {
		if step == "restore_resources_ready" {
			return ErrInterrupted
		}
		return nil
	})
	wait(
		t,
		e,
		api.Request{
			Key:        "delete-source",
			Action:     "delete_snapshot",
			SnapshotID: op.SnapshotID,
		},
	)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = openTest(t, dir, b, false, nil)
	defer e.Close()
	if final := awaitStatus(t, e, op.ID); final.Status != "succeeded" {
		t.Fatalf("handoff depends on source: %+v", final)
	}
	after, err := e.loadOp(context.Background(), op.ID)
	if err != nil ||
		!bytes.Equal(
			encode(record.Journal.RestoreMaterials),
			encode(after.Journal.RestoreMaterials),
		) {
		t.Fatalf("handoff proof changed: %v", err)
	}
}

func TestRestoreCandidateRunningDiskIsNotRebuilt(t *testing.T) {
	for _, boundary := range []string{"restore_launch_intent", "launched", "booted", "agent_ready"} {
		t.Run(boundary, func(t *testing.T) {
			dir, b, op, record, _ := restoreCandidateFixture(t, boundary)
			r := *record.Journal.Resource
			if boundary == "restore_launch_intent" {
				// Simulate death after Launch returned but before its checkpoint.
				if err := b.Launch(context.Background(), r, true); err != nil {
					t.Fatal(err)
				}
			}
			written := []byte("guest-writes-after-delivery")
			if err := os.WriteFile(r.Disk, written, 0o600); err != nil {
				t.Fatal(err)
			}
			e := openTest(t, dir, b, false, nil)
			defer e.Close()
			if final := awaitStatus(t, e, op.ID); final.Status != "succeeded" {
				t.Fatalf("running disk rejected: %+v", final)
			}
			got, err := os.ReadFile(r.Disk)
			if err != nil || !bytes.Equal(got, written) {
				t.Fatalf("running disk rebuilt: %q %v", got, err)
			}
			b.mu.Lock()
			launches := b.launches
			b.mu.Unlock()
			if launches != 2 {
				t.Fatalf("duplicate launch: %d", launches)
			}
		})
	}
	for _, scenario := range []string{"unknown-before-proof", "unknown-after-proof", "live-without-launch", "legacy-without-proof", "memory-inode-replaced"} {
		t.Run(scenario, func(t *testing.T) {
			boundary := "restore_resources_ready"
			if scenario == "unknown-before-proof" {
				boundary = "restore_candidate_copied"
			}
			dir, b, op, record, _ := restoreCandidateFixture(t, boundary)
			r := *record.Journal.Resource
			if scenario == "legacy-without-proof" {
				e := openTest(
					t,
					dir,
					b,
					false,
					func(string) error { return ErrInterrupted },
				)
				lock := e.vmLock(op.VMID)
				lock.Lock()
				record.Journal.RestoreMaterials = nil
				if _, err := e.db.Exec(
					"UPDATE operations SET body=? WHERE id=?",
					encode(record),
					op.ID,
				); err != nil {
					t.Fatal(err)
				}
				lock.Unlock()
				if err := e.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "memory-inode-replaced" {
				path := filepath.Join(r.RestoreDir, "memory-ranges")
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, data, 0o444); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "live-without-launch" {
				if err := b.Launch(context.Background(), r, true); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "unknown-before-proof" ||
				scenario == "unknown-after-proof" {
				b.mu.Lock()
				b.inspectError = errors.New("unknown candidate identity")
				b.mu.Unlock()
			}
			e := openTest(t, dir, b, false, nil)
			defer e.Close()
			if final := awaitStatus(t, e, op.ID); final.Status != "failed" {
				t.Fatalf("unsafe candidate adopted: %+v", final)
			}
			if _, err := os.Stat(r.RestoreDir); err != nil {
				t.Fatalf("candidate evidence lost: %v", err)
			}
		})
	}
}

func TestRestoreCandidateRetentionAndExplicitDeletion(t *testing.T) {
	t.Run("existing-restore-target", func(t *testing.T) {
		dir, b, op, record, source := restoreCandidateFixture(
			t,
			"restore_materials_complete",
			"restore",
		)
		damageRestoreCandidate(t, record.Journal.Resource.Disk, "rewrite")
		e := openTest(t, dir, b, false, nil)
		defer e.Close()
		failed := awaitStatus(t, e, op.ID)
		if failed.Status != "failed" {
			t.Fatalf("damaged restore succeeded: %+v", failed)
		}
		original := encode(failed)
		if record.Journal.Old.Instance == "" ||
			record.Journal.Old.Instance == record.Journal.Resource.Instance {
			t.Fatal("fixture lacks distinct old and candidate identities")
		}
		for _, r := range []*Resource{record.Journal.Old, record.Journal.Resource} {
			if err := b.Launch(context.Background(), *r, false); err != nil {
				t.Fatal(err)
			}
		}
		vm, err := e.GetVM(context.Background(), op.VMID)
		if err != nil {
			t.Fatal(err)
		}
		wait(
			t,
			e,
			api.Request{
				Key:              "delete-restore-target",
				Action:           "delete",
				VMID:             op.VMID,
				ExpectedInstance: vm.Instance,
			},
		)
		for _, r := range []*Resource{record.Journal.Old, record.Journal.Resource} {
			obs, x := b.Inspect(context.Background(), *r)
			if x != nil || obs.Alive || obs.Network {
				t.Fatalf("known instance survived delete: %+v %v", obs, x)
			}
		}
		if _, err = os.Stat(filepath.Join(source.Dir, "disk.raw")); err != nil {
			t.Fatalf("target deletion damaged independent snapshot: %v", err)
		}
		after, err := e.GetOperation(context.Background(), op.ID)
		if err != nil || !bytes.Equal(original, encode(after)) {
			t.Fatalf("original restore response changed: %v", err)
		}
		wait(
			t,
			e,
			api.Request{
				Key:        "delete-independent-snapshot",
				Action:     "delete_snapshot",
				SnapshotID: op.SnapshotID,
			},
		)
	})
	for _, boundary := range []string{"restore_candidate_synced", "restore_materials_complete", "restore_resources_ready"} {
		t.Run(boundary, func(t *testing.T) {
			dir, b, op, record, _ := restoreCandidateFixture(t, boundary)
			damageRestoreCandidate(t, record.Journal.Resource.Disk, "rewrite")
			e := openTest(t, dir, b, false, nil)
			failed := awaitStatus(t, e, op.ID)
			if failed.Status != "failed" {
				t.Fatalf("damaged candidate accepted: %+v", failed)
			}
			original := encode(failed)
			v, err := e.GetVM(context.Background(), op.VMID)
			if err != nil || v.OperationID != op.ID {
				t.Fatalf("failed restore fence lost: %+v %v", v, err)
			}
			if err = e.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, action := range []string{"start", "restore"} {
				_, err = e.Submit(
					context.Background(),
					api.Request{
						Key:        "reject-" + action,
						Action:     action,
						VMID:       op.VMID,
						SnapshotID: op.SnapshotID,
					},
				)
				if err == nil {
					t.Fatalf("%s bypassed restore fence", action)
				}
			}
			var leases int
			if err = e.db.QueryRow("SELECT COUNT(*) FROM leases WHERE operation=?", op.ID).
				Scan(&leases); err != nil {
				t.Fatal(err)
			}
			if (boundary != "restore_resources_ready") != (leases == 1) {
				t.Fatalf("unexpected retained lease count: %d", leases)
			}
			if leases == 1 {
				del, x := e.Submit(
					context.Background(),
					api.Request{
						Key:        "source-blocked",
						Action:     "delete_snapshot",
						SnapshotID: op.SnapshotID,
					},
				)
				if x != nil || awaitStatus(t, e, del.ID).Status != "failed" {
					t.Fatalf("source lease bypassed: %v", x)
				}
			}
			_, _ = e.CollectGarbage(context.Background(), GCOptions{})
			if _, err = os.Stat(record.Journal.Resource.Disk); err != nil {
				t.Fatalf("GC removed blocked candidate: %v", err)
			}
			if _, err = e.Submit(
				context.Background(),
				api.Request{
					Key:              "stale-delete",
					Action:           "delete",
					VMID:             op.VMID,
					ExpectedInstance: "stale",
				},
			); err == nil {
				t.Fatal("stale delete admitted")
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			// A blanket recovery acknowledgement must not authorize this VM again.
			if boundary == "restore_materials_complete" {
				if err = writeJSON(
					filepath.Join(dir, "RECOVERY_REQUIRED"),
					RecoveryPoint{ID: "restore-fence-review"},
				); err != nil {
					t.Fatal(err)
				}
				if err = AcknowledgeRecovery(
					dir,
					"review retained restore materials",
				); err != nil {
					t.Fatal(err)
				}
			}
			// An uncertain instance must also block explicit deletion without releasing the original lease.
			b.mu.Lock()
			b.inspectError = errors.New("unknown ownership")
			b.mu.Unlock()
			e = openTest(t, dir, b, false, nil)
			fenced, x := e.GetVM(context.Background(), op.VMID)
			if x != nil || fenced.OperationID != op.ID {
				t.Fatalf(
					"recovery acknowledgement cleared restore fence: %+v %v",
					fenced,
					x,
				)
			}
			if _, x = e.Submit(
				context.Background(),
				api.Request{
					Key:    "start-after-review",
					Action: "start",
					VMID:   op.VMID,
				},
			); x == nil {
				t.Fatal("review bypassed restore fence")
			}
			request := api.Request{
				Key:              "delete-unknown",
				Action:           "delete",
				VMID:             op.VMID,
				ExpectedInstance: v.Instance,
			}
			deletion, err := e.Submit(context.Background(), request)
			if err != nil || awaitStatus(t, e, deletion.ID).Status != "failed" {
				t.Fatalf("unknown delete accepted: %v", err)
			}
			if _, err = os.Stat(record.Journal.Resource.Disk); err != nil {
				t.Fatal("unknown delete removed evidence")
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			b.mu.Lock()
			b.inspectError = nil
			// Both the old and candidate identities must be stopped, including devices.
			b.states[record.Journal.Old.Instance] = Observation{
				Alive:   true,
				Network: true,
			}
			b.states[record.Journal.Resource.Instance] = Observation{
				Alive:   true,
				Network: true,
			}
			b.mu.Unlock()
			e = openTest(t, dir, b, false, nil)
			unknown := filepath.Join(
				filepath.Dir(record.Journal.Resource.Disk),
				"unknown-evidence",
			)
			if err = os.WriteFile(
				unknown,
				[]byte("retain"),
				0o600,
			); err != nil {
				t.Fatal(err)
			}
			request.Key = "delete-unknown-entry"
			deletion, err = e.Submit(context.Background(), request)
			if err != nil || awaitStatus(t, e, deletion.ID).Status != "failed" {
				t.Fatalf("unknown artifact removed: %v", err)
			}
			if _, err = os.Stat(unknown); err != nil {
				t.Fatalf("unknown evidence lost: %v", err)
			}
			if err = os.Remove(unknown); err != nil {
				t.Fatal(err)
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			// A failed partial deletion resumes the receipt of the original blocked restore.
			e = openTest(t, dir, b, false, func(step string) error {
				if step == "gc_unlinked" {
					return errors.New("delete unlink response lost")
				}
				return nil
			})
			request.Key = "delete-partial"
			deletion, err = e.Submit(context.Background(), request)
			if err != nil || awaitStatus(t, e, deletion.ID).Status != "failed" {
				t.Fatalf("partial delete failed to stop: %v", err)
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			e = openTest(t, dir, b, false, func(step string) error {
				if step == "restore_delete_removed" {
					return ErrInterrupted
				}
				return nil
			})
			request.Key = "delete-final"
			deletion, err = e.Submit(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			awaitStep(t, e, deletion.ID, "restore_delete_removed")
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			e = openTest(t, dir, b, false, func(step string) error {
				if step == "restore_delete_committed" {
					return ErrInterrupted
				}
				return nil
			})
			awaitStep(t, e, deletion.ID, "restore_delete_committed")
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			e = openTest(t, dir, b, false, nil)
			defer e.Close()
			if final := awaitStatus(
				t,
				e,
				deletion.ID,
			); final.Status != "succeeded" {
				t.Fatalf("delete replay failed: %+v", final)
			}
			b.mu.Lock()
			oldState, candidateState := b.states[record.Journal.Old.Instance], b.states[record.Journal.Resource.Instance]
			b.mu.Unlock()
			if oldState.Alive || oldState.Network || candidateState.Alive ||
				candidateState.Network {
				t.Fatal("delete left a known instance/device active")
			}
			repeated, err := e.Submit(context.Background(), request)
			if err != nil || repeated.ID != deletion.ID ||
				repeated.Status != "succeeded" {
				t.Fatalf("delete lost-response retry: %+v %v", repeated, err)
			}
			after, err := e.GetOperation(context.Background(), op.ID)
			if err != nil || !bytes.Equal(original, encode(after)) {
				t.Fatalf(
					"original failure response mutated: %s %s %v",
					original,
					encode(after),
					err,
				)
			}
			cleaned, err := e.loadOp(context.Background(), op.ID)
			if err != nil || !cleaned.Journal.RestoreCleaned ||
				cleaned.Journal.Lease {
				t.Fatalf("cleanup marker missing: %+v %v", cleaned.Journal, err)
			}
			if _, err = e.GetVM(context.Background(), op.VMID); err == nil {
				t.Fatal("deleted VM retained")
			}
			for _, query := range []string{"SELECT COUNT(*) FROM leases WHERE operation=?", "SELECT COUNT(*) FROM backing WHERE vm=?", "SELECT COUNT(*) FROM reservations WHERE owner=? OR owner LIKE ?"} {
				args := []any{op.VMID}
				if query == "SELECT COUNT(*) FROM leases WHERE operation=?" {
					args = []any{op.ID}
				}
				if query == "SELECT COUNT(*) FROM reservations WHERE owner=? OR owner LIKE ?" {
					args = []any{op.VMID, op.VMID + ":%"}
				}
				var count int
				if err = e.db.QueryRow(query, args...).
					Scan(&count); err != nil ||
					count != 0 {
					t.Fatalf(
						"cleanup ledger retained: %s count=%d %v",
						query,
						count,
						err,
					)
				}
			}
		})
	}
}
