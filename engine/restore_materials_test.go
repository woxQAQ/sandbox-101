package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

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
