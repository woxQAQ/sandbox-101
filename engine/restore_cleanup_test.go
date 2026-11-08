package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

// restoreScopeBackend uses the production sibling-scope check over an owned
// temporary inventory. Local instance effects stay in the lifecycle test backend.
// Stop proves only its exact recorded identity and does not inspect siblings.
type restoreScopeBackend struct {
	*testBackend

	cloud     *CloudHypervisor
	scopes    string
	known     map[string]bool
	uncertain string
	networks  int
	removes   int
}

func (b *restoreScopeBackend) Inspect(
	ctx context.Context,
	r Resource,
) (Observation, error) {
	entries, err := os.ReadDir(b.scopes)
	if err != nil {
		return Observation{}, err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), r.VMID+"-") ||
			entry.Name() == r.VMID+"-"+r.Instance {
			continue
		}
		data, err := os.ReadFile(
			filepath.Join(b.scopes, entry.Name(), "cgroup.procs"),
		)
		if err != nil {
			return Observation{}, err
		}
		if strings.TrimSpace(string(data)) != "" {
			// This path returns at the actual production sibling check, before
			// any host network or process inspection can occur.
			return b.cloud.Inspect(ctx, r)
		}
	}
	return b.testBackend.Inspect(ctx, r)
}

func (b *restoreScopeBackend) Stop(ctx context.Context, r Resource) error {
	if r.Instance == b.uncertain && r.Instance != "" ||
		r.Instance != "" && !b.known[r.Instance] {
		return api.Fail(api.ManualRecovery, "instance identity is uncertain")
	}
	if err := b.testBackend.Stop(ctx, r); err != nil {
		return err
	}
	path := filepath.Join(b.scopes, r.VMID+"-"+r.Instance, "cgroup.procs")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	return os.WriteFile(path, nil, 0o600)
}

func (b *restoreScopeBackend) Network(
	ctx context.Context,
	r Resource,
	enabled bool,
) error {
	b.networks++
	return b.testBackend.Network(ctx, r, enabled)
}

func (b *restoreScopeBackend) Remove(ctx context.Context, r Resource) error {
	b.removes++
	return b.testBackend.Remove(ctx, r)
}

func TestRestoreCandidateRetentionAndExplicitDeletion(t *testing.T) {
	t.Run("production-sibling-scopes", func(t *testing.T) {
		for _, scenario := range []string{"candidate-alive", "multiple-known-alive", "unknown-sibling", "uncertain-candidate"} {
			t.Run(scenario, func(t *testing.T) {
				dir, b, op, record, _ := restoreCandidateFixture(
					t,
					"launched",
					"restore",
				)
				b.mu.Lock()
				b.agentError = errors.New(
					"restore agent handshake failed after launch",
				)
				b.mu.Unlock()
				e := openTest(t, dir, b, false, nil)
				failed := awaitStatus(t, e, op.ID)
				if failed.Status != "failed" {
					t.Fatal("post-launch restore did not fail")
				}
				if err := e.Close(); err != nil {
					t.Fatal(err)
				}
				original := encode(failed)
				scopes := t.TempDir()
				backend := &restoreScopeBackend{
					testBackend: b,
					cloud: NewCloudHypervisor(
						CloudOptions{CgroupRoot: scopes},
					),
					scopes: scopes,
					known: map[string]bool{
						record.Journal.Old.Instance:      true,
						record.Journal.Resource.Instance: true,
					},
				}
				defer backend.cloud.Close()
				if scenario == "multiple-known-alive" {
					if err := b.Launch(
						context.Background(),
						*record.Journal.Old,
						false,
					); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "uncertain-candidate" {
					backend.uncertain = record.Journal.Resource.Instance
				}
				for instance, obs := range b.states {
					if !obs.Alive {
						continue
					}
					path := filepath.Join(scopes, op.VMID+"-"+instance)
					if err := os.Mkdir(path, 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(
						filepath.Join(path, "cgroup.procs"),
						[]byte("12345\n"),
						0o600,
					); err != nil {
						t.Fatal(err)
					}
				}
				unknown := filepath.Join(
					scopes,
					op.VMID+"-run-unknown",
					"cgroup.procs",
				)
				if scenario == "unknown-sibling" {
					if err := os.Mkdir(
						filepath.Dir(unknown),
						0o700,
					); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(
						unknown,
						[]byte("67890\n"),
						0o600,
					); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				e, err = Open(Options{
					StateDir:          dir,
					Backend:           backend,
					SafetyBytes:       1,
					MaxMemoryMiB:      8192,
					MaxVCPUs:          64,
					ReconcileInterval: time.Hour,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer e.Close()
				vm, err := e.GetVM(context.Background(), op.VMID)
				if err != nil {
					t.Fatal(err)
				}
				backend.networks, backend.removes = 0, 0 // Count only the explicit delete's effects.
				deletion, err := e.Submit(context.Background(), api.Request{
					Key:              "delete-scoped-restore",
					Action:           "delete",
					VMID:             op.VMID,
					ExpectedInstance: vm.Instance,
				})
				if err != nil {
					t.Fatal(err)
				}
				final := awaitStatus(t, e, deletion.ID)
				lock := e.vmLock(op.VMID)
				lock.Lock()
				defer lock.Unlock()
				uncertain := scenario == "unknown-sibling" ||
					scenario == "uncertain-candidate"
				if uncertain {
					if final.Status != "failed" || final.Error == nil ||
						final.Error.Code != api.ManualRecovery {
						t.Fatalf("uncertain deletion accepted: %+v", final)
					}
					if backend.networks != 0 || backend.removes != 0 {
						t.Fatal(
							"uncertainty allowed network/device mutation or removal",
						)
					}
					if _, err = os.Stat(
						record.Journal.Resource.Disk,
					); err != nil {
						t.Fatalf("candidate evidence removed: %v", err)
					}
					if _, err = e.GetVM(
						context.Background(),
						op.VMID,
					); err != nil {
						t.Fatal("uncertain VM ownership removed")
					}
					blocked, x := e.loadOp(context.Background(), op.ID)
					if x != nil || !blocked.Journal.RestoreBlocked ||
						blocked.Journal.RestoreCleaned {
						t.Fatal("uncertain restore fence cleared")
					}
					if scenario == "unknown-sibling" {
						data, x := os.ReadFile(unknown)
						if x != nil || string(data) != "67890\n" {
							t.Fatal("unknown scope was mutated")
						}
					}
				} else {
					if final.Status != "succeeded" {
						t.Fatalf(
							"known instances could not be deleted: %+v",
							final,
						)
					}
					if _, err = e.GetVM(
						context.Background(),
						op.VMID,
					); err == nil {
						t.Fatal("deleted VM retained")
					}
					if _, err = os.Stat(
						record.Journal.Resource.Disk,
					); !os.IsNotExist(
						err,
					) {
						t.Fatalf("deleted candidate retained: %v", err)
					}
				}
				for instance, obs := range b.states {
					if !uncertain && obs.Alive {
						t.Fatalf("known instance survived delete: %s", instance)
					}
				}
				after, err := e.GetOperation(context.Background(), op.ID)
				if err != nil || !bytes.Equal(original, encode(after)) {
					t.Fatal("original failed restore response changed")
				}
			})
		}
	})
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
