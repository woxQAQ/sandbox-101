package workflows_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/sdk"
	"github.com/woxQAQ/haven/types"
)

func TestHavenReleaseFailedCapture(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, runtime, _, _ := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	runtime.Before = func(name string, _ loom.Resource) error {
		if name == "capture" {
			return fmt.Errorf("explicit capture failure")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	failed, err := service.CommandAdmission.Execute(ctx, principal, "checkpoint", operation.Intent{
		Action:    "checkpoint",
		SandboxID: claim.Result.SandboxID,
	})
	if err == nil || failed.Status != "failed" {
		t.Fatal(failed, err)
	}
	captureLink, _, err := machine.LoadResourceLink(ctx, service.State.Database(), failed.ID, "capture")
	if err != nil {
		t.Fatal(err)
	}
	original, err := service.OperationMonitor.GetForTenant(ctx, "tenant", captureLink.ChildID)
	if err != nil {
		t.Fatal(err)
	}
	released, err := service.CommandAdmission.Execute(ctx, principal, "release", operation.Intent{
		Action:    "release",
		SandboxID: claim.Result.SandboxID,
	})
	if err != nil {
		reason := service.Workers.State(released.ID).Err
		t.Fatal(released, reason, err)
	}
	var slots, storage, vms int
	if err = service.State.Database().QueryRow("SELECT (SELECT COALESCE(SUM(sandbox_slots),0) FROM quota_resources WHERE deleted_at IS NULL),(SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE deleted_at IS NULL),(SELECT COUNT(*) FROM vms WHERE deleted_at IS NULL)").Scan(&slots, &storage, &vms); err != nil || slots != 0 || storage != 0 || vms != 0 {
		t.Fatal(slots, storage, vms, err)
	}
	after, err := service.OperationMonitor.GetForTenant(ctx, "tenant", captureLink.ChildID)
	if err != nil || !reflect.DeepEqual(original, after) {
		t.Fatal("release changed original capture failure", original, after, err)
	}
}

func TestHavenReleasePausedAndFailedCandidates(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, boundary := range []string{
		"paused",
		"boot",
		"rollback",
	} {
		t.Run(boundary, func(t *testing.T) {
			var factory func(*testfixture.ColdRuntime) loom.Backend
			if boundary == "rollback" {
				factory = testfixture.GenerationBackend
			}
			service, runtime, _, _ := testapp.SandboxFixtureWithBackend(t, nil, factory)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if boundary == "boot" {
				runtime.Mu.Lock()
				runtime.FailBoot = true
				runtime.Mu.Unlock()
			}
			claim, err := service.Sandboxes.AdmitClaim(ctx, principal, "claim", operation.Intent{
				Action:     "claim",
				TemplateID: "cold",
				Policy: egress.Policy{
					Mode: "none",
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			claim, claimErr := service.OperationMonitor.Wait(ctx, principal, claim.ID)
			if boundary != "boot" && claimErr != nil {
				t.Fatal(claimErr)
			}
			if boundary == "paused" {
				if _, err = service.CommandAdmission.Execute(ctx, principal, "pause", operation.Intent{
					Action:    "pause",
					SandboxID: claim.Result.SandboxID,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "rollback" {
				checkpoint, err := service.CommandAdmission.Execute(ctx, principal, "checkpoint", operation.Intent{
					Action:    "checkpoint",
					SandboxID: claim.Result.SandboxID,
				})
				if err != nil {
					t.Fatal(err)
				}
				runtime.Before = func(name string, _ loom.Resource) error {
					if name == "boot" {
						return fmt.Errorf("explicit failed rollback boot")
					}
					return nil
				}
				if _, err = service.CommandAdmission.Execute(ctx, principal, "rollback", operation.Intent{
					Action:       "rollback",
					SandboxID:    claim.Result.SandboxID,
					CheckpointID: checkpoint.Result.CheckpointID,
				}); err == nil {
					t.Fatal("rollback unexpectedly succeeded")
				}
			}
			released, err := service.CommandAdmission.Execute(ctx, principal, "release", operation.Intent{
				Action:    "release",
				SandboxID: claim.Result.SandboxID,
			})
			if err != nil {
				reason := service.Workers.State(released.ID).Err
				t.Fatal(released, reason, err)
			}
			sandbox, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
			if err != nil || sandbox.State != "terminated" {
				t.Fatal(sandbox, err)
			}
			db := service.State.Database()
			usage, err := quota.TenantUsage(ctx, db, principal.TenantID)
			if err != nil || usage.Slots != 0 {
				t.Fatal("release retained sandbox quota", usage, err)
			}
			vms, err := machine.VMRoots(ctx, db, service.State.Directory())
			if err != nil || len(vms) != 0 {
				t.Fatal("release retained VMs", vms, err)
			}
			leases, err := machine.SourceLeases(ctx, db)
			if err != nil || len(leases) != 0 {
				t.Fatal("release retained snapshot leases", leases, err)
			}
		})
	}
}

func TestHavenRollbackKeepsLogicalSandbox(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, state := range []string{
		"running",
		"paused",
	} {
		t.Run(state, func(t *testing.T) {
			service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.GenerationBackend)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			claim := (testfixture.SandboxClaims{
				App: service,
			}).Claim(t, "claim", nil, egress.Policy{
				Mode: "none",
			})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			checkpoint, err := service.CommandAdmission.Execute(ctx, principal, "checkpoint", operation.Intent{
				Action:    "checkpoint",
				SandboxID: claim.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			if state == "paused" {
				if _, err = service.CommandAdmission.Execute(ctx, principal, "pause", operation.Intent{
					Action:    "pause",
					SandboxID: claim.Result.SandboxID,
				}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			rollback, err := service.CommandAdmission.Execute(ctx, principal, "rollback", operation.Intent{
				Action:       "rollback",
				SandboxID:    before.ID,
				CheckpointID: checkpoint.Result.CheckpointID,
			})
			if err != nil {
				reason := service.Workers.State(rollback.ID).Err
				t.Fatal(rollback, reason, err)
			}
			after, err := service.Catalog.Get(ctx, principal, before.ID)
			if err != nil || after.State != state || after.ID != before.ID {
				t.Fatal(after, err)
			}
			if state == "paused" {
				if after.SnapshotID != checkpoint.Result.SnapshotID || after.VMID != "" {
					t.Fatal(after)
				}
			} else {
				if after.VMID != before.VMID || after.Instance == before.Instance || after.Instance == "" {
					t.Fatal(before, after)
				}
			}
			var slots int
			if err = service.State.Database().QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources WHERE deleted_at IS NULL").Scan(&slots); err != nil || slots != 1 {
				t.Fatal(slots, err)
			}
		})
	}
}

func TestHavenForkUsesCheckpointOwner(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	checkpoint, err := service.CommandAdmission.Execute(ctx, principal, "checkpoint", operation.Intent{
		Action:    "checkpoint",
		SandboxID: claim.Result.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	fork, err := service.CommandAdmission.Execute(ctx, principal, "tenant-fork", operation.Intent{
		Action:       "fork",
		CheckpointID: checkpoint.Result.CheckpointID,
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err != nil {
		t.Fatal(fork, service.Workers.State(fork.ID).Err, err)
	}
	server := httptest.NewServer(api.New(api.Services{
		Authentication:   service.Authentication,
		CommandAdmission: service.CommandAdmission,
		OperationMonitor: service.OperationMonitor,
		OperationViews:   service.OperationViews,
	}))
	t.Cleanup(server.Close)
	admin := sdk.New(server.URL, service.Options.AdminToken)
	admin.Tenant = principal.TenantID
	result, err := admin.ForkCheckpoint(ctx, checkpoint.Result.CheckpointID, "admin-fork", types.Command{
		Policy: types.Policy{
			Mode: "none",
		},
	})
	if err != nil || len(result.Sandboxes) != 1 {
		t.Fatal("admin checkpoint fork", result, err)
	}
	for _, branch := range []string{
		fork.Result.SandboxID,
		result.Sandboxes[0].ID,
	} {
		sandbox, err := service.Dataplane.Ready(ctx, principal, branch)
		if err != nil || sandbox.State != "running" || sandbox.ID == claim.Result.SandboxID || sandbox.TenantID != "tenant" || sandbox.TemplateID != "cold" {
			t.Fatal(sandbox, err)
		}
	}
	if _, err = service.Forks.AdmitFork(ctx, authorization.Principal{
		TenantID: "other",
	}, "fork", operation.Intent{
		Action:       "fork",
		CheckpointID: checkpoint.Result.CheckpointID,
		Policy: egress.Policy{
			Mode: "none",
		},
	}); !errors.Is(err, errs.ErrNotFound) {
		t.Fatal("foreign tenant fork", err)
	}
	// Neither missing settings nor missing restore evidence may reveal a foreign source.
	db := service.State.Database()
	if _, err = db.Exec(`DELETE FROM sandbox_settings WHERE sandbox_id=?`, claim.Result.SandboxID); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE checkpoint_bindings SET snapshot_id='missing' WHERE id=?`, checkpoint.Result.CheckpointID); err != nil {
		t.Fatal(err)
	}
	for _, source := range []operation.Intent{
		{
			Action:       "fork",
			CheckpointID: checkpoint.Result.CheckpointID,
		},
		{
			Action:    "fork",
			SandboxID: claim.Result.SandboxID,
		},
	} {
		_, err = service.Forks.AdmitFork(ctx, authorization.Principal{
			Root:     true,
			TenantID: "other",
		}, "wrong-owner", source)
		if !errors.Is(err, errs.ErrNotFound) {
			t.Errorf("foreign source exposed its state: %v", err)
		}
	}
	var slots int
	if err = service.State.Database().QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources WHERE deleted_at IS NULL AND tenant_id='tenant'").Scan(&slots); err != nil || slots != 3 {
		t.Fatal(slots, err)
	}
}

func TestHavenResumeFailurePreservesSleep(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, boundary := range []string{
		"boot",
		"activate",
	} {
		t.Run(boundary, func(t *testing.T) {
			service, runtime, guest, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			claim := (testfixture.SandboxClaims{
				App: service,
			}).Claim(t, "claim", nil, egress.Policy{
				Mode: "none",
			})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := service.CommandAdmission.Execute(ctx, principal, "pause", operation.Intent{
				Action:    "pause",
				SandboxID: claim.Result.SandboxID,
			}); err != nil {
				t.Fatal(err)
			}
			before, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			if boundary == "boot" {
				runtime.Mu.Lock()
				runtime.FailBoot = true
				runtime.Mu.Unlock()
			} else {
				guest.Before = func(name string, _ loom.Resource) error {
					if name == "activate" {
						return fmt.Errorf("explicit activation failure")
					}
					return nil
				}
			}
			failed, err := service.CommandAdmission.Execute(ctx, principal, "resume", operation.Intent{
				Action:    "resume",
				SandboxID: before.ID,
			})
			if err == nil || failed.Status != "failed" {
				t.Fatal(failed, err)
			}
			for {
				step, err := sandbox.LoadLifecycleStep(ctx, service.State.Database(), failed.ID)
				if err != nil {
					t.Fatal(err)
				}
				if step.Status == "failed" {
					break
				}
				select {
				case <-ctx.Done():
					reason := service.Workers.State(failed.ID).Err
					t.Fatal("resume cleanup did not finish", reason)
				case <-time.After(time.Millisecond):
				}
			}
			after, err := service.Catalog.Get(ctx, principal, before.ID)
			if err != nil || after.State != "paused" || after.SnapshotID != before.SnapshotID || after.VMID != "" {
				t.Fatal(after, err)
			}
			var slots, storage, vms int
			if err = service.State.Database().QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources WHERE deleted_at IS NULL),(SELECT SUM(storage_mib) FROM quota_resources WHERE deleted_at IS NULL),(SELECT COUNT(*) FROM vms WHERE deleted_at IS NULL)").Scan(&slots, &storage, &vms); err != nil || slots != 1 || storage != 192 || vms != 0 {
				t.Fatal(slots, storage, vms, err)
			}
		})
	}
}

func TestHavenDataResumeConverges(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := service.CommandAdmission.Execute(ctx, principal, "pause", operation.Intent{
		Action:    "pause",
		SandboxID: claim.Result.SandboxID,
	}); err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 8)
	for range 8 {
		go func() {
			sandbox, err := service.Dataplane.Ready(ctx, authorization.Principal{
				TenantID:  "tenant",
				SandboxID: claim.Result.SandboxID,
			}, claim.Result.SandboxID)
			if err == nil && sandbox.State != "running" {
				err = fmt.Errorf("unexpected state %s", sandbox.State)
			}
			completed <- err
		}()
	}
	for range 8 {
		if err := <-completed; err != nil {
			t.Fatal(err)
		}
	}
	var attempts int
	if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM operation_steps WHERE step_key='lifecycle' AND action='resume'").Scan(&attempts); err != nil || attempts != 1 {
		t.Fatal("concurrent data created duplicate attempts", attempts, err)
	}
}

func TestHavenResumeRetainsSandboxSlot(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := service.CommandAdmission.Execute(ctx, principal, "pause", operation.Intent{
		Action:    "pause",
		SandboxID: claim.Result.SandboxID,
	}); err != nil {
		t.Fatal(err)
	}
	before, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := service.CommandAdmission.Execute(ctx, principal, "resume", operation.Intent{
		Action:    "resume",
		SandboxID: before.ID,
	})
	if err != nil {
		reason := service.Workers.State(resumed.ID).Err
		t.Fatal(resumed, reason, err)
	}
	after, err := service.Dataplane.Ready(ctx, principal, before.ID)
	if err != nil || after.State != "running" || after.VMID == claim.Result.VMID || after.Instance == "" || after.SnapshotID != "" {
		t.Fatal(after, err)
	}
	usage, err := quota.TenantUsage(ctx, service.State.Database(), principal.TenantID)
	if err != nil || usage.Slots != 1 {
		t.Fatal("resume lost sandbox quota", usage, err)
	}
	backings, err := machine.BackingRoots(ctx, service.State.Database())
	if err != nil || len(backings) != 1 {
		t.Fatal("resume did not retain one memory backing", backings, err)
	}
}

func TestHavenLifecycleAdmissionAndHandoff(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, action := range []string{
		"checkpoint",
		"pause",
		"release",
	} {
		t.Run(action, func(t *testing.T) {
			service, _, _, _ := testapp.SandboxFixture(t)
			claim := (testfixture.SandboxClaims{
				App: service,
			}).Claim(t, "claim", nil, egress.Policy{
				Mode: "none",
			})
			acceptedRecord, err := service.CommandAdmission.Submit(context.Background(), authorization.Principal{
				TenantID: "tenant",
			}, action, operation.Intent{
				Action:    action,
				SandboxID: claim.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			result, err := service.OperationMonitor.Wait(ctx, authorization.Principal{
				TenantID: "tenant",
			}, acceptedRecord.ID)
			if err != nil {
				reason := service.Workers.State(acceptedRecord.ID).Err
				t.Fatal("lifecycle failed", action, result, reason, err)
			}
			sandbox, err := service.Catalog.Get(ctx, authorization.Principal{
				TenantID: "tenant",
			}, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			var slots, storage int
			if err = service.State.Database().QueryRow("SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE deleted_at IS NULL AND tenant_id='tenant'").Scan(&slots, &storage); err != nil {
				t.Fatal(err)
			}
			switch action {
			case "checkpoint":
				if sandbox.State != "running" || slots != 1 || storage != 256 {
					t.Fatal(sandbox, slots, storage)
				}
			case "pause":
				if sandbox.State != "paused" || sandbox.VMID != "" || sandbox.SnapshotID == "" || slots != 1 || storage != 192 {
					t.Fatal(sandbox, slots, storage)
				}
			case "release":
				if sandbox.State != "terminated" || slots != 0 || storage != 0 {
					t.Fatal(sandbox, slots, storage)
				}
			}
		})
	}
}
