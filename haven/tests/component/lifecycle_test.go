package component

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
)

func TestHavenReleaseFailedCapture(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, _, _ := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (sandboxClaims{
		App: service,
	}).claim(t, "claim", nil, egress.Policy{
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
	failed, err := service.SandboxRequests.Execute(ctx, principal, "checkpoint", operation.Command{
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
	original, err := service.Operations.GetOperation(ctx, "tenant", captureLink.ChildID)
	if err != nil {
		t.Fatal(err)
	}
	released, err := service.SandboxRequests.Execute(ctx, principal, "release", operation.Command{
		Action:    "release",
		SandboxID: claim.Result.SandboxID,
	})
	if err != nil {
		reason := service.Workers.Snapshot(released.ID).Err
		t.Fatal(released, reason, err)
	}
	var slots, storage, vms int
	if err = service.State.Database().QueryRow("SELECT (SELECT COALESCE(SUM(sandbox_slots),0) FROM quota_resources),(SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources),(SELECT COUNT(*) FROM vms)").Scan(&slots, &storage, &vms); err != nil || slots != 0 || storage != 0 || vms != 0 {
		t.Fatal(slots, storage, vms, err)
	}
	after, err := service.Operations.GetOperation(ctx, "tenant", captureLink.ChildID)
	if err != nil || !reflect.DeepEqual(original, after) {
		t.Fatal("release changed original capture failure", original, after, err)
	}
}

func TestHavenReleasePausedAndFailedCandidates(t *testing.T) {
	parallelLifecycle(t)
	for _, boundary := range []string{
		"paused",
		"boot",
		"rollback",
	} {
		t.Run(boundary, func(t *testing.T) {
			var factory func(*testfixture.ColdRuntime) loom.Backend
			if boundary == "rollback" {
				factory = generationBackend
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
			claim, err := service.Sandboxes.AdmitClaim(ctx, principal, "claim", operation.Command{
				Action:     "claim",
				TemplateID: "cold",
				Policy: egress.Policy{
					Mode: "none",
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			claim, claimErr := service.Operations.Wait(ctx, principal, claim.ID)
			if boundary != "boot" && claimErr != nil {
				t.Fatal(claimErr)
			}
			if boundary == "paused" {
				if _, err = service.SandboxRequests.Execute(ctx, principal, "pause", operation.Command{
					Action:    "pause",
					SandboxID: claim.Result.SandboxID,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "rollback" {
				checkpoint, err := service.SandboxRequests.Execute(ctx, principal, "checkpoint", operation.Command{
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
				if _, err = service.SandboxRequests.Execute(ctx, principal, "rollback", operation.Command{
					Action:       "rollback",
					SandboxID:    claim.Result.SandboxID,
					CheckpointID: checkpoint.Result.CheckpointID,
				}); err == nil {
					t.Fatal("rollback unexpectedly succeeded")
				}
			}
			released, err := service.SandboxRequests.Execute(ctx, principal, "release", operation.Command{
				Action:    "release",
				SandboxID: claim.Result.SandboxID,
			})
			if err != nil {
				reason := service.Workers.Snapshot(released.ID).Err
				t.Fatal(released, reason, err)
			}
			sandbox, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
			if err != nil || sandbox.State != "terminated" {
				t.Fatal(sandbox, err)
			}
			var slots, vms, leases int
			if err = service.State.Database().QueryRow("SELECT (SELECT COALESCE(SUM(sandbox_slots),0) FROM quota_resources),(SELECT COUNT(*) FROM vms),(SELECT COUNT(*) FROM source_leases)").Scan(&slots, &vms, &leases); err != nil || slots != 0 || vms != 0 || leases != 0 {
				t.Fatal(slots, vms, leases, err)
			}
		})
	}
}

func TestHavenRollbackKeepsLogicalSandbox(t *testing.T) {
	parallelLifecycle(t)
	for _, state := range []string{
		"running",
		"paused",
	} {
		t.Run(state, func(t *testing.T) {
			service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, generationBackend)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			claim := (sandboxClaims{
				App: service,
			}).claim(t, "claim", nil, egress.Policy{
				Mode: "none",
			})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			checkpoint, err := service.SandboxRequests.Execute(ctx, principal, "checkpoint", operation.Command{
				Action:    "checkpoint",
				SandboxID: claim.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			if state == "paused" {
				if _, err = service.SandboxRequests.Execute(ctx, principal, "pause", operation.Command{
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
			rollback, err := service.SandboxRequests.Execute(ctx, principal, "rollback", operation.Command{
				Action:       "rollback",
				SandboxID:    before.ID,
				CheckpointID: checkpoint.Result.CheckpointID,
			})
			if err != nil {
				reason := service.Workers.Snapshot(rollback.ID).Err
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
			if err = service.State.Database().QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources").Scan(&slots); err != nil || slots != 1 {
				t.Fatal(slots, err)
			}
		})
	}
}

func TestHavenForkUsesCheckpointOwner(t *testing.T) {
	parallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, restorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (sandboxClaims{
		App: service,
	}).claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	checkpoint, err := service.SandboxRequests.Execute(ctx, principal, "checkpoint", operation.Command{
		Action:    "checkpoint",
		SandboxID: claim.Result.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, actor := range []authorization.Principal{
		principal,
		{
			Root:     true,
			TenantID: "root",
		},
	} {
		fork, err := service.SandboxRequests.Execute(ctx, actor, "fork", operation.Command{
			Action:       "fork",
			CheckpointID: checkpoint.Result.CheckpointID,
			Policy: egress.Policy{
				Mode: "none",
			},
		})
		if err != nil {
			reason := service.Workers.Snapshot(fork.ID).Err
			t.Fatal(fork, reason, err)
		}
		sandbox, err := service.Dataplane.Ready(ctx, principal, fork.Result.SandboxID)
		if err != nil || sandbox.State != "running" || sandbox.ID == claim.Result.SandboxID || sandbox.TenantID != "tenant" || sandbox.TemplateID != "cold" {
			t.Fatal(sandbox, err)
		}
	}
	if _, err = service.Forks.AdmitFork(ctx, authorization.Principal{
		TenantID: "other",
	}, "fork", operation.Command{
		Action:       "fork",
		CheckpointID: checkpoint.Result.CheckpointID,
		Policy: egress.Policy{
			Mode: "none",
		},
	}); err == nil {
		t.Fatal("foreign tenant fork accepted")
	}
	var slots int
	if err = service.State.Database().QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant'").Scan(&slots); err != nil || slots != 3 {
		t.Fatal(slots, err)
	}
}

func TestHavenResumeFailurePreservesSleep(t *testing.T) {
	parallelLifecycle(t)
	for _, boundary := range []string{
		"boot",
		"activate",
	} {
		t.Run(boundary, func(t *testing.T) {
			service, runtime, guest, _ := testapp.SandboxFixtureWithBackend(t, nil, restorableBackend)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			claim := (sandboxClaims{
				App: service,
			}).claim(t, "claim", nil, egress.Policy{
				Mode: "none",
			})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := service.SandboxRequests.Execute(ctx, principal, "pause", operation.Command{
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
			failed, err := service.SandboxRequests.Execute(ctx, principal, "resume", operation.Command{
				Action:    "resume",
				SandboxID: before.ID,
			})
			if err == nil || failed.Status != "failed" {
				t.Fatal(failed, err)
			}
			for {
				step, err := sandbox.LoadLifecycle(ctx, service.State.Database(), failed.ID)
				if err != nil {
					t.Fatal(err)
				}
				if step.Status == "failed" {
					break
				}
				select {
				case <-ctx.Done():
					reason := service.Workers.Snapshot(failed.ID).Err
					t.Fatal("resume cleanup did not finish", reason)
				case <-time.After(time.Millisecond):
				}
			}
			after, err := service.Catalog.Get(ctx, principal, before.ID)
			if err != nil || after.State != "paused" || after.SnapshotID != before.SnapshotID || after.VMID != "" {
				t.Fatal(after, err)
			}
			var slots, storage, vms int
			if err = service.State.Database().QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources),(SELECT COUNT(*) FROM vms)").Scan(&slots, &storage, &vms); err != nil || slots != 1 || storage != 192 || vms != 0 {
				t.Fatal(slots, storage, vms, err)
			}
		})
	}
}

func TestHavenDataResumeConverges(t *testing.T) {
	parallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, restorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (sandboxClaims{
		App: service,
	}).claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := service.SandboxRequests.Execute(ctx, principal, "pause", operation.Command{
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
	parallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, restorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (sandboxClaims{
		App: service,
	}).claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := service.SandboxRequests.Execute(ctx, principal, "pause", operation.Command{
		Action:    "pause",
		SandboxID: claim.Result.SandboxID,
	}); err != nil {
		t.Fatal(err)
	}
	before, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := service.SandboxRequests.Execute(ctx, principal, "resume", operation.Command{
		Action:    "resume",
		SandboxID: before.ID,
	})
	if err != nil {
		reason := service.Workers.Snapshot(resumed.ID).Err
		t.Fatal(resumed, reason, err)
	}
	after, err := service.Dataplane.Ready(ctx, principal, before.ID)
	if err != nil || after.State != "running" || after.VMID == claim.Result.VMID || after.Instance == "" || after.SnapshotID != "" {
		t.Fatal(after, err)
	}
	var slots, backings int
	if err = service.State.Database().QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant'),(SELECT COUNT(*) FROM backing)").Scan(&slots, &backings); err != nil || slots != 1 || backings != 1 {
		t.Fatal("resume lost/duplicated responsibilities", slots, backings, err)
	}
}

func TestHavenLifecycleAdmissionAndHandoff(t *testing.T) {
	parallelLifecycle(t)
	for _, action := range []string{
		"checkpoint",
		"pause",
		"release",
	} {
		t.Run(action, func(t *testing.T) {
			service, _, _, _ := testapp.SandboxFixture(t)
			claim := (sandboxClaims{
				App: service,
			}).claim(t, "claim", nil, egress.Policy{
				Mode: "none",
			})
			acceptedRecord, err := service.SandboxRequests.Submit(context.Background(), authorization.Principal{
				TenantID: "tenant",
			}, action, operation.Command{
				Action:    action,
				SandboxID: claim.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			result, err := service.Operations.Wait(ctx, authorization.Principal{
				TenantID: "tenant",
			}, acceptedRecord.ID)
			if err != nil {
				reason := service.Workers.Snapshot(acceptedRecord.ID).Err
				t.Fatal("lifecycle failed", action, result, reason, err)
			}
			sandbox, err := service.Catalog.Get(ctx, authorization.Principal{
				TenantID: "tenant",
			}, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			var slots, storage int
			if err = service.State.Database().QueryRow("SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE tenant_id='tenant'").Scan(&slots, &storage); err != nil {
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
