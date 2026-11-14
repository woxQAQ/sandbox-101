package haven

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
	"github.com/woxQAQ/haven/loom"
)

func TestHavenReleaseFailedCapture(t *testing.T) {
	service, runtime, _, _ := sandboxFixture(t)
	principal := Principal{
		TenantID: "tenant",
	}
	claim := claimSandbox(t, service, "claim", nil, egress.Policy{
		Mode: "none",
	})
	runtime.before = func(name string, _ loom.Resource) error {
		if name == "capture" {
			return fmt.Errorf("explicit capture failure")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	failed, err := service.Execute(ctx, principal, "checkpoint", Command{
		Action:    "checkpoint",
		SandboxID: claim.Result.SandboxID,
	})
	if err == nil || failed.Status != "failed" {
		t.Fatal(failed, err)
	}
	captureLink, _, err := loadResourceLink(ctx, service.store.db, failed.ID, "capture")
	if err != nil {
		t.Fatal(err)
	}
	original, err := service.GetOperation(ctx, "tenant", captureLink.ChildID)
	if err != nil {
		t.Fatal(err)
	}
	released, err := service.Execute(ctx, principal, "release", Command{
		Action:    "release",
		SandboxID: claim.Result.SandboxID,
	})
	if err != nil {
		service.mu.Lock()
		reason := service.lastErrors[released.ID]
		service.mu.Unlock()
		t.Fatal(released, reason, err)
	}
	var slots, storage, vms int
	if err = service.store.db.QueryRow("SELECT (SELECT COALESCE(SUM(sandbox_slots),0) FROM quota_resources),(SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources),(SELECT COUNT(*) FROM vms)").Scan(&slots, &storage, &vms); err != nil || slots != 0 || storage != 0 || vms != 0 {
		t.Fatal(slots, storage, vms, err)
	}
	after, err := service.GetOperation(ctx, "tenant", captureLink.ChildID)
	if err != nil || !reflect.DeepEqual(original, after) {
		t.Fatal("release changed original capture failure", original, after, err)
	}
}

func TestHavenReleasePausedAndFailedCandidates(t *testing.T) {
	for _, boundary := range []string{
		"paused",
		"boot",
		"rollback",
	} {
		t.Run(boundary, func(t *testing.T) {
			service, runtime, _, _ := sandboxFixture(t)
			if boundary == "rollback" {
				service.opts.Backend = &generationRuntime{
					restorableRuntime: &restorableRuntime{runtime},
					known:             map[string]loom.Resource{},
					states:            map[string]loom.Observation{},
				}
			}
			principal := Principal{
				TenantID: "tenant",
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if boundary == "boot" {
				runtime.mu.Lock()
				runtime.failBoot = true
				runtime.mu.Unlock()
			}
			claim, err := service.AdmitClaim(ctx, principal, "claim", Command{
				Action:     "claim",
				TemplateID: "cold",
				Policy: egress.Policy{
					Mode: "none",
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			claim, claimErr := service.Wait(ctx, principal, claim.ID)
			if boundary != "boot" && claimErr != nil {
				t.Fatal(claimErr)
			}
			if boundary == "paused" {
				if _, err = service.Execute(ctx, principal, "pause", Command{
					Action:    "pause",
					SandboxID: claim.Result.SandboxID,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "rollback" {
				checkpoint, err := service.Execute(ctx, principal, "checkpoint", Command{
					Action:    "checkpoint",
					SandboxID: claim.Result.SandboxID,
				})
				if err != nil {
					t.Fatal(err)
				}
				runtime.before = func(name string, _ loom.Resource) error {
					if name == "boot" {
						return fmt.Errorf("explicit failed rollback boot")
					}
					return nil
				}
				if _, err = service.Execute(ctx, principal, "rollback", Command{
					Action:       "rollback",
					SandboxID:    claim.Result.SandboxID,
					CheckpointID: checkpoint.Result.CheckpointID,
				}); err == nil {
					t.Fatal("rollback unexpectedly succeeded")
				}
			}
			released, err := service.Execute(ctx, principal, "release", Command{
				Action:    "release",
				SandboxID: claim.Result.SandboxID,
			})
			if err != nil {
				service.mu.Lock()
				reason := service.lastErrors[released.ID]
				service.mu.Unlock()
				t.Fatal(released, reason, err)
			}
			sandbox, err := service.Get(ctx, principal, claim.Result.SandboxID)
			if err != nil || sandbox.State != "terminated" {
				t.Fatal(sandbox, err)
			}
			var slots, vms, leases int
			if err = service.store.db.QueryRow("SELECT (SELECT COALESCE(SUM(sandbox_slots),0) FROM quota_resources),(SELECT COUNT(*) FROM vms),(SELECT COUNT(*) FROM source_leases)").Scan(&slots, &vms, &leases); err != nil || slots != 0 || vms != 0 || leases != 0 {
				t.Fatal(slots, vms, leases, err)
			}
		})
	}
}

func TestHavenRollbackKeepsLogicalSandbox(t *testing.T) {
	for _, state := range []string{
		"running",
		"paused",
	} {
		t.Run(state, func(t *testing.T) {
			service, runtime, _, _ := sandboxFixture(t)
			service.opts.Backend = &generationRuntime{
				restorableRuntime: &restorableRuntime{runtime},
				known:             map[string]loom.Resource{},
				states:            map[string]loom.Observation{},
			}
			principal := Principal{
				TenantID: "tenant",
			}
			claim := claimSandbox(t, service, "claim", nil, egress.Policy{
				Mode: "none",
			})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			checkpoint, err := service.Execute(ctx, principal, "checkpoint", Command{
				Action:    "checkpoint",
				SandboxID: claim.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			if state == "paused" {
				if _, err = service.Execute(ctx, principal, "pause", Command{
					Action:    "pause",
					SandboxID: claim.Result.SandboxID,
				}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := service.Get(ctx, principal, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			rollback, err := service.Execute(ctx, principal, "rollback", Command{
				Action:       "rollback",
				SandboxID:    before.ID,
				CheckpointID: checkpoint.Result.CheckpointID,
			})
			if err != nil {
				service.mu.Lock()
				reason := service.lastErrors[rollback.ID]
				service.mu.Unlock()
				t.Fatal(rollback, reason, err)
			}
			after, err := service.Get(ctx, principal, before.ID)
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
			if err = service.store.db.QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources").Scan(&slots); err != nil || slots != 1 {
				t.Fatal(slots, err)
			}
		})
	}
}

func TestHavenForkUsesCheckpointOwner(t *testing.T) {
	service, runtime, _, _ := sandboxFixture(t)
	service.opts.Backend = &restorableRuntime{runtime}
	principal := Principal{
		TenantID: "tenant",
	}
	claim := claimSandbox(t, service, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	checkpoint, err := service.Execute(ctx, principal, "checkpoint", Command{
		Action:    "checkpoint",
		SandboxID: claim.Result.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, actor := range []Principal{
		principal,
		{
			Root:     true,
			TenantID: "root",
		},
	} {
		fork, err := service.Execute(ctx, actor, "fork", Command{
			Action:       "fork",
			CheckpointID: checkpoint.Result.CheckpointID,
			Policy: egress.Policy{
				Mode: "none",
			},
		})
		if err != nil {
			service.mu.Lock()
			reason := service.lastErrors[fork.ID]
			service.mu.Unlock()
			t.Fatal(fork, reason, err)
		}
		sandbox, err := service.DataReady(ctx, principal, fork.Result.SandboxID)
		if err != nil || sandbox.State != "running" || sandbox.ID == claim.Result.SandboxID || sandbox.TenantID != "tenant" || sandbox.TemplateID != "cold" {
			t.Fatal(sandbox, err)
		}
	}
	if _, err = service.AdmitFork(ctx, Principal{
		TenantID: "other",
	}, "fork", Command{
		Action:       "fork",
		CheckpointID: checkpoint.Result.CheckpointID,
		Policy: egress.Policy{
			Mode: "none",
		},
	}); err == nil {
		t.Fatal("foreign tenant fork accepted")
	}
	var slots int
	if err = service.store.db.QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant'").Scan(&slots); err != nil || slots != 3 {
		t.Fatal(slots, err)
	}
}

func TestHavenResumeFailurePreservesSleep(t *testing.T) {
	for _, boundary := range []string{
		"boot",
		"activate",
	} {
		t.Run(boundary, func(t *testing.T) {
			service, runtime, guest, _ := sandboxFixture(t)
			service.opts.Backend = &restorableRuntime{runtime}
			principal := Principal{
				TenantID: "tenant",
			}
			claim := claimSandbox(t, service, "claim", nil, egress.Policy{
				Mode: "none",
			})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := service.Execute(ctx, principal, "pause", Command{
				Action:    "pause",
				SandboxID: claim.Result.SandboxID,
			}); err != nil {
				t.Fatal(err)
			}
			before, err := service.Get(ctx, principal, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			if boundary == "boot" {
				runtime.mu.Lock()
				runtime.failBoot = true
				runtime.mu.Unlock()
			} else {
				guest.before = func(name string, _ loom.Resource) error {
					if name == "activate" {
						return fmt.Errorf("explicit activation failure")
					}
					return nil
				}
			}
			failed, err := service.Execute(ctx, principal, "resume", Command{
				Action:    "resume",
				SandboxID: before.ID,
			})
			if err == nil || failed.Status != "failed" {
				t.Fatal(failed, err)
			}
			for {
				step, err := loadLifecycle(ctx, service.store.db, failed.ID)
				if err != nil {
					t.Fatal(err)
				}
				if step.Status == "failed" {
					break
				}
				select {
				case <-ctx.Done():
					service.mu.Lock()
					reason := service.lastErrors[failed.ID]
					service.mu.Unlock()
					t.Fatal("resume cleanup did not finish", reason)
				case <-time.After(time.Millisecond):
				}
			}
			after, err := service.Get(ctx, principal, before.ID)
			if err != nil || after.State != "paused" || after.SnapshotID != before.SnapshotID || after.VMID != "" {
				t.Fatal(after, err)
			}
			var slots, storage, vms int
			if err = service.store.db.QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources),(SELECT COUNT(*) FROM vms)").Scan(&slots, &storage, &vms); err != nil || slots != 1 || storage != 192 || vms != 0 {
				t.Fatal(slots, storage, vms, err)
			}
		})
	}
}

func TestHavenDataResumeConverges(t *testing.T) {
	service, runtime, _, _ := sandboxFixture(t)
	service.opts.Backend = &restorableRuntime{runtime}
	principal := Principal{
		TenantID: "tenant",
	}
	claim := claimSandbox(t, service, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := service.Execute(ctx, principal, "pause", Command{
		Action:    "pause",
		SandboxID: claim.Result.SandboxID,
	}); err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 8)
	for range 8 {
		go func() {
			sandbox, err := service.DataReady(ctx, Principal{
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
	if err := service.store.db.QueryRow("SELECT COUNT(*) FROM operation_steps WHERE step_key='lifecycle' AND action='resume'").Scan(&attempts); err != nil || attempts != 1 {
		t.Fatal("concurrent data created duplicate attempts", attempts, err)
	}
}

func TestHavenResumeRetainsSandboxSlot(t *testing.T) {
	service, runtime, _, _ := sandboxFixture(t)
	service.opts.Backend = &restorableRuntime{runtime}
	principal := Principal{
		TenantID: "tenant",
	}
	claim := claimSandbox(t, service, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := service.Execute(ctx, principal, "pause", Command{
		Action:    "pause",
		SandboxID: claim.Result.SandboxID,
	}); err != nil {
		t.Fatal(err)
	}
	before, err := service.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := service.Execute(ctx, principal, "resume", Command{
		Action:    "resume",
		SandboxID: before.ID,
	})
	if err != nil {
		service.mu.Lock()
		reason := service.lastErrors[resumed.ID]
		service.mu.Unlock()
		t.Fatal(resumed, reason, err)
	}
	after, err := service.DataReady(ctx, principal, before.ID)
	if err != nil || after.State != "running" || after.VMID == claim.Result.VMID || after.Instance == "" || after.SnapshotID != "" {
		t.Fatal(after, err)
	}
	var slots, backings int
	if err = service.store.db.QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant'),(SELECT COUNT(*) FROM backing)").Scan(&slots, &backings); err != nil || slots != 1 || backings != 1 {
		t.Fatal("resume lost/duplicated responsibilities", slots, backings, err)
	}
}

func TestHavenLifecycleAdmissionAndHandoff(t *testing.T) {
	for _, action := range []string{
		"checkpoint",
		"pause",
		"release",
	} {
		t.Run(action, func(t *testing.T) {
			service, _, _, _ := sandboxFixture(t)
			claim := claimSandbox(t, service, "claim", nil, egress.Policy{
				Mode: "none",
			})
			operation, err := service.Submit(context.Background(), Principal{
				TenantID: "tenant",
			}, action, Command{
				Action:    action,
				SandboxID: claim.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			result, err := service.Wait(ctx, Principal{
				TenantID: "tenant",
			}, operation.ID)
			if err != nil {
				service.mu.Lock()
				reason := service.lastErrors[operation.ID]
				service.mu.Unlock()
				t.Fatal("lifecycle failed", action, result, reason, err)
			}
			sandbox, err := service.Get(ctx, Principal{
				TenantID: "tenant",
			}, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			var slots, storage int
			if err = service.store.db.QueryRow("SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE tenant_id='tenant'").Scan(&slots, &storage); err != nil {
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
