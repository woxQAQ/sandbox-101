package engine

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestInstanceConditionRejectsForeignCandidate(t *testing.T) {
	for _, action := range []string{"start", "restore"} {
		for _, damage := range []string{
			"operation-pointer", "missing-candidate", "empty-candidate-instance", "candidate-vm",
			"candidate-instance", "vm-instance", "resource-instance", "foreign-current-instance",
			"resource-vm", "resource-dir", "resource-disk", "resource-run", "resource-restore",
			"resource-image", "resource-network-token", "resource-slot", "resource-config", "unknown-identity",
			"action-network", "action-network_policy", "action-stop", "action-delete", "action-snapshot",
		} {
			t.Run(action+"/"+damage, func(t *testing.T) {
				f := newInstanceRecoveryFixture(t, action, "launched")
				f.mutate(t, func(o *opRecord, v *vmRecord) {
					switch damage {
					case "operation-pointer":
						v.VM.OperationID = "op-foreign"
					case "missing-candidate":
						o.Journal.Resource = nil
					case "empty-candidate-instance":
						o.Journal.Resource.Instance = ""
					case "candidate-vm":
						o.Journal.Resource.VMID = "vm-foreign"
						v.Resource.VMID = "vm-foreign"
					case "candidate-instance":
						o.Journal.Resource.Instance = "run-foreign"
					case "vm-instance":
						v.VM.Instance = "run-foreign"
					case "resource-instance":
						v.Resource.Instance = "run-foreign"
					case "foreign-current-instance":
						v.VM.Instance = "run-foreign"
						v.Resource.Instance = "run-foreign"
						f.b.mu.Lock()
						f.b.states["run-foreign"] = Observation{
							Alive:     true,
							PID:       999,
							StartTime: 999,
						}
						f.b.mu.Unlock()
					case "resource-vm":
						v.Resource.VMID = "vm-foreign"
					case "resource-dir":
						v.Resource.Dir += ".foreign"
					case "resource-disk":
						v.Resource.Disk += ".foreign"
					case "resource-run":
						v.Resource.RunDir += ".foreign"
					case "resource-restore":
						v.Resource.RestoreDir += ".foreign"
					case "resource-image":
						v.Resource.ImageDir += ".foreign"
					case "resource-network-token":
						v.Resource.NetworkToken += "foreign"
					case "resource-slot":
						v.Resource.Slot++
					case "resource-config":
						v.Resource.Config.Shape.MemoryMiB++
					case "unknown-identity":
						f.b.mu.Lock()
						f.b.inspectError = api.Fail(
							api.ManualRecovery,
							"candidate PID/starttime unknown",
						)
						f.b.mu.Unlock()
					default:
						o.Operation.Request.Action = damage[len("action-"):]
					}
				})
				before, err := f.e.loadVM(context.Background(), f.op.VMID)
				if err != nil {
					t.Fatal(err)
				}
				candidate := *f.record.Journal.Resource
				f.b.mu.Lock()
				states := make(map[string]Observation, len(f.b.states))
				for id, obs := range f.b.states {
					states[id] = obs
				}
				f.b.mu.Unlock()
				effects := f.b.effects()
				f.restart(t)
				final := awaitStatus(t, f.e, f.op.ID)
				if final.Status != "failed" || final.Error == nil ||
					(final.Error.Code != api.Conflict && final.Error.Code != api.ManualRecovery) {
					t.Fatalf(
						"foreign or unknown candidate accepted: %+v",
						final,
					)
				}
				after, err := f.e.loadVM(context.Background(), f.op.VMID)
				if err != nil || after.VM.Instance != before.VM.Instance ||
					!reflect.DeepEqual(after.Resource, before.Resource) {
					t.Fatalf(
						"recovery replaced foreign resource: %+v %v",
						after,
						err,
					)
				}
				if damage == "operation-pointer" &&
					after.VM.OperationID != "op-foreign" {
					t.Fatal("recovery took another operation's ownership")
				}
				if damage == "unknown-identity" && after.VM.Verified {
					t.Fatal("uncertain candidate marked verified")
				}
				if f.b.effects() != effects {
					t.Fatalf(
						"rejection changed instance effects: before=%+v after=%+v",
						effects,
						f.b.effects(),
					)
				}
				f.b.mu.Lock()
				// Startup may record default-off network on an absent test instance.
				for id, obs := range states {
					if f.b.states[id] != obs {
						t.Errorf(
							"rejection changed runtime instance %s: %+v -> %+v",
							id,
							obs,
							f.b.states[id],
						)
					}
				}
				if !f.b.states[candidate.Instance].Alive {
					t.Error("rejection stopped original candidate")
				}
				f.b.mu.Unlock()
			})
		}
	}
}

func TestInstanceConditionKeepsTerminalResult(t *testing.T) {
	for _, action := range []string{"start", "restore"} {
		for _, status := range []string{"succeeded", "failed"} {
			t.Run(action+"/"+status, func(t *testing.T) {
				f := newInstanceRecoveryFixture(t, action, "launched")
				if status == "failed" {
					f.mutate(t, func(_ *opRecord, v *vmRecord) {
						v.VM.Instance = "run-foreign"
						v.Resource.Instance = "run-foreign"
					})
				}
				f.restart(t)
				final := awaitStatus(t, f.e, f.op.ID)
				if final.Status != status ||
					!reflect.DeepEqual(final.Request, f.request) {
					t.Fatalf("unexpected original result: %+v", final)
				}
				original := string(encode(final))
				effects := f.b.effects()
				for range 2 {
					f.restart(t)
					for range 3 {
						same, err := f.e.Submit(context.Background(), f.request)
						if err != nil || string(encode(same)) != original {
							t.Fatalf(
								"terminal replay changed original response: %+v %v",
								same,
								err,
							)
						}
						changed := f.request
						changed.ExpectedInstance = f.record.Journal.Resource.Instance
						same, err = f.e.Submit(context.Background(), changed)
						if err == nil || asAPI(err).Code != api.Conflict ||
							string(encode(same)) != original {
							t.Fatalf(
								"same key accepted different parameters: %+v %v",
								same,
								err,
							)
						}
						stale := f.request
						stale.Key = "new-key-stale-instance"
						if _, err = f.e.Submit(
							context.Background(),
							stale,
						); err == nil ||
							asAPI(err).Code != api.Conflict {
							t.Fatalf("admission accepted stale A: %v", err)
						}
						if _, err = f.e.FindOperation(
							context.Background(),
							stale.Key,
						); err == nil ||
							asAPI(err).Code != api.NotFound {
							t.Fatalf(
								"rejected admission persisted a new operation: %v",
								err,
							)
						}
						found, err := f.e.FindOperation(
							context.Background(),
							f.request.Key,
						)
						if err != nil || string(encode(found)) != original {
							t.Fatalf(
								"original keyed result changed: %+v %v",
								found,
								err,
							)
						}
					}
				}
				if f.b.effects() != effects {
					t.Fatalf(
						"terminal replay changed instance effects: before=%+v after=%+v",
						effects,
						f.b.effects(),
					)
				}
			})
		}
	}
}

func TestInstanceConditionStartRecovery(t *testing.T) {
	for _, boundary := range []string{"launch_intent", "instance_persisted", "launch_unrecorded", "launched", "booted", "agent_ready"} {
		t.Run(boundary, func(t *testing.T) {
			assertInstanceConditionRecovery(t, "start", boundary)
		})
	}
}

func TestInstanceConditionRestoreRecovery(t *testing.T) {
	for _, boundary := range []string{"restore_resources_ready", "restore_launch_intent", "instance_persisted", "launch_unrecorded", "launched", "booted", "agent_ready"} {
		t.Run(boundary, func(t *testing.T) {
			assertInstanceConditionRecovery(t, "restore", boundary)
		})
	}
}

func assertInstanceConditionRecovery(t *testing.T, action, boundary string) {
	t.Helper()
	f := newInstanceRecoveryFixture(t, action, boundary)
	r := *f.record.Journal.Resource
	if r.Instance == "" || r.Instance == f.old.Instance {
		t.Fatal("operation did not allocate fresh candidate B")
	}
	var guestWrite []byte
	if boundary == "launch_unrecorded" || boundary == "launched" ||
		boundary == "booted" ||
		boundary == "agent_ready" {
		guestWrite = []byte("candidate guest writes after launch")
		if err := os.WriteFile(r.Disk, guestWrite, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f.restart(t)
	final := awaitStatus(t, f.e, f.op.ID)
	if final.Status != "succeeded" || final.ID != f.op.ID ||
		!reflect.DeepEqual(final.Request, f.request) {
		t.Fatalf("conditional %s recovery failed: %+v", action, final)
	}
	v, err := f.e.loadVM(context.Background(), f.op.VMID)
	if err != nil || v.VM.Instance != r.Instance ||
		!reflect.DeepEqual(v.Resource, r) ||
		v.VM.Phase != "running" ||
		!v.VM.AgentReady ||
		!v.VM.Verified ||
		v.VM.NetworkActual ||
		v.VM.NetworkEnabled ||
		v.VM.NetworkPolicy != nil ||
		v.VM.NetworkPolicyInstance != "" {
		t.Fatalf("candidate identity/readiness/network changed: %+v %v", v, err)
	}
	f.b.mu.Lock()
	launches := f.b.launches
	obs := f.b.states[r.Instance]
	f.b.mu.Unlock()
	if launches != 2 || !obs.Alive || v.VM.PID != obs.PID ||
		v.VM.StartTime != obs.StartTime ||
		obs.Network {
		t.Fatalf(
			"candidate was relaunched or lost process identity: launches=%d vm=%+v observed=%+v",
			launches,
			v.VM,
			obs,
		)
	}
	if guestWrite != nil {
		got, err := os.ReadFile(r.Disk)
		if err != nil || string(got) != string(guestWrite) {
			t.Fatalf("candidate disk was rebuilt: %q %v", got, err)
		}
	}
	after, err := f.e.loadOp(context.Background(), f.op.ID)
	if err != nil ||
		!reflect.DeepEqual(
			after.Journal.RestoreMaterials,
			f.record.Journal.RestoreMaterials,
		) {
		t.Fatalf("restore delivery proof changed: %v", err)
	}
	if f.b.stops.Load() != 1 || f.b.removes.Load() != 0 ||
		f.b.networkOn.Load() != 0 {
		t.Fatal("recovery stopped, deleted, or enabled network on an instance")
	}
}
