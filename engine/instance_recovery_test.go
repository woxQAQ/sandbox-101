package engine

import (
	"context"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

// Count effects at the Backend boundary while the real Engine persists and replays.
type instanceRecoveryBackend struct {
	*testBackend

	boundary    string
	armed       atomic.Bool
	interrupted chan struct{}
	stops       atomic.Int32
	launchCalls atomic.Int32
	removes     atomic.Int32
	networkOn   atomic.Int32
}

func (b *instanceRecoveryBackend) Launch(
	ctx context.Context,
	r Resource,
	restore bool,
) error {
	b.launchCalls.Add(1)
	interrupt := b.armed.Load() &&
		(b.boundary == "instance_persisted" || b.boundary == "launch_unrecorded")
	if interrupt && b.boundary == "instance_persisted" {
		b.armed.Store(false)
		close(b.interrupted)
		return ErrInterrupted
	}
	if err := b.testBackend.Launch(ctx, r, restore); err != nil {
		return err
	}
	if interrupt {
		b.armed.Store(false)
		close(b.interrupted)
		return ErrInterrupted
	}
	return nil
}

func (b *instanceRecoveryBackend) Stop(ctx context.Context, r Resource) error {
	b.stops.Add(1)
	return b.testBackend.Stop(ctx, r)
}

func (b *instanceRecoveryBackend) Remove(
	ctx context.Context,
	r Resource,
) error {
	b.removes.Add(1)
	return b.testBackend.Remove(ctx, r)
}

func (b *instanceRecoveryBackend) Network(
	ctx context.Context,
	r Resource,
	on bool,
) error {
	if on {
		b.networkOn.Add(1)
	}
	return b.testBackend.Network(ctx, r, on)
}

type instanceRecoveryFixture struct {
	e       *Engine
	dir     string
	b       *instanceRecoveryBackend
	request api.Request
	op      api.Operation
	old     api.VM
	record  opRecord
}

func newInstanceRecoveryFixture(
	t *testing.T,
	action, boundary string,
) *instanceRecoveryFixture {
	t.Helper()
	b := &instanceRecoveryBackend{
		testBackend: newTestBackend(),
		boundary:    boundary,
		interrupted: make(chan struct{}),
	}
	f := &instanceRecoveryFixture{dir: t.TempDir(), b: b}
	f.e = openTest(t, f.dir, b, true, func(step string) error {
		if step == boundary && b.armed.CompareAndSwap(true, false) {
			close(b.interrupted)
			return ErrInterrupted
		}
		return nil
	})
	t.Cleanup(func() { _ = f.e.Close() })
	vm := wait(
		t,
		f.e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	wait(
		t,
		f.e,
		api.Request{Key: "first-start", Action: "start", VMID: vm.VMID},
	)
	f.request = api.Request{Key: "conditional", Action: action, VMID: vm.VMID}
	if action == "restore" {
		snapshot := wait(
			t,
			f.e,
			api.Request{Key: "snapshot", Action: "snapshot", VMID: vm.VMID},
		)
		f.request.SnapshotID = snapshot.SnapshotID
	}
	wait(t, f.e, api.Request{Key: "stop", Action: "stop", VMID: vm.VMID})
	var err error
	f.old, err = f.e.GetVM(context.Background(), vm.VMID)
	if err != nil || f.old.Instance == "" || f.old.Phase != "stopped" {
		t.Fatalf("missing stopped instance A: %+v %v", f.old, err)
	}
	f.request.ExpectedInstance = f.old.Instance
	b.armed.Store(true)
	f.op, err = f.e.Submit(context.Background(), f.request)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.interrupted:
	case <-time.After(10 * time.Second):
		t.Fatal("instance recovery fault was not reached")
	}
	lock := f.e.vmLock(vm.VMID)
	lock.Lock()
	f.record, err = f.e.loadOp(context.Background(), f.op.ID)
	lock.Unlock()
	if err != nil || f.record.Journal.Resource == nil {
		t.Fatalf("missing candidate journal: %+v %v", f.record, err)
	}
	return f
}

func (f *instanceRecoveryFixture) restart(t *testing.T) {
	t.Helper()
	if err := f.e.Close(); err != nil {
		t.Fatal(err)
	}
	f.e = openTest(t, f.dir, f.b, false, nil)
}

type instanceRecoveryEffects struct {
	launches  int32
	stops     int32
	removes   int32
	networkOn int32
}

func (b *instanceRecoveryBackend) effects() instanceRecoveryEffects {
	return instanceRecoveryEffects{
		b.launchCalls.Load(),
		b.stops.Load(),
		b.removes.Load(),
		b.networkOn.Load(),
	}
}

func (f *instanceRecoveryFixture) mutate(
	t *testing.T,
	change func(*opRecord, *vmRecord),
) {
	t.Helper()
	lock := f.e.vmLock(f.op.VMID)
	lock.Lock()
	defer lock.Unlock()
	o, err := f.e.loadOp(context.Background(), f.op.ID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := f.e.loadVM(context.Background(), f.op.VMID)
	if err != nil {
		t.Fatal(err)
	}
	change(&o, &v)
	if err = f.e.saveVM(v); err != nil {
		t.Fatal(err)
	}
	if err = f.e.saveOp(&o, o.Operation.Step); err != nil {
		t.Fatal(err)
	}
}

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
