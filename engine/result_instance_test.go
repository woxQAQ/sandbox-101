package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func resultInstance(t *testing.T, o api.Operation) string {
	t.Helper()
	var wire struct {
		Instance string `json:"result_instance"`
	}
	if err := json.Unmarshal(encode(o), &wire); err != nil {
		t.Fatal(err)
	}
	return wire.Instance
}

func TestOperationResultInstanceLifecycle(t *testing.T) {
	b := newTestBackend()
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	create := wait(
		t,
		e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	start := wait(
		t,
		e,
		api.Request{Key: "start", Action: "start", VMID: create.VMID},
	)
	v, err := e.GetVM(context.Background(), create.VMID)
	if err != nil {
		t.Fatal(err)
	}
	if resultInstance(t, start) == "" ||
		resultInstance(t, start) != v.Instance {
		t.Fatalf("start lacks immutable instance witness: %+v", start)
	}
	if resultInstance(t, create) != "" {
		t.Fatal("create has generation witness")
	}
	snap := wait(
		t,
		e,
		api.Request{Key: "snapshot", Action: "snapshot", VMID: v.ID},
	)
	stop := wait(t, e, api.Request{Key: "stop", Action: "stop", VMID: v.ID})
	restored := wait(
		t,
		e,
		api.Request{
			Key:              "restore",
			Action:           "restore",
			VMID:             v.ID,
			SnapshotID:       snap.SnapshotID,
			ExpectedInstance: v.Instance,
		},
	)
	current, err := e.GetVM(context.Background(), v.ID)
	if err != nil || resultInstance(t, restored) != current.Instance ||
		current.Instance == v.Instance {
		t.Fatalf("restore witness: %+v %+v %v", restored, current, err)
	}
	clone := wait(
		t,
		e,
		api.Request{
			Key:        "clone",
			Action:     "clone",
			SnapshotID: snap.SnapshotID,
			Config:     config(),
		},
	)
	cloned, err := e.GetVM(context.Background(), clone.VMID)
	if err != nil || clone.Request.VMID != "" ||
		resultInstance(t, clone) == "" ||
		resultInstance(t, clone) != cloned.Instance {
		t.Fatalf("clone witness: %+v %v", clone, err)
	}
	for _, o := range []api.Operation{snap, stop} {
		if o.ResultInstance != "" {
			t.Fatal("non-generation witness")
		}
	}
	conflict, conflictErr := api.Wait(
		context.Background(),
		e,
		api.Request{
			Key:              "already-running",
			Action:           "start",
			VMID:             current.ID,
			ExpectedInstance: current.Instance,
		},
	)
	var conflictAPI *api.Error
	if !errors.As(conflictErr, &conflictAPI) ||
		conflictAPI.Code != api.Conflict ||
		conflict.Status != "failed" ||
		conflict.ResultInstance != "" {
		t.Fatalf(
			"running start contract changed: %+v %v",
			conflict,
			conflictErr,
		)
	}
	network := wait(
		t,
		e,
		api.Request{
			Key:              "network",
			Action:           "network",
			VMID:             current.ID,
			ExpectedInstance: current.Instance,
		},
	)
	deleted := wait(
		t,
		e,
		api.Request{
			Key:              "delete-clone",
			Action:           "delete",
			VMID:             clone.VMID,
			ExpectedInstance: clone.ResultInstance,
		},
	)
	if network.ResultInstance != "" || deleted.ResultInstance != "" {
		t.Fatal("other action has generation witness")
	}
}

func TestOperationResultInstanceAtomicCompletion(t *testing.T) {
	for _, action := range []string{"start", "restore"} {
		for _, fault := range []string{"vm", "terminal", "commit"} {
			t.Run(action+"/"+fault, func(t *testing.T) {
				f := newInstanceRecoveryFixture(t, action, "agent_ready")
				candidate := f.record.Journal.Resource.Instance
				switch fault {
				case "vm":
					_, err := f.e.db.Exec(
						`CREATE TRIGGER reject_finish BEFORE UPDATE ON vms WHEN json_extract(NEW.body,'$.vm.operation_id') IS NULL BEGIN SELECT RAISE(ABORT,'completion VM'); END`,
					)
					if err != nil {
						t.Fatal(err)
					}
				case "terminal":
					_, err := f.e.db.Exec(
						`CREATE TRIGGER reject_finish BEFORE UPDATE ON operations WHEN NEW.status='succeeded' BEGIN SELECT RAISE(ABORT,'completion terminal'); END`,
					)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := f.e.Close(); err != nil {
					t.Fatal(err)
				}
				reached := make(chan struct{})
				var once sync.Once
				f.e = openTest(t, f.dir, f.b, false, func(step string) error {
					if step == "agent_ready" {
						once.Do(func() { close(reached) })
					}
					if step == "finish_before_commit" && fault == "commit" {
						return ErrInterrupted
					}
					return nil
				})
				select {
				case <-reached:
				case <-time.After(10 * time.Second):
					t.Fatal("delivery not reached")
				}
				lock := f.e.vmLock(f.op.VMID)
				lock.Lock()
				op, err := f.e.loadOp(context.Background(), f.op.ID)
				v, vErr := f.e.GetVM(context.Background(), f.op.VMID)
				lock.Unlock()
				if err != nil || vErr != nil || terminal(op.Operation.Status) ||
					op.Operation.ResultInstance != "" ||
					v.OperationID != f.op.ID ||
					v.Instance != candidate {
					t.Fatalf("half completion: %+v %+v %v %v", op, v, err, vErr)
				}
				if _, err = f.e.db.Exec(
					`DROP TRIGGER IF EXISTS reject_finish`,
				); err != nil {
					t.Fatal(err)
				}
				launches := f.b.launchCalls.Load()
				f.restart(t)
				final := awaitStatus(t, f.e, f.op.ID)
				if final.Status != "succeeded" ||
					final.ResultInstance != candidate ||
					f.b.launchCalls.Load() != launches {
					t.Fatalf("same generation not adopted: %+v", final)
				}
				found, err := f.e.FindOperation(
					context.Background(),
					f.request.Key,
				)
				cached, cErr := f.e.Submit(context.Background(), f.request)
				if err != nil || cErr != nil ||
					!bytes.Equal(encode(found), encode(final)) ||
					!bytes.Equal(encode(cached), encode(final)) {
					t.Fatal("receipt cache differs", err, cErr)
				}
			})
		}
	}
}

func TestOperationResultInstanceRejectsForeignCandidate(t *testing.T) {
	for _, damage := range []string{"owner", "vm", "instance", "disk", "run", "config", "slot", "token", "decode"} {
		t.Run(damage, func(t *testing.T) {
			f := newInstanceRecoveryFixture(t, "start", "agent_ready")
			if err := f.e.Close(); err != nil {
				t.Fatal(err)
			}
			reached := make(chan struct{})
			var once sync.Once
			// The completion boundary is after real Inspect/Boot/Agent/Inspect delivery.
			// Damage is persisted before the finish transaction reads the owned candidate.
			var current *Engine
			gate := make(chan struct{})
			current = openTest(t, f.dir, f.b, false, func(step string) error {
				if step != "finish_candidate_ready" {
					return nil
				}
				<-gate
				once.Do(func() {
					v, err := current.loadVM(context.Background(), f.op.VMID)
					if err != nil {
						panic(err)
					}
					switch damage {
					case "owner":
						v.VM.OperationID = "foreign"
					case "vm":
						v.Resource.VMID = "foreign"
					case "instance":
						v.VM.Instance = "foreign"
						v.Resource.Instance = "foreign"
					case "disk":
						v.Resource.Disk += ".foreign"
					case "run":
						v.Resource.RunDir += ".foreign"
					case "config":
						v.Resource.Config.Shape.DiskMiB++
					case "slot":
						v.Resource.Slot++
					case "token":
						v.Resource.NetworkToken += "foreign"
					}
					if damage == "decode" {
						_, err = current.db.Exec(
							`UPDATE vms SET body='{' WHERE id=?`,
							f.op.VMID,
						)
					} else {
						err = current.saveVM(v)
					}
					if err != nil {
						panic(err)
					}
					close(reached)
				})
				return nil
			})
			f.e = current
			close(gate)
			select {
			case <-reached:
			case <-time.After(10 * time.Second):
				t.Fatal("completion boundary missing")
			}
			lock := f.e.vmLock(f.op.VMID)
			lock.Lock()
			op, err := f.e.GetOperation(context.Background(), f.op.ID)
			lock.Unlock()
			if err != nil || terminal(op.Status) || op.ResultInstance != "" {
				t.Fatalf("foreign candidate published: %+v %v", op, err)
			}
		})
	}
}

func TestOperationResultInstanceSurvivesRestartAndNewerInstance(t *testing.T) {
	b := newTestBackend()
	dir := t.TempDir()
	e := openTest(t, dir, b, true, nil)
	defer func() { _ = e.Close() }()
	created := wait(
		t,
		e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	first := wait(
		t,
		e,
		api.Request{Key: "start-A", Action: "start", VMID: created.VMID},
	)
	snapshot := wait(
		t,
		e,
		api.Request{
			Key:              "capture-A",
			Action:           "snapshot",
			VMID:             created.VMID,
			ExpectedInstance: first.ResultInstance,
		},
	)
	wait(
		t,
		e,
		api.Request{
			Key:              "stop-A",
			Action:           "stop",
			VMID:             created.VMID,
			ExpectedInstance: first.ResultInstance,
		},
	)
	request := api.Request{
		Key:              "original",
		Action:           "restore",
		VMID:             created.VMID,
		SnapshotID:       snapshot.SnapshotID,
		ExpectedInstance: first.ResultInstance,
	}
	original := wait(t, e, request)
	wire := encode(original)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = openTest(t, dir, b, false, nil)
	wait(
		t,
		e,
		api.Request{
			Key:              "stop",
			Action:           "stop",
			VMID:             created.VMID,
			ExpectedInstance: original.ResultInstance,
		},
	)
	next := wait(
		t,
		e,
		api.Request{
			Key:              "newer",
			Action:           "start",
			VMID:             created.VMID,
			ExpectedInstance: original.ResultInstance,
		},
	)
	if next.ResultInstance == original.ResultInstance {
		t.Fatal("no successor generation")
	}
	wait(
		t,
		e,
		api.Request{
			Key:              "delete",
			Action:           "delete",
			VMID:             created.VMID,
			ExpectedInstance: next.ResultInstance,
		},
	)
	for _, read := range []func() (api.Operation, error){func() (api.Operation, error) { return e.GetOperation(context.Background(), original.ID) }, func() (api.Operation, error) { return e.FindOperation(context.Background(), request.Key) }, func() (api.Operation, error) { return e.Submit(context.Background(), request) }} {
		got, err := read()
		if err != nil || !bytes.Equal(wire, encode(got)) {
			t.Fatal("historical witness changed", err)
		}
	}
}

func TestOperationResultInstanceFailureAndLegacy(t *testing.T) {
	for _, fault := range []string{"inspect", "agent", "legacy", "material"} {
		t.Run(fault, func(t *testing.T) {
			if fault == "material" {
				f := newInstanceRecoveryFixture(
					t,
					"restore",
					"restore_resources_ready",
				)
				if err := os.WriteFile(
					f.record.Journal.Resource.Disk,
					[]byte("damaged"),
					0o600,
				); err != nil {
					t.Fatal(err)
				}
				f.restart(t)
				final := awaitStatus(t, f.e, f.op.ID)
				if final.Status != "failed" || final.ResultInstance != "" {
					t.Fatalf("damaged restore witnessed: %+v", final)
				}
				return
			}
			b := newTestBackend()
			dir := t.TempDir()
			e := openTest(t, dir, b, true, nil)
			defer func() { _ = e.Close() }()
			created := wait(
				t,
				e,
				api.Request{Key: "create", Action: "create", Config: config()},
			)
			if fault != "legacy" {
				b.mu.Lock()
				if fault == "inspect" {
					b.inspectError = errors.New("unknown process identity")
				} else {
					b.agentError = errors.New("agent handshake failed")
				}
				b.mu.Unlock()
				op, err := e.Submit(
					context.Background(),
					api.Request{
						Key:    "start",
						Action: "start",
						VMID:   created.VMID,
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				final := awaitStatus(t, e, op.ID)
				if final.Status != "failed" || final.ResultInstance != "" {
					t.Fatalf("failed operation witnessed: %+v", final)
				}
				return
			}
			req := api.Request{
				Key:    "start",
				Action: "start",
				VMID:   created.VMID,
			}
			op := wait(t, e, req)
			record, err := e.loadOp(context.Background(), op.ID)
			if err != nil {
				t.Fatal(err)
			}
			record.Operation.ResultInstance = ""
			if _, err = e.db.Exec(
				`UPDATE operations SET body=? WHERE id=?`,
				encode(record),
				op.ID,
			); err != nil {
				t.Fatal(err)
			}
			before := encode(record.Operation)
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			e = openTest(t, dir, b, false, nil)
			got, err := e.Submit(context.Background(), req)
			if err != nil || !bytes.Equal(before, encode(got)) {
				t.Fatal("legacy receipt enriched", err)
			}
			got, err = e.FindOperation(context.Background(), req.Key)
			if err != nil || !bytes.Equal(before, encode(got)) {
				t.Fatal("legacy Find enriched", err)
			}
		})
	}
}
