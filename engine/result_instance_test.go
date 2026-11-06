package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
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

func TestOperationResultInstanceIgnoredWrites(t *testing.T) {
	for _, action := range []string{"start", "restore"} {
		for _, target := range []string{"vm", "terminal"} {
			t.Run(action+"/"+target, func(t *testing.T) {
				f := newInstanceRecoveryFixture(t, action, "agent_ready")
				candidate := *f.record.Journal.Resource
				launches := f.b.launchCalls.Load()
				f.b.mu.Lock()
				physicalBefore := f.b.states[candidate.Instance]
				f.b.mu.Unlock()
				if err := f.e.Close(); err != nil {
					t.Fatal(err)
				}
				gate, reached := make(chan struct{}), make(chan struct{})
				var install sync.Once
				var current *Engine
				current = openTest(
					t,
					f.dir,
					f.b,
					false,
					func(step string) error {
						if step != "finish_candidate_ready" {
							return nil
						}
						<-gate
						var installErr error
						install.Do(func() {
							defer close(reached)
							_, installErr = current.db.Exec(
								`CREATE TABLE ignored_finish_writes(n INTEGER)`,
							)
							if installErr != nil {
								return
							}
							statement := `CREATE TRIGGER ignore_finish BEFORE UPDATE ON vms
							WHEN json_extract(NEW.body,'$.vm.operation_id') IS NULL
							BEGIN INSERT INTO ignored_finish_writes VALUES(1); SELECT RAISE(IGNORE); END`
							if target == "terminal" {
								statement = `CREATE TRIGGER ignore_finish BEFORE UPDATE ON operations
								WHEN NEW.status='succeeded' AND OLD.status IN ('pending','running')
								BEGIN INSERT INTO ignored_finish_writes VALUES(1); SELECT RAISE(IGNORE); END`
							}
							_, installErr = current.db.Exec(statement)
						})
						return installErr
					},
				)
				f.e = current
				close(gate)
				finishAttemptSettled(t, current, f.op.VMID, reached)
				op, vm, leases, hits := finishIgnoredState(
					t,
					current,
					f.op.ID,
					f.op.VMID,
				)
				f.b.mu.Lock()
				physicalFault := f.b.states[candidate.Instance]
				f.b.mu.Unlock()
				t.Logf(
					"fault candidate=%s physicalBefore=%+v physicalAfter=%+v launchCalls=%d baseline=%d",
					encode(candidate),
					physicalBefore,
					physicalFault,
					f.b.launchCalls.Load(),
					launches,
				)
				if physicalFault != physicalBefore ||
					f.b.launchCalls.Load() != launches {
					t.Error("completion changed candidate runtime effects")
				}
				if terminal(op.Operation.Status) ||
					op.Operation.ResultInstance != "" ||
					vm.VM.OperationID != f.op.ID ||
					vm.VM.Instance != candidate.Instance ||
					!reflect.DeepEqual(vm.Resource, candidate) ||
					!reflect.DeepEqual(op.Journal, f.record.Journal) ||
					!vm.VM.Verified ||
					!vm.VM.AgentReady ||
					leases != 0 ||
					hits != 0 {
					t.Errorf(
						"ignored %s write committed half completion: op=%+v VM=%+v leases=%d hits=%d",
						target,
						op,
						vm,
						leases,
						hits,
					)
				}
				ctx, cancel := context.WithTimeout(
					context.Background(),
					100*time.Millisecond,
				)
				waited, waitErr := api.Wait(ctx, current, f.request)
				cancel()
				t.Logf(
					"fault Wait: request=%s operation=%s error=%v",
					encode(f.request),
					encode(waited),
					waitErr,
				)
				if !errors.Is(waitErr, context.DeadlineExceeded) {
					t.Errorf(
						"fault published terminal response: %+v %v",
						waited,
						waitErr,
					)
				}
				found, err := current.FindOperation(
					context.Background(),
					f.request.Key,
				)
				if err != nil ||
					!bytes.Equal(encode(found), encode(op.Operation)) {
					t.Fatal("fault Find differs from durable receipt", err)
				}
				if _, err = current.db.Exec(
					`DROP TRIGGER ignore_finish`,
				); err != nil {
					t.Fatal(err)
				}
				f.restart(t)
				final := wait(t, f.e, f.request)
				after, restored, _, afterHits := finishIgnoredState(
					t,
					f.e,
					f.op.ID,
					f.op.VMID,
				)
				f.b.mu.Lock()
				physical := f.b.states[candidate.Instance]
				f.b.mu.Unlock()
				t.Logf(
					"reopened same key=%q candidate=%s launchCalls=%d baseline=%d physical=%+v",
					f.request.Key,
					encode(candidate),
					f.b.launchCalls.Load(),
					launches,
					physical,
				)
				if final.ID != f.op.ID ||
					final.Status != "succeeded" ||
					final.ResultInstance != candidate.Instance ||
					restored.VM.OperationID != "" ||
					!reflect.DeepEqual(restored.Resource, candidate) ||
					!reflect.DeepEqual(after.Journal, f.record.Journal) ||
					afterHits != 0 ||
					f.b.launchCalls.Load() != launches ||
					!physical.Alive ||
					!physical.Booted ||
					restored.VM.PID != physical.PID ||
					restored.VM.StartTime != physical.StartTime {
					t.Errorf(
						"same candidate did not converge: final=%+v VM=%+v hits=%d",
						final,
						restored,
						afterHits,
					)
				}
				row := assertFinishReceiptImmutable(t, f.e, final, f.request)
				// A new exact-B delete must be admissible after completion releases ownership.
				wait(
					t,
					f.e,
					api.Request{
						Key:              "delete-delivered-B",
						Action:           "delete",
						VMID:             f.op.VMID,
						ExpectedInstance: candidate.Instance,
					},
				)
				f.restart(t)
				if !bytes.Equal(
					row,
					assertFinishReceiptImmutable(t, f.e, final, f.request),
				) {
					t.Error(
						"terminal SQL body changed after deletion and reopen",
					)
				}
				if f.b.launchCalls.Load() != launches {
					t.Fatal("terminal reads or deletion relaunched a candidate")
				}
			})
		}
	}
}

type finishLeaseFailureBackend struct {
	*instanceRecoveryBackend

	failCheck atomic.Bool
}

func (b *finishLeaseFailureBackend) Check(
	ctx context.Context,
	c api.Config,
) error {
	if b.failCheck.Load() {
		return errors.New("restore pre-candidate backend check failed")
	}
	return b.instanceRecoveryBackend.Check(ctx, c)
}

func TestOperationResultInstanceIgnoredLeaseDelete(t *testing.T) {
	b := &finishLeaseFailureBackend{
		instanceRecoveryBackend: &instanceRecoveryBackend{
			testBackend: newTestBackend(),
		},
	}
	dir := t.TempDir()
	var armed atomic.Bool
	reached := make(chan struct{})
	var e *Engine
	e = openTest(t, dir, b, true, func(step string) error {
		if step != "finish_candidate_ready" ||
			!armed.CompareAndSwap(true, false) {
			return nil
		}
		defer close(reached)
		if _, err := e.db.Exec(
			`CREATE TABLE ignored_finish_writes(n INTEGER)`,
		); err != nil {
			return err
		}
		_, err := e.db.Exec(
			`CREATE TRIGGER ignore_finish BEFORE DELETE ON leases
			BEGIN INSERT INTO ignored_finish_writes VALUES(1); SELECT RAISE(IGNORE); END`,
		)
		return err
	})
	t.Cleanup(func() { _ = e.Close() })
	created := wait(
		t,
		e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	started := wait(
		t,
		e,
		api.Request{Key: "start", Action: "start", VMID: created.VMID},
	)
	snap := wait(
		t,
		e,
		api.Request{Key: "snapshot", Action: "snapshot", VMID: created.VMID},
	)
	wait(t, e, api.Request{Key: "stop", Action: "stop", VMID: created.VMID})
	launches := b.launchCalls.Load()
	b.failCheck.Store(true)
	armed.Store(true)
	req := api.Request{
		Key:              "restore-before-candidate",
		Action:           "restore",
		VMID:             created.VMID,
		SnapshotID:       snap.SnapshotID,
		ExpectedInstance: started.ResultInstance,
	}
	accepted, err := e.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	finishAttemptSettled(t, e, created.VMID, reached)
	op, vm, leases, hits := finishIgnoredState(t, e, accepted.ID, created.VMID)
	if terminal(op.Operation.Status) || op.Operation.ResultInstance != "" ||
		op.Journal.Resource != nil || op.Journal.RestoreBlocked || !op.Journal.Lease ||
		vm.VM.OperationID != accepted.ID || leases != 1 || hits != 0 {
		t.Errorf(
			"ignored lease deletion committed half completion: op=%+v VM=%+v leases=%d hits=%d",
			op,
			vm,
			leases,
			hits,
		)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	waited, waitErr := api.Wait(ctx, e, req)
	cancel()
	t.Logf(
		"fault Wait: request=%s operation=%s error=%v",
		encode(req),
		encode(waited),
		waitErr,
	)
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Errorf("fault published terminal failure: %+v %v", waited, waitErr)
	}
	if _, err = e.db.Exec(`DROP TRIGGER ignore_finish`); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	// The backend failure remains armed: restart must finish failure, not retry a successful restore.
	e = openTest(t, dir, b, false, nil)
	final := awaitStatus(t, e, accepted.ID)
	after, afterVM, afterLeases, afterHits := finishIgnoredState(
		t,
		e,
		accepted.ID,
		created.VMID,
	)
	if final.Status != "failed" ||
		final.ResultInstance != "" ||
		final.Error == nil ||
		final.Error.Message != "restore pre-candidate backend check failed" ||
		after.Journal.Resource != nil ||
		after.Journal.RestoreBlocked ||
		after.Journal.Lease ||
		afterVM.VM.OperationID != "" ||
		afterLeases != 0 ||
		afterHits != 0 ||
		b.launchCalls.Load() != launches {
		t.Errorf(
			"failed restore did not converge: final=%+v VM=%+v leases=%d hits=%d launches=%d baseline=%d",
			final,
			afterVM,
			afterLeases,
			afterHits,
			b.launchCalls.Load(),
			launches,
		)
	}
	row := assertFinishReceiptImmutable(t, e, final, req)
	wait(
		t,
		e,
		api.Request{
			Key:        "delete-snapshot",
			Action:     "delete_snapshot",
			SnapshotID: snap.SnapshotID,
		},
	)
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e = openTest(t, dir, b, false, nil)
	if !bytes.Equal(row, assertFinishReceiptImmutable(t, e, final, req)) {
		t.Error(
			"failed terminal SQL body changed after snapshot deletion and reopen",
		)
	}
	if b.launchCalls.Load() != launches {
		t.Fatal("pre-candidate failure launched a new instance")
	}
}

func finishAttemptSettled(
	t *testing.T,
	e *Engine,
	vmID string,
	reached <-chan struct{},
) {
	t.Helper()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("finish fault boundary not reached")
	}
	settled := make(chan struct{})
	go func() {
		lock := e.vmLock(vmID)
		lock.Lock()
		defer lock.Unlock()
		close(settled)
	}()
	select {
	case <-settled:
	case <-time.After(10 * time.Second):
		t.Fatal("finish did not release lifecycle lock")
	}
}

func finishIgnoredState(
	t *testing.T,
	e *Engine,
	opID, vmID string,
) (opRecord, vmRecord, int, int) {
	t.Helper()
	o, err := e.loadOp(context.Background(), opID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := e.loadVM(context.Background(), vmID)
	if err != nil {
		t.Fatal(err)
	}
	var status string
	var operationRow, vmRow []byte
	if err = e.db.QueryRow(`SELECT status,body FROM operations WHERE id=?`, opID).
		Scan(&status, &operationRow); err != nil {
		t.Fatal(err)
	}
	if err = e.db.QueryRow(`SELECT body FROM vms WHERE id=?`, vmID).
		Scan(&vmRow); err != nil {
		t.Fatal(err)
	}
	var leases, hits int
	if err = e.db.QueryRow(`SELECT count(*) FROM leases WHERE operation=?`, opID).
		Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if err = e.db.QueryRow(`SELECT count(*) FROM ignored_finish_writes`).
		Scan(&hits); err != nil {
		t.Fatal(err)
	}
	rows, err := e.db.Query(
		`SELECT snapshot,operation FROM leases WHERE operation=?`,
		opID,
	)
	if err != nil {
		t.Fatal(err)
	}
	var leaseRows [][2]string
	for rows.Next() {
		var lease [2]string
		if err = rows.Scan(&lease[0], &lease[1]); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		leaseRows = append(leaseRows, lease)
	}
	rowErr, closeErr := rows.Err(), rows.Close()
	if rowErr != nil || closeErr != nil {
		t.Fatal("read lease rows", rowErr, closeErr)
	}
	t.Logf(
		"durable SQL: status=%s operation=%s VM=%s leaseCount=%d leaseRows=%s committedTriggerHits=%d",
		status,
		operationRow,
		vmRow,
		leases,
		encode(leaseRows),
		hits,
	)
	if status != o.Operation.Status {
		t.Fatal("status column differs from operation body")
	}
	return o, v, leases, hits
}

func assertFinishReceiptImmutable(
	t *testing.T,
	e *Engine,
	final api.Operation,
	req api.Request,
) []byte {
	t.Helper()
	var row []byte
	if err := e.db.QueryRow(`SELECT body FROM operations WHERE id=?`, final.ID).
		Scan(&row); err != nil {
		t.Fatal(err)
	}
	t.Logf("terminal SQL body: key=%q body=%s", req.Key, row)
	want := encode(final)
	get, gErr := e.GetOperation(context.Background(), final.ID)
	find, fErr := e.FindOperation(context.Background(), req.Key)
	cached, cErr := e.Submit(context.Background(), req)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	waited, waitErr := api.Wait(ctx, e, req)
	if gErr != nil || fErr != nil || cErr != nil ||
		(final.Status == "succeeded" && waitErr != nil) || (final.Status == "failed" && waitErr == nil) ||
		!bytes.Equal(want, encode(get)) || !bytes.Equal(want, encode(find)) ||
		!bytes.Equal(
			want,
			encode(cached),
		) || !bytes.Equal(want, encode(waited)) {
		t.Fatal(
			"terminal Get/Find/cache/Wait changed receipt",
			gErr,
			fErr,
			cErr,
			waitErr,
		)
	}
	return row
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
