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

func waitFixtureWorker(t *testing.T, service *Service, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		service.mu.Lock()
		active := service.active[id]
		service.mu.Unlock()
		if !active {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("private workflow worker still active")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHavenConfigureFailureMatrix(t *testing.T) {
	for _, stage := range []string{
		"inspect",
		"network_off",
		"info",
		"activate",
		"bind",
		"save_intent",
		"running_save",
		"final_settings",
		"fence_delete",
		"fence_ignored",
		"operation_result",
		"commit",
	} {
		t.Run(stage, func(t *testing.T) {
			service, runtime, guest, manager := sandboxFixture(t)
			principal := Principal{
				TenantID: "tenant",
			}
			wide := egress.Policy{
				Mode: "relay",
				AllowedHosts: []string{
					"old.invalid",
				},
			}
			claim := claimSandbox(t, service, "claim", map[string]EnvVar{
				"REVISION": {
					Value: "old",
				},
			}, wide)
			before, err := service.Get(context.Background(), principal, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			connection, err := service.Connect(context.Background(), principal, before.ID)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = connection.Close()
			}()
			waitFixtureWorker(t, service, claim.ID)
			var statement string
			switch stage {
			case "inspect":
				runtime.mu.Lock()
				runtime.inspectError = fmt.Errorf("private runtime query failure")
				runtime.mu.Unlock()
			case "network_off":
				runtime.before = func(name string, _ loom.Resource) error {
					if name == "network" {
						return fmt.Errorf("private network failure")
					}
					return nil
				}
			case "info", "activate":
				guest.before = func(name string, _ loom.Resource) error {
					if name == stage {
						return fmt.Errorf("private guest failure")
					}
					return nil
				}
			case "bind":
				manager.before = func(name string) error {
					if name == "bind" {
						return fmt.Errorf("private bind failure")
					}
					return nil
				}
			case "save_intent":
				statement = `CREATE TRIGGER deny_config BEFORE UPDATE ON sandbox_settings WHEN json_extract(NEW.body,'$.env.REVISION')='new' BEGIN SELECT RAISE(ABORT,'private settings refusal'); END`
			case "running_save":
				statement = `CREATE TRIGGER deny_config BEFORE UPDATE ON sandboxes WHEN json_extract(NEW.body,'$.last_error') IS NULL BEGIN SELECT RAISE(ABORT,'private running refusal'); END`
			case "final_settings":
				statement = `CREATE TRIGGER deny_config BEFORE UPDATE ON sandbox_settings WHEN json_extract(NEW.body,'$.active_epoch') IS NOT NULL AND json_extract(NEW.body,'$.env.REVISION')='new' BEGIN SELECT RAISE(ABORT,'private final settings refusal'); END`
			case "fence_delete":
				statement = `CREATE TRIGGER deny_config BEFORE DELETE ON configuration_fences BEGIN SELECT RAISE(ABORT,'private fence refusal'); END`
			case "fence_ignored":
				statement = `CREATE TRIGGER deny_config BEFORE DELETE ON configuration_fences BEGIN SELECT RAISE(IGNORE); END`
			case "operation_result":
				statement = `CREATE TRIGGER deny_config BEFORE UPDATE ON operations WHEN NEW.status='succeeded' BEGIN SELECT RAISE(ABORT,'private result refusal'); END`
			case "commit":
				statement = `CREATE TABLE fault_parent(id INTEGER PRIMARY KEY); CREATE TABLE fault_child(id INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER deny_config AFTER DELETE ON configuration_fences BEGIN INSERT INTO fault_child VALUES(1); END`
			}
			if statement != "" {
				if _, err = service.store.db.Exec(statement); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			command := Command{
				Action:    "configure",
				SandboxID: before.ID,
				Env: map[string]EnvVar{
					"REVISION": {
						Value: "new",
					},
				},
				Policy: egress.Policy{
					Mode: "relay",
					AllowedHosts: []string{
						"new.invalid",
					},
				},
			}
			failed, err := service.Execute(ctx, principal, "configure", command)
			if err == nil || failed.Status != "failed" {
				t.Fatal("failure changed accepted goal", failed, err)
			}
			waitFixtureWorker(t, service, failed.ID)
			var fence string
			if err = service.store.db.QueryRow("SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", before.ID).Scan(&fence); err != nil || fence != failed.ID {
				t.Fatal("failure lost durable fence", fence, failed, err)
			}
			after, err := service.Get(ctx, principal, before.ID)
			if err != nil || after.LastError == "" || after.Instance != before.Instance || !after.Deadline.Equal(before.Deadline) {
				t.Fatal("failure changed identity/lease or lost diagnostic", before, after, err)
			}
			if stage == "save_intent" && (!reflect.DeepEqual(after.Env, before.Env) || !reflect.DeepEqual(after.Policy, before.Policy)) {
				t.Fatal("refused desired settings replaced old intent", before, after)
			}
			manager.mu.Lock()
			_, bound := manager.binds[before.ID]
			manager.mu.Unlock()
			if bound {
				t.Fatal("failed configure retained a proxy binding")
			}
			if err = connection.SetReadDeadline(time.Now().Add(time.Second)); err == nil {
				var buffer [1]byte
				if _, err = connection.Read(buffer[:]); err == nil {
					t.Fatal("failed configure retained old data connection")
				}
			}
			if _, err = service.DataReady(ctx, principal, before.ID); err == nil {
				t.Fatal("failed configure admitted data")
			}
			runtime.mu.Lock()
			runtime.inspectError = nil
			runtime.mu.Unlock()
			runtime.before = nil
			guest.before = nil
			manager.before = nil
			if statement != "" {
				if _, err = service.store.db.Exec("DROP TRIGGER deny_config"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = service.Execute(ctx, principal, "repair", command); err != nil {
				t.Fatal("fresh explicit configure did not repair fence", err)
			}
			original, err := service.GetOperation(ctx, "tenant", failed.ID)
			if err != nil || !reflect.DeepEqual(original, failed) {
				t.Fatal("repair rewrote original terminal", original, failed, err)
			}
			if _, err = service.Execute(ctx, principal, "configure", command); err == nil {
				t.Fatal("failed idempotent configure became success")
			}
		})
	}
}
