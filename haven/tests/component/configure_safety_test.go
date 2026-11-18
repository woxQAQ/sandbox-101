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
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenConfigureFailureMatrix(t *testing.T) {
	fixtures.ParallelLifecycle(t)
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
			service, runtime, guest, manager := testapp.SandboxFixture(t)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			wide := egress.Policy{
				Mode: "proxy",
				AllowedHosts: []string{
					"old.invalid",
				},
			}
			claim := (fixtures.SandboxClaims{
				App: service,
			}).Claim(t, "claim", map[string]operation.EnvVar{
				"REVISION": {
					Value: "old",
				},
			}, wide)
			before, err := service.Catalog.Get(context.Background(), principal, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			connection, err := service.Dataplane.Connect(context.Background(), principal, before.ID)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = connection.Close()
			}()
			fixtures.WaitFixtureWorker(t, service, claim.ID)
			var statement string
			switch stage {
			case "inspect":
				runtime.Mu.Lock()
				runtime.InspectError = fmt.Errorf("private runtime query failure")
				runtime.Mu.Unlock()
			case "network_off":
				runtime.Before = func(name string, _ loom.Resource) error {
					if name == "network" {
						return fmt.Errorf("private network failure")
					}
					return nil
				}
			case "info", "activate":
				guest.Before = func(name string, _ loom.Resource) error {
					if name == stage {
						return fmt.Errorf("private guest failure")
					}
					return nil
				}
			case "bind":
				manager.Before = func(name string) error {
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
				if _, err = service.State.Database().Exec(statement); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			command := operation.Command{
				Action:    "configure",
				SandboxID: before.ID,
				Env: map[string]operation.EnvVar{
					"REVISION": {
						Value: "new",
					},
				},
				Policy: egress.Policy{
					Mode: "proxy",
					AllowedHosts: []string{
						"new.invalid",
					},
				},
			}
			failed, err := service.SandboxRequests.Execute(ctx, principal, "configure", command)
			if err == nil || failed.Status != "failed" {
				t.Fatal("failure changed accepted goal", failed, err)
			}
			fixtures.WaitFixtureWorker(t, service, failed.ID)
			var fence string
			if err = service.State.Database().QueryRow("SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", before.ID).Scan(&fence); err != nil || fence != failed.ID {
				t.Fatal("failure lost durable fence", fence, failed, err)
			}
			after, err := service.Catalog.Get(ctx, principal, before.ID)
			if err != nil || after.LastError == "" || after.Instance != before.Instance || !after.Deadline.Equal(before.Deadline) {
				t.Fatal("failure changed identity/lease or lost diagnostic", before, after, err)
			}
			if stage == "save_intent" && (!reflect.DeepEqual(after.Env, before.Env) || !reflect.DeepEqual(after.Policy, before.Policy)) {
				t.Fatal("refused desired settings replaced old intent", before, after)
			}
			manager.Mu.Lock()
			_, bound := manager.Binds[before.ID]
			manager.Mu.Unlock()
			if bound {
				t.Fatal("failed configure retained a proxy binding")
			}
			if err = connection.SetReadDeadline(time.Now().Add(time.Second)); err == nil {
				var buffer [1]byte
				if _, err = connection.Read(buffer[:]); err == nil {
					t.Fatal("failed configure retained old data connection")
				}
			}
			if _, err = service.Dataplane.Ready(ctx, principal, before.ID); err == nil {
				t.Fatal("failed configure admitted data")
			}
			runtime.Mu.Lock()
			runtime.InspectError = nil
			runtime.Mu.Unlock()
			runtime.Before = nil
			guest.Before = nil
			manager.Before = nil
			if statement != "" {
				if _, err = service.State.Database().Exec("DROP TRIGGER deny_config"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = service.SandboxRequests.Execute(ctx, principal, "repair", command); err != nil {
				t.Fatal("fresh explicit configure did not repair fence", err)
			}
			original, err := service.Operations.GetOperation(ctx, "tenant", failed.ID)
			if err != nil || !reflect.DeepEqual(original, failed) {
				t.Fatal("repair rewrote original terminal", original, failed, err)
			}
			if _, err = service.SandboxRequests.Execute(ctx, principal, "configure", command); err == nil {
				t.Fatal("failed idempotent configure became success")
			}
		})
	}
}
