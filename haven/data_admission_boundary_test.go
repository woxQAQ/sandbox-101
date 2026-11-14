package haven

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/loom"
)

func TestHavenDataAdmissionRepeatsFinalGuard(t *testing.T) {
	for _, change := range []string{
		"configuration",
		"configuration_provenance",
		"unverified_instance",
		"deadline",
		"shutdown",
	} {
		t.Run(change, func(t *testing.T) {
			service, _, guest, _ := sandboxFixture(t)
			principal := Principal{
				TenantID: "tenant",
			}
			claim := claimSandbox(t, service, "claim", nil, egress.Policy{
				Mode: "none",
			})
			waitFixtureWorker(t, service, claim.ID)
			entered, release := make(chan struct{}), make(chan struct{})
			guest.before = func(name string, _ loom.Resource) error {
				if name == "connect" {
					close(entered)
					<-release
				}
				return nil
			}
			type result struct {
				connection net.Conn
				err        error
			}
			done := make(chan result, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			go func() {
				connection, err := service.Connect(ctx, principal, claim.Result.SandboxID)
				done <- result{connection, err}
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			sandbox, err := service.Get(ctx, principal, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "configuration":
				if _, err = service.Execute(ctx, principal, "configure", Command{
					Action:    "configure",
					SandboxID: sandbox.ID,
					Env: map[string]EnvVar{
						"REVISION": {
							Value: "new",
						},
					},
					Policy: egress.Policy{
						Mode: "none",
					},
				}); err != nil {
					t.Fatal(err)
				}
			case "configuration_provenance":
				settings, err := loadSettings(ctx, service.store.db, sandbox.ID)
				if err != nil {
					t.Fatal(err)
				}
				settings.Env["UNPUBLISHED"] = "private-fixture"
				if _, err = service.store.db.Exec("UPDATE sandbox_settings SET body=? WHERE sandbox_id=?", encode(settings), sandbox.ID); err != nil {
					t.Fatal(err)
				}
			case "unverified_instance":
				source, err := service.dataResource(ctx, sandbox)
				if err != nil {
					t.Fatal(err)
				}
				vm, _, _, err := service.loadVM(ctx, service.store.db, source)
				if err != nil {
					t.Fatal(err)
				}
				vm.Verified = false
				if _, err = service.store.db.Exec("UPDATE vms SET body=? WHERE id=?", encode(vm), sandbox.VMID); err != nil {
					t.Fatal(err)
				}
			case "deadline":
				settings, err := loadSettings(ctx, service.store.db, sandbox.ID)
				if err != nil {
					t.Fatal(err)
				}
				settings.Deadline = time.Now().Add(-time.Second)
				if _, err = service.store.db.Exec("UPDATE sandbox_settings SET body=? WHERE sandbox_id=?", encode(settings), sandbox.ID); err != nil {
					t.Fatal(err)
				}
			case "shutdown":
				shutdownCtx, stop := context.WithTimeout(ctx, 5*time.Millisecond)
				defer stop()
				_ = service.Close(shutdownCtx)
				if unexpected, err := openStore(service.store.directory, false); !errors.Is(err, errStoreConflict) {
					if unexpected != nil {
						_ = unexpected.close()
					}
					t.Fatal("shutdown did not retain writer during pending data call", err)
				}
			}
			close(release)
			select {
			case result := <-done:
				if result.connection != nil {
					_ = result.connection.Close()
					t.Fatal("late connection escaped final admission guard")
				}
				if result.err == nil {
					t.Fatal("late connection lacked rejection")
				}
			case <-ctx.Done():
				t.Fatal(fmt.Errorf("data admission did not finish: %w", ctx.Err()))
			}
		})
	}
}
