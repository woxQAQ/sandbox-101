package haven

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
)

func TestHavenStartupRejectsMalformedWarmStepBeforeEffects(t *testing.T) {
	service, runtime, _ := goldenSupplyFixture(t, 1)
	principal := Principal{TenantID: "tenant"}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := service.Execute(ctx, principal, "refill", Command{Action: "refill", TemplateID: "golden"}); err != nil {
		t.Fatal(err)
	}
	service.opts.Fault = func(point string) error {
		if point == "activation_off_intent" {
			return ErrInterrupted
		}
		return nil
	}
	claim, err := service.AdmitClaim(ctx, principal, "warm", Command{Action: "claim", TemplateID: "golden", Policy: egress.Policy{Mode: "none"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Wait(ctx, principal, claim.ID); !errors.Is(err, ErrInterrupted) {
		t.Fatal(err)
	}
	waitFixtureWorker(t, service, claim.ID)
	if _, err = service.store.db.Exec("UPDATE operation_steps SET body=? WHERE operation_id=? AND step_key='warm_claim'", encode(warmClaimState{Phase: "unsupported-stage"}), claim.ID); err != nil {
		t.Fatal(err)
	}
	options := service.opts
	options.Initialize = false
	options.Fault = nil
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	before := storeSnapshot(t, options.StateDir)
	launches := runtime.count("launch")
	opened, err := Open(options)
	if opened != nil {
		_ = opened.Close(ctx)
	}
	if !errors.Is(err, errStoreRecovery) {
		t.Fatal("malformed durable step passed startup", err)
	}
	if after := storeSnapshot(t, options.StateDir); !reflect.DeepEqual(before, after) {
		for path, value := range before {
			if !reflect.DeepEqual(after[path], value) {
				t.Log("changed", path, value, after[path])
			}
		}
		for path := range after {
			if _, ok := before[path]; !ok {
				t.Log("created", path)
			}
		}
		t.Fatal("rejected startup changed private durable state")
	}
	if runtime.count("launch") != launches {
		t.Fatal("rejected step started runtime effects")
	}
}
