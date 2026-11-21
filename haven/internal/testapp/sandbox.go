package testapp

import (
	"testing"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
)

func SandboxFixture(t *testing.T) (*application.App, *testfixture.ColdRuntime, *testfixture.WorkloadFixture, *testfixture.EgressFixture) {
	t.Helper()
	return SandboxFixtureWithFault(t, nil)
}

func SandboxFixtureWithFault(t *testing.T, fault func(string) error) (*application.App, *testfixture.ColdRuntime, *testfixture.WorkloadFixture, *testfixture.EgressFixture) {
	t.Helper()
	return SandboxFixtureWithBackend(t, fault, nil)
}

func SandboxFixtureWithBackend(t *testing.T, fault func(string) error, factory func(*testfixture.ColdRuntime) loom.Backend) (*application.App, *testfixture.ColdRuntime, *testfixture.WorkloadFixture, *testfixture.EgressFixture) {
	t.Helper()
	guest := &testfixture.WorkloadFixture{}
	proxy := &testfixture.EgressFixture{
		Binds: map[string]map[string]egress.Secret{},
	}
	service, runtime := ColdServiceWithOptions(t, application.Options{
		Fault:      fault,
		Guest:      guest,
		Egress:     proxy,
		AdminToken: "fixture-root-token",
	}, factory)
	t.Cleanup(guest.Close)
	return service, runtime, guest, proxy
}
