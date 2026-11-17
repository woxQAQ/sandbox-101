package image_test

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type imageFixture struct {
	State      *store.State
	Images     *image.Images
	Operations *operation.Operations
	Workers    *execution.Workers
}

func newImageFixture(t *testing.T) *imageFixture {
	t.Helper()
	state := testfixture.OpenState(t)
	lifetime := execution.NewLifetime(t.Context())
	guards := execution.NewGuards()
	workers := execution.NewWorkers(lifetime)
	gate := execution.NewAdmissionGate(guards.Registration(), lifetime, state.Reviewed())
	fixture := &imageFixture{
		State:      state,
		Images:     image.NewImages(state, guards, gate, workers, image.ImageOptions{}),
		Operations: operation.NewOperations(state.Database(), workers, state.Identity().Identity),
		Workers:    workers,
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := lifetime.Close(ctx, func() {}, state.Close); err != nil {
			t.Error(err)
		}
	})
	return fixture
}
