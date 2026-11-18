package fixtures

import (
	"github.com/woxQAQ/haven/internal/testfixture"
)

type BlockingCloseRuntime struct {
	*testfixture.ColdRuntime

	Entered chan struct{}
	Release <-chan struct{}
}

func (backend *BlockingCloseRuntime) Close() error {
	close(backend.Entered)
	<-backend.Release
	return nil
}
