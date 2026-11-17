package dataplane

import (
	"context"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/store"
)

// ProxySockets verifies the live sandbox authority behind egress socket handoffs.
// It borrows metadata and inspection; inode mutation stays on the anchored socket.
type ProxySockets struct {
	ctx     context.Context
	catalog *sandbox.Catalog
	backend machine.InstanceInspector
}

func NewProxySockets(state *store.State, ctx context.Context, backend machine.InstanceInspector) *ProxySockets {
	return &ProxySockets{
		ctx:     ctx,
		catalog: sandbox.NewCatalog(state.Database()),
		backend: backend,
	}
}
