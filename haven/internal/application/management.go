package application

import (
	"context"

	"github.com/woxQAQ/haven/node"
	"github.com/woxQAQ/haven/types"
)

// InspectState validates offline metadata under the normal exclusive writer lock.
// It never constructs a runtime or dispatches retained workflows.
func InspectState(ctx context.Context, directory string) (types.NodeStatus, error) {
	state, err := openStore(directory, false)
	if err != nil {
		return types.NodeStatus{}, err
	}
	status, err := node.ReadNodeStatus(ctx, state)
	closeErr := state.Close()
	if err != nil {
		return status, err
	}
	return status, closeErr
}
