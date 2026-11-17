package machine

import (
	"context"

	"github.com/woxQAQ/haven/loom"
)

type ProvisionBackend interface {
	StopBackend

	Check(context.Context, loom.Config) error
	Prepare(context.Context, loom.Resource) error
	Launch(context.Context, loom.Resource, bool) error
	Boot(context.Context, loom.Resource, bool) error
	Agent(context.Context, loom.Resource, bool) error
}
