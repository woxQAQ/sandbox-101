package host

import (
	"context"

	"github.com/woxQAQ/haven/loom"
)

func systemAgent(ctx context.Context, socket, instance string, restore bool) error {
	return loom.SystemAgent(ctx, socket, instance, restore)
}
