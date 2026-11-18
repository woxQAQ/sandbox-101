package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/woxQAQ/haven/gc"
	image "github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

// validateWorkflowProof decodes each frozen workflow using the same loaders as
// execution. Startup receives a read-only probe, never a partial Service.
func validateWorkflowProof(ctx context.Context, database store.DBTX, origin store.Origin, item operation.Step) error {
	var proofErr error
	switch {
	case item.Key == "resource":
		step, lookup := machine.LoadResourceStep(ctx, database, item.OperationID)
		proofErr = lookup
		if proofErr == nil && !origin.Reviewed {
			proofErr = machine.ValidateResourceStep(origin.Directory, step)
		}
	case item.Key == "capture":
		_, proofErr = machine.LoadCapture(ctx, database, item.OperationID)
	case item.Key == "lifecycle":
		_, proofErr = sandbox.LoadLifecycle(ctx, database, item.OperationID)
	case item.Key == "supply":
		_, proofErr = supply.LoadSupply(ctx, database, item.OperationID)
	case item.Key == "fork_group":
		_, proofErr = sandbox.LoadFork(ctx, database, item.OperationID)
	case item.Key == "warm_claim":
		_, proofErr = sandbox.LoadWarmClaim(ctx, database, item.OperationID)
	case item.Key == "supply_cleanup":
		_, proofErr = supply.LoadSupplyCleanup(ctx, database, item.OperationID)
	case item.Key == "image_import":
		proofErr = image.ValidateImageImport(ctx, database, item.OperationID)
	case item.Key == "image_cleanup":
		proofErr = image.ValidateImageCleanup(ctx, database, item.OperationID)
	case item.Key == "direct_retirement":
		_, proofErr = machine.LoadDirectRetirement(ctx, database, item.OperationID)
	case item.Key == "gc":
		_, proofErr = gc.Load(ctx, database, item.OperationID)
	case item.Key == "stop":
		_, proofErr = machine.LoadStop(ctx, database, item.OperationID)
	case item.Key == "discard":
		_, proofErr = machine.LoadDiscard(ctx, database, item.OperationID)
	case item.Key == "capture_discard":
		proofErr = machine.ValidateCaptureDiscard(ctx, database, item.OperationID)
	case item.Key == "capture_cleanup":
		proofErr = machine.ValidateCaptureCleanup(ctx, database, item.OperationID)
	case item.Key == "snapshot_delete":
		proofErr = machine.ValidateSnapshotDelete(ctx, database, item.OperationID)
	case item.Action == "activate":
		var input sandbox.ActivationInput
		proofErr = json.Unmarshal(item.Input, &input)
		if proofErr == nil && (item.Key != sandbox.ActivationKey(input.Attempt) || input.ResourceHash == "" || input.ExpectedInstance == "") {
			proofErr = fmt.Errorf("activation original resource identity incomplete")
		}
	case item.Action == "resource_link":
		if !strings.HasPrefix(item.Key, "link:") {
			proofErr = fmt.Errorf("resource link key invalid")
		} else {
			_, _, proofErr = machine.LoadResourceLink(ctx, database, item.OperationID, strings.TrimPrefix(item.Key, "link:"))
		}
	case item.Key == "configuration" && item.Action == "configure_paused", item.Key == "lease" && item.Action == "renew", item.Key == "checkpoint_delete" && item.Action == operation.ActionCheckpointDelete:
	default:
		proofErr = fmt.Errorf("unsupported control step kind")
	}
	return proofErr
}
