package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// validateWorkflows interprets persisted step identities on the read-only probe.
// No runtime effects or mutable ownership discovery belong to this boundary.
func (state *store) validateWorkflows(database *sql.DB) error {
	ctx := context.Background()
	rows, err := database.QueryContext(ctx, "SELECT operation_id,step_key,action,input_hash,input,body FROM operation_steps ORDER BY rowid")
	if err != nil {
		return err
	}
	type entry struct {
		operation, key, action, hash string
		input, body                  []byte
	}
	var entries []entry
	for rows.Next() {
		var item entry
		if err = rows.Scan(&item.operation, &item.key, &item.action, &item.hash, &item.input, &item.body); err != nil {
			break
		}
		entries = append(entries, item)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return err
	}
	service := &Service{store: state}
	for _, item := range entries {
		if digest(item.input) != item.hash || !json.Valid(item.input) || !json.Valid(item.body) {
			return fmt.Errorf("invalid durable step encoding: %s/%s", item.operation, item.key)
		}
		var proofErr error
		switch {
		case item.key == "resource":
			step, lookup := loadStep(ctx, database, item.operation)
			proofErr = lookup
			if proofErr == nil && !state.reviewed {
				proofErr = service.validateStep(step)
			}
		case item.key == "capture":
			_, proofErr = loadCapture(ctx, database, item.operation)
		case item.key == "lifecycle":
			_, proofErr = loadLifecycle(ctx, database, item.operation)
		case item.key == "supply":
			_, proofErr = loadSupply(ctx, database, item.operation)
		case item.key == "fork_group":
			_, proofErr = loadFork(ctx, database, item.operation)
		case item.key == "warm_claim":
			_, proofErr = loadWarmClaim(ctx, database, item.operation)
		case item.key == "supply_cleanup":
			_, proofErr = loadSupplyCleanup(ctx, database, item.operation)
		case item.key == "image_import":
			_, proofErr = loadImageImport(ctx, database, item.operation)
		case item.key == "image_cleanup":
			_, proofErr = loadImageCleanup(ctx, database, item.operation)
		case item.key == "direct_retirement":
			_, proofErr = loadDirectRetirement(ctx, database, item.operation)
		case item.key == "gc":
			_, proofErr = loadGC(ctx, database, item.operation)
		case item.key == "stop":
			_, proofErr = loadStop(ctx, database, item.operation)
		case item.key == "discard":
			_, proofErr = loadDiscard(ctx, database, item.operation)
		case item.key == "capture_discard":
			_, proofErr = loadCaptureDiscard(ctx, database, item.operation)
		case item.key == "capture_cleanup":
			_, proofErr = loadCaptureCleanup(ctx, database, item.operation)
		case item.key == "snapshot_delete":
			_, proofErr = loadSnapshotDelete(ctx, database, item.operation)
		case item.action == "activate":
			var input activationInput
			proofErr = json.Unmarshal(item.input, &input)
			if proofErr == nil && (item.key != activationKey(input.Attempt) || input.ResourceHash == "" || input.ExpectedInstance == "") {
				proofErr = fmt.Errorf("activation original resource identity incomplete")
			}
		case item.action == "resource_link":
			if !strings.HasPrefix(item.key, "link:") {
				proofErr = fmt.Errorf("resource link key invalid")
			} else {
				_, _, proofErr = loadResourceLink(ctx, database, item.operation, strings.TrimPrefix(item.key, "link:"))
			}
		case item.key == "configuration" && item.action == "configure_paused", item.key == "lease" && item.action == "renew", item.key == "checkpoint_delete" && item.action == "checkpoint_delete":
		default:
			proofErr = fmt.Errorf("unsupported control step kind")
		}
		if proofErr != nil {
			return fmt.Errorf("durable step %s/%s requires recovery: %w", item.operation, item.key, proofErr)
		}
	}
	return nil
}
