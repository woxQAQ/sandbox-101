package supply

import (
	"context"
	"encoding/json"
	"time"

	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

func RefillBackoff(ctx context.Context, database store.DBTX, template string, now time.Time) (bool, error) {
	attempts, err := dbgen.New(database).ListRefillAttempts(ctx, template)
	if err != nil {
		return true, err
	}
	failures := 0
	var latest time.Time
	for _, attempt := range attempts {
		if attempt.Status == "succeeded" {
			break
		}
		var intent SupplyInput
		var state SupplyState
		if err = json.Unmarshal(attempt.Input, &intent); err != nil {
			return true, err
		}
		if err = json.Unmarshal(attempt.Body, &state); err != nil {
			return true, err
		}
		if failures == 0 {
			latest = state.FinishedAt
			if latest.IsZero() {
				latest = intent.CreatedAt
			}
		}
		failures++
	}
	if failures == 0 {
		return false, nil
	}
	delay := time.Second << min(failures-1, 8)
	return now.Before(latest.Add(delay)), nil
}
