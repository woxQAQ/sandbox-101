package machine

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
)

func CheckGenerationCounts(ctx context.Context, tx *sql.Tx, source ResourceStep, generations []ResourceStep) error {
	var expectedBacking int64
	for _, generation := range generations {
		if generation.State.BackingReady {
			expectedBacking++
		}
	}

	count, err := RetainedInstanceCount(ctx, tx, source.Candidate)
	if err != nil || count != int64(len(generations)) {
		return fmt.Errorf("unrecorded VM generation or backing retained: %v", err)
	}
	count, err = RetainedVersionCount(ctx, tx, source.Candidate)
	if err != nil || count != int64(len(generations)) {
		return fmt.Errorf("unrecorded VM generation or backing retained: %v", err)
	}
	count, err = dbgen.New(tx).CountVMBackings(ctx, source.Candidate)

	if err != nil || count != expectedBacking {
		return fmt.Errorf("unrecorded VM generation or backing retained: %v", err)
	}
	return nil
}
