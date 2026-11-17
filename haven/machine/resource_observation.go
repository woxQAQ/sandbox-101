package machine

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/store"
)

type InstanceInspector interface {
	Inspect(context.Context, loom.Resource) (loom.Observation, error)
}

func InspectStoppedGenerations(ctx context.Context, inspector InstanceInspector, generations []ResourceStep) error {
	for _, generation := range generations {
		observation, err := inspector.Inspect(ctx, generation.Input.Resource)
		if err != nil {
			return err
		}
		if observation.Alive || observation.Network {
			return fmt.Errorf("%w: known instance host removal unconfirmed", errs.ErrUnknown)
		}
	}
	return nil
}

func LoadVM(ctx context.Context, database store.DBTX, step ResourceStep) (VMRecord, string, string, error) {
	current, err := LoadOwnership(ctx, database, step.Candidate)
	var record VMRecord
	if err != nil {
		return record, "", "", err
	}
	if err = json.Unmarshal(current.Body, &record); err != nil {
		return record, "", "", err
	}
	if !reflect.DeepEqual(record.Resource, step.Input.Resource) {
		return record, "", "", fmt.Errorf("VM candidate resource changed")
	}
	return record, current.Instance, current.Owner, nil
}
