package image

import (
	"context"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func importRemainingCharge(ctx context.Context, q store.DBTX, source imageImportStep) (int64, error) {
	funded, err := fundedCaches(ctx, q, source.OperationID)
	if err != nil {
		return 0, err
	}
	remaining := source.Input.Reservation
	for _, record := range funded {
		found := false
		for _, layer := range source.State.Layers {
			if layer.Cache != nil && reflect.DeepEqual(*layer.Cache, record.Witness) {
				found = true
				break
			}
		}
		if !found || record.Witness.PhysicalBytes <= 0 || record.Witness.PhysicalBytes > remaining {
			return 0, fmt.Errorf("import cache funding lacks original returned witness")
		}
		remaining -= record.Witness.PhysicalBytes
	}
	for _, layer := range source.State.Layers {
		if layer.Cache == nil {
			continue
		}
		record, err := loadImageCache(ctx, q, layer.Cache.Cache.Builder, layer.Cache.Cache.DiffID)
		if err != nil {
			return 0, err
		}
		if !reflect.DeepEqual(record.Witness, *layer.Cache) {
			return 0, fmt.Errorf("import cache historical ownership changed")
		}
		count, err := quota.New(q).CountMatchingReservation(ctx, quota.Reservation{
			Owner: "cache:" + record.Witness.Cache.Builder + ":" + record.Witness.Cache.DiffID,
			Bytes: record.Witness.PhysicalBytes,
		})
		if err != nil {
			return 0, err
		}
		if count != 1 {
			return 0, fmt.Errorf("cache independent physical charge missing")
		}
	}
	return remaining, nil
}
