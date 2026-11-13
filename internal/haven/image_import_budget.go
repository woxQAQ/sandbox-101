package haven

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

func importRemainingCharge(ctx context.Context, q rowsQueryer, source imageImportStep) (int64, error) {
	rows, err := q.QueryContext(ctx, "SELECT body FROM image_caches WHERE json_extract(body,'$.source_operation')=?", source.OperationID)
	if err != nil {
		return 0, err
	}
	var funded []imageCacheRecord
	for rows.Next() {
		var body []byte
		var record imageCacheRecord
		if err = rows.Scan(&body); err != nil {
			break
		}
		if err = json.Unmarshal(body, &record); err != nil {
			break
		}
		funded = append(funded, record)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
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
		var count int
		if err = q.QueryRowContext(ctx, "SELECT COUNT(*) FROM reservations WHERE owner=? AND bytes=? AND memory=0 AND cpus=0", "cache:"+record.Witness.Cache.Builder+":"+record.Witness.Cache.DiffID, record.Witness.PhysicalBytes).Scan(&count); err != nil {
			return 0, err
		}
		if count != 1 {
			return 0, fmt.Errorf("cache independent physical charge missing")
		}
	}
	return remaining, nil
}
