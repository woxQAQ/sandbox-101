package engine

import (
	"encoding/json"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (e *Engine) checkImagePublication(
	r Resource,
	captured json.RawMessage,
) error {
	requires, ok := e.opts.Backend.(interface{ RequiresImageCatalog() bool })
	if !ok || !requires.RequiresImageCatalog() {
		return nil
	}
	var body string
	if err := e.db.QueryRow("SELECT value FROM meta WHERE key=?", "image:"+r.Config.Image).
		Scan(&body); err != nil {
		return api.Fail(
			api.Incompatible,
			"image has no published catalog record: %v",
			err,
		)
	}
	var published, disk ImageManifest
	if err := json.Unmarshal([]byte(body), &published); err != nil {
		return err
	}
	if err := readJSON(
		filepath.Join(r.ImageDir, "image.json"),
		&disk,
	); err != nil {
		return err
	}
	if !reflect.DeepEqual(published, disk) {
		return api.Fail(
			api.Incompatible,
			"image materialization differs from published catalog",
		)
	}
	if captured != nil {
		var saved ImageManifest
		if err := json.Unmarshal(captured, &saved); err != nil {
			return err
		}
		if !reflect.DeepEqual(published, saved) {
			return api.Fail(
				api.Incompatible,
				"snapshot image materialization differs from published catalog",
			)
		}
	}
	return published.Verify(r.ImageDir, r.Config.Image)
}
