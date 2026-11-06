package engine

import (
	"fmt"
	"path/filepath"

	"github.com/woxQAQ/sandbox-engine/api"
)

func runtimeImage(r Resource) (ImageManifest, []ImageBinding, error) {
	var manifest ImageManifest
	if err := readJSON(filepath.Join(r.ImageDir, "image.json"), &manifest); err != nil {
		return manifest, nil, err
	}
	if manifest.Digest != r.Config.Image {
		return manifest, nil, api.Fail(api.Incompatible, "runtime image digest differs from VM configuration")
	}
	bindings, err := manifest.ReadOnlyBindings(r.ImageDir)
	return manifest, bindings, err
}

type cloudDisk struct {
	ID          string         `json:"id"`
	Path        string         `json:"path"`
	ReadOnly    bool           `json:"readonly"`
	Serial      string         `json:"serial"`
	ImageType   string         `json:"image_type"`
	NumQueues   int            `json:"num_queues"`
	RateLimiter map[string]any `json:"rate_limiter_config"`
}

func cloudDisks(bindings []ImageBinding) []cloudDisk {
	limiter := map[string]any{"bandwidth": map[string]any{"size": 64 << 20, "refill_time": 100}, "ops": map[string]any{"size": 1000, "refill_time": 100}}
	disks := make([]cloudDisk, 0, len(bindings)+1)
	for i, binding := range bindings {
		id := "root"
		if i > 0 {
			id = fmt.Sprintf("data-%04d", i-1)
		}
		disks = append(disks, cloudDisk{ID: id, Path: binding.GuestPath, ReadOnly: true, Serial: binding.Serial, ImageType: "Raw", NumQueues: 1, RateLimiter: limiter})
	}
	return append(disks, cloudDisk{ID: "upper", Path: "/disks/upper.raw", Serial: "sandbox-write", ImageType: "Raw", NumQueues: 1, RateLimiter: limiter})
}

func validateSnapshotDisks(saved, expected []cloudDisk) error {
	if len(saved) != len(expected) {
		return api.Fail(api.Incompatible, "snapshot disk count differs from image materialization")
	}
	for i, disk := range saved {
		want := expected[i]
		if disk.ID != want.ID || disk.Path != want.Path || disk.Serial != want.Serial || disk.ReadOnly != want.ReadOnly || disk.ImageType != want.ImageType || disk.NumQueues != want.NumQueues {
			return api.Fail(api.Incompatible, "snapshot disk %d differs from image materialization", i)
		}
	}
	return nil
}
