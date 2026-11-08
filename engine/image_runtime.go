package engine

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/woxQAQ/sandbox-engine/api"
)

func runtimeImage(r Resource) (ImageManifest, []ImageBinding, error) {
	var manifest ImageManifest
	if err := readJSON(
		filepath.Join(r.ImageDir, "image.json"),
		&manifest,
	); err != nil {
		return manifest, nil, err
	}
	if manifest.Digest != r.Config.Image {
		return manifest, nil, api.Fail(
			api.Incompatible,
			"runtime image digest differs from VM configuration",
		)
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
	limiter := map[string]any{
		"bandwidth": map[string]any{"size": 64 << 20, "refill_time": 100},
		"ops":       map[string]any{"size": 1000, "refill_time": 100},
	}
	disks := make([]cloudDisk, 0, len(bindings)+1)
	for i, binding := range bindings {
		id := "root"
		if i > 0 {
			id = fmt.Sprintf("data-%04d", i-1)
		}
		disks = append(
			disks,
			cloudDisk{
				ID:          id,
				Path:        binding.GuestPath,
				ReadOnly:    true,
				Serial:      binding.Serial,
				ImageType:   "Raw",
				NumQueues:   1,
				RateLimiter: limiter,
			},
		)
	}
	return append(
		disks,
		cloudDisk{
			ID:          "upper",
			Path:        "/disks/upper.raw",
			Serial:      "sandbox-write",
			ImageType:   "Raw",
			NumQueues:   1,
			RateLimiter: limiter,
		},
	)
}

func validateSnapshotDisks(saved, expected []cloudDisk) error {
	if len(saved) != len(expected) {
		return api.Fail(
			api.Incompatible,
			"snapshot disk count differs from image materialization",
		)
	}
	for i, disk := range saved {
		want := expected[i]
		if disk.ID != want.ID || disk.Path != want.Path ||
			disk.Serial != want.Serial ||
			disk.ReadOnly != want.ReadOnly ||
			disk.ImageType != want.ImageType ||
			disk.NumQueues != want.NumQueues {
			return api.Fail(
				api.Incompatible,
				"snapshot disk %d differs from image materialization",
				i,
			)
		}
	}
	return nil
}

// ImageBinding describes a verified host file to bind read-only into the VMM
// namespace and attach as a readonly raw block device. Order is significant.
type ImageBinding struct {
	HostPath  string
	GuestPath string
	Serial    string
	SHA256    string
	Role      string
	DiffID    string
	DeviceID  int
}

func (m ImageManifest) ReadOnlyBindings(
	imageDir string,
) ([]ImageBinding, error) {
	if err := m.Verify(imageDir, m.Digest); err != nil {
		return nil, err
	}
	if m.Layout != LayeredEROFSLayout {
		return []ImageBinding{
			{
				HostPath:  filepath.Join(imageDir, "root.erofs"),
				GuestPath: "/images/root.erofs",
				Serial:    "sandbox-root",
				SHA256:    m.Files["root.erofs"],
				Role:      "merged",
			},
		}, nil
	}
	state := filepath.Dir(filepath.Dir(imageDir))
	bindings := make([]ImageBinding, 0, len(m.Artifacts))
	for _, a := range m.Artifacts {
		bindings = append(
			bindings,
			ImageBinding{
				HostPath:  filepath.Join(state, a.Path),
				GuestPath: a.GuestPath,
				Serial:    a.Serial,
				SHA256:    a.SHA256,
				Role:      a.Role,
				DiffID:    a.DiffID,
				DeviceID:  a.DeviceID,
			},
		)
	}
	return bindings, nil
}

// DataSerials is the ordered external-device list for initramfs device= options.
// It validates the manifest structure; ReadOnlyBindings verifies the files too.
func (m ImageManifest) DataSerials() (string, error) {
	if m.Layout == "merged-erofs-v1" && m.Format == 1 &&
		len(m.Artifacts) == 0 &&
		m.Builder == "" {
		return "", nil
	}
	if m.Layout == MergedOCIEROFSLayout {
		return "", m.validateMergedOCI()
	}
	if err := m.validateLayered(); err != nil {
		return "", err
	}
	serials := make([]string, 0, len(m.DiffIDs))
	for _, a := range m.Artifacts[1:] {
		serials = append(serials, a.Serial)
	}
	return strings.Join(serials, ","), nil
}

func (m ImageManifest) DataKernelParameter() (string, error) {
	s, err := m.DataSerials()
	if err != nil || s == "" {
		return "", err
	}
	return "sandbox.data_serials=" + s, nil
}
