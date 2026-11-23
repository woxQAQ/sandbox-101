// Package host implements explicit Linux, Cloud Hypervisor and image-file
// effects. It does not own a catalog, durable operations or recovery workers.
package host

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/woxQAQ/haven/loom"
)

type (
	Resource             = loom.Resource
	Observation          = loom.Observation
	Backend              = loom.Backend
	NetworkPolicyBackend = loom.NetworkPolicyBackend
)

func encode(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}

func gcID(id, prefix string) bool {
	return loom.ValidResourceID(id, prefix)
}

func openCaptureDir(path string) (*os.File, error) {
	return loom.OpenCaptureDir(path)
}

func delegateCapture(resource Resource, destination string, uid, gid int) error {
	return loom.DelegateCapture(resource, destination, uid, gid)
}

// CheckConfig verifies resource syntax and the current supported shape bounds.
// Catalog publication and admission budgets are checked by the control plane.
func CheckConfig(config *loom.Config) error {
	if config == nil {
		return errors.New("config required")
	}
	if !strings.HasPrefix(config.Image, "sha256:") || len(config.Image) != 71 {
		return errors.New("image must be an imported sha256 manifest digest")
	}
	if _, err := hex.DecodeString(config.Image[7:]); err != nil {
		return errors.New("invalid image digest")
	}
	shape := config.Shape
	if shape.VCPUs < 1 || shape.VCPUs > 255 ||
		shape.MemoryMiB < 128 || shape.MemoryMiB > 1<<24 ||
		shape.DiskMiB < 64 || shape.DiskMiB > 1<<30 {
		return errors.New("shape outside supported bounds")
	}
	return nil
}
