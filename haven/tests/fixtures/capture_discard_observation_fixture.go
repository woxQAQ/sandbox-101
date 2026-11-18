package fixtures

import (
	"path/filepath"

	"github.com/woxQAQ/haven/machine"
)

func CaptureDiscardPaths(step machine.CaptureDiscardStep) []string {
	source := step.Input.Source
	return []string{
		filepath.Join(source.Input.Source.Input.Resource.Dir, "capture", source.OperationID),
		source.Input.Snapshot.Dir,
	}
}
