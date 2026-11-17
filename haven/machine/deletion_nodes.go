package machine

import (
	"path/filepath"

	"github.com/woxQAQ/haven/loom"
)

func GenerationFixedNodes(source ResourceStep) map[string]loom.FileIdentity {
	fixed := map[string]loom.FileIdentity{}
	r := source.Input.Resource
	if source.State.DiskIdentity != nil {
		relative, _ := filepath.Rel(r.Dir, r.Disk)
		fixed[relative] = *source.State.DiskIdentity
	}
	if proof := source.State.Materials; proof != nil {
		version, _ := filepath.Rel(r.Dir, filepath.Dir(r.RestoreDir))
		fixed[version] = proof.Version
		fixed[version+"/restore"] = proof.Restore
		for _, name := range loom.RestoreFileNames() {
			file := proof.Files[name]
			if file.Candidate == "" && name != "memory-ranges" {
				continue
			}
			relative := version + "/restore/" + name
			if name == "disk.raw" {
				relative = version + "/disk.raw"
			}
			fixed[relative] = file.Identity
		}
	}
	return fixed
}
