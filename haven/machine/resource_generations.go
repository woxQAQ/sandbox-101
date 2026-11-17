package machine

import (
	"encoding/json"

	"github.com/woxQAQ/haven/loom"
	operation "github.com/woxQAQ/haven/operation"
)

func ExpectedCurrentInstance(step ResourceStep, current string) bool {
	if current == step.Input.Resource.Instance {
		return true
	}
	if step.Action != "restore" || current != step.ExpectedInstance {
		return false
	}
	switch step.State.Phase {
	case "admitted", "restore_prepare_intent", "restore_candidate_prepared", "restore_candidate_copied", "restore_candidate_renamed", "restore_candidate_synced", "restore_materials_complete", "restore_resources_ready":
		return true
	default:
		return false
	}
}

func CopyRestoreMaterials(proof *loom.RestoreMaterials) *loom.RestoreMaterials {
	if proof == nil {
		return nil
	}
	var copy loom.RestoreMaterials
	if err := json.Unmarshal(operation.Encode(proof), &copy); err != nil {
		panic(err)
	}
	return &copy
}
