package machine

import (
	"fmt"
	"path/filepath"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	operation "github.com/woxQAQ/haven/operation"
)

func ValidateResourceStep(stateDirectory string, step ResourceStep) error {
	r := step.Input.Resource
	if err := host.CheckConfig(&r.Config); err != nil {
		return err
	}
	source := step.OperationID
	if step.Action == "release" {
		source = step.Input.SourceOperation
	}
	if (step.Action != "cold_start" && step.Action != "clone" && step.Action != "restore" && step.Action != "release") || !loom.ValidResourceID(source, "op-") || !loom.ValidResourceID(r.VMID, "vm-") || !loom.ValidResourceID(r.Instance, "run-") || !loom.ValidResourceID(r.NetworkToken, "net-") || r.Slot < 1 || r.Slot >= 32768 {
		return fmt.Errorf("unsupported or incomplete resource step identity")
	}
	if ((step.Action == "cold_start" || step.Action == "clone") && step.ExpectedInstance != "") || (step.Action == "release" && step.ExpectedInstance != r.Instance) || (!operation.Terminal(step.Status) && step.State.Witness != "") {
		return fmt.Errorf("step instance condition or historical witness is inconsistent")
	}
	if step.Action == "restore" && (step.ExpectedInstance == "" || step.ExpectedInstance == r.Instance || len(step.Input.Ancestors) == 0 || !loom.ValidResourceID(step.Input.StopOperation, "op-")) {
		return fmt.Errorf("restore predecessor/stop proof missing")
	}
	if (step.Input.SlotBudget.Kind != "sandbox_slot" && step.Input.SlotBudget.Kind != "build_slot") || step.Input.SlotBudget.Units < 0 || step.Input.SlotBudget.Units > 1 {
		return fmt.Errorf("resource slot budget is incomplete")
	}
	if step.Input.DiskKey == "" || !loom.ValidResourceID(step.Input.SlotOperation, "op-") || step.Input.RuntimeBytes <= 0 {
		return fmt.Errorf("resource responsibility identity missing")
	}
	if step.Action == "cold_start" {
		unprepared := step.State.Phase == "admitted" || step.State.Phase == "prepare_intent"
		if unprepared != (step.State.DiskIdentity == nil) {
			return fmt.Errorf("cold disk completion proof missing or out of phase")
		}
	}
	if step.Action == "release" && step.Input.DiskIdentity == nil {
		return fmt.Errorf("release disk witness missing")
	}
	directory := filepath.Join(stateDirectory, "vms", r.VMID)
	disk := filepath.Join(directory, "disks", source+".raw")
	restoreDir := ""
	if step.Input.Restore != nil {
		if step.Action == "cold_start" {
			return fmt.Errorf("cold step contains restore source")
		}
		disk = filepath.Join(directory, "versions", source, "disk.raw")
		restoreDir = filepath.Join(directory, "versions", source, "restore")
	} else if step.Action == "clone" || step.Action == "restore" {
		return fmt.Errorf("clone source proof missing")
	}
	if r.Dir != directory || r.Disk != disk || r.RunDir != filepath.Join(directory, "runs", r.Instance) || r.RestoreDir != restoreDir || r.ImageDir != filepath.Join(stateDirectory, "images", r.Config.Image[7:]) || step.Input.Image.Digest != r.Config.Image {
		return fmt.Errorf("resource step paths or image proof mismatch")
	}
	return nil
}
