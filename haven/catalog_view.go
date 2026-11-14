package haven

import "github.com/woxQAQ/haven/types"

// Catalog views expose business fields without environment values or host paths.
func wireSandbox(value Sandbox) types.Sandbox {
	return types.Sandbox{
		ID:              value.ID,
		TenantID:        value.TenantID,
		State:           value.State,
		VMID:            value.VMID,
		Instance:        value.Instance,
		SnapshotID:      value.SnapshotID,
		TemplateID:      value.TemplateID,
		ProvisionSource: value.ProvisionSource,
		Shape:           types.Shape(value.Shape),
		Deadline:        value.Deadline,
		PausedUntil:     value.PausedUntil,
		TTLSeconds:      value.TTLSeconds,
		OnExpire:        value.OnExpire,
		AutoRenew:       value.AutoRenew,
		Policy:          types.Policy(value.Policy),
		RequiresSecrets: value.RequiresSecrets,
		ResumeAttempt:   value.ResumeAttempt,
		Epoch:           value.Epoch,
		OperationID:     value.OperationID,
		LastError:       value.LastError,
		CreatedAt:       value.CreatedAt,
	}
}

func wireTemplate(value templateRecord) types.Template {
	return types.Template{
		ID:         value.ID,
		TenantID:   value.TenantID,
		Image:      value.Config.Image,
		Shape:      types.Shape(value.Config.Shape),
		SnapshotID: value.SnapshotID,
		State:      value.State,
		TargetWarm: value.TargetWarm,
		Warmup:     value.Warmup,
	}
}

func wireCheckpoint(value Checkpoint) types.Checkpoint {
	return types.Checkpoint{
		ID:         value.ID,
		TenantID:   value.TenantID,
		SandboxID:  value.SandboxID,
		SnapshotID: value.SnapshotID,
		Shape:      types.Shape(value.Shape),
		CreatedAt:  value.CreatedAt,
	}
}
