package machine

type FreshRequest struct {
	TenantID   string `json:"tenant_id"`
	Key        string `json:"key"`
	TemplateID string `json:"template_id"`
}

type ReleaseRequest struct {
	TenantID  string `json:"tenant_id"`
	Key       string `json:"key"`
	SandboxID string `json:"sandbox_id"`
}

type CloneRequest struct {
	TenantID   string `json:"tenant_id"`
	Key        string `json:"key"`
	SnapshotID string `json:"snapshot_id"`
}

type RestoreRequest struct {
	TenantID         string `json:"tenant_id"`
	Key              string `json:"key"`
	SandboxID        string `json:"sandbox_id"`
	SnapshotID       string `json:"snapshot_id"`
	ExpectedInstance string `json:"expected_instance"`
	StopOperationID  string `json:"stop_operation_id"`
}
