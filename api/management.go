package api

import "time"

type NodeStatus struct {
	Format           string `json:"format"`
	Version          int    `json:"version"`
	StoreID          string `json:"store_id"`
	Epoch            int64  `json:"epoch"`
	RecoveryReviewed bool   `json:"recovery_reviewed"`
	Draining         bool   `json:"draining"`
}
type ImageImport struct {
	Layout          string `json:"layout"`
	Digest          string `json:"digest"`
	Materialization string `json:"materialization"`
	MaxBytes        int64  `json:"max_bytes"`
	MaxFiles        int64  `json:"max_files"`
}
type GCOptions struct {
	DryRun bool          `json:"dry_run"`
	Batch  int           `json:"batch"`
	Budget time.Duration `json:"budget"`
}
type GCItem struct {
	Kind         string `json:"kind"`
	CacheBuilder string `json:"cache_builder,omitempty"`
	CacheDiffID  string `json:"cache_diff_id,omitempty"`
	ID           string `json:"id"`
	TenantID     string `json:"tenant_id,omitempty"`
	Proof        string `json:"proof,omitempty"`
	OperationID  string `json:"operation_id,omitempty"`
	Reason       string `json:"reason,omitempty"`
}
type GCRoot struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	Path string `json:"path,omitempty"`
}
type GCReport struct {
	OperationID string    `json:"operation_id,omitempty"`
	DryRun      bool      `json:"dry_run"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	Roots       []GCRoot  `json:"roots"`
	Candidates  []GCItem  `json:"candidates"`
	Collected   []GCItem  `json:"collected"`
	Deferred    []GCItem  `json:"deferred"`
	Blocked     bool      `json:"blocked"`
}
type ManagementRequest struct {
	Action       string       `json:"action"`
	Key          string       `json:"key"`
	Import       *ImageImport `json:"import,omitempty"`
	GC           *GCOptions   `json:"gc,omitempty"`
	OperationID  string       `json:"operation_id,omitempty"`
	CacheBuilder string       `json:"cache_builder,omitempty"`
	CacheDiffID  string       `json:"cache_diff_id,omitempty"`
}
type ManagementResult struct {
	SandboxIDs    []string `json:"sandbox_ids,omitempty"`
	ImageDigest   string   `json:"image_digest,omitempty"`
	TemplateID    string   `json:"template_id,omitempty"`
	CheckpointID  string   `json:"checkpoint_id,omitempty"`
	SnapshotID    string   `json:"snapshot_id,omitempty"`
	SandboxID     string   `json:"sandbox_id"`
	VMID          string   `json:"vm_id"`
	ResourceReady bool     `json:"resource_ready,omitempty"`
	Error         string   `json:"error,omitempty"`
}
type ManagementOperation struct {
	ID     string           `json:"id"`
	Key    string           `json:"key"`
	Status string           `json:"status"`
	Result ManagementResult `json:"result"`
}
type ManagementResponse struct {
	Operation *ManagementOperation `json:"operation,omitempty"`
	GC        *GCReport            `json:"gc,omitempty"`
}
