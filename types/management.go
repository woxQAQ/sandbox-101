package types

import "time"

// NodeStatus identifies the single control store and its current admission state.
type NodeStatus struct {
	Format           string `json:"format"`
	Version          int    `json:"version"`
	StoreID          string `json:"store_id"`
	Epoch            int64  `json:"epoch"`
	RecoveryReviewed bool   `json:"recovery_reviewed"`
	Draining         bool   `json:"draining"`
}

// ImageImport selects a local OCI manifest and bounded materialization profile.
// Zero MaxBytes/MaxFiles selects importer defaults.
type ImageImport struct {
	Layout          string `json:"layout"`
	Digest          string `json:"digest"`
	Materialization string `json:"materialization"`
	MaxBytes        int64  `json:"max_bytes"`
	MaxFiles        int64  `json:"max_files"`
}

// GCOptions bounds one collection pass. Budget is encoded as nanoseconds in JSON;
// CLI --gc-budget accepts Go duration syntax. DryRun never admits an operation.
type GCOptions struct {
	DryRun bool          `json:"dry_run"`
	Batch  int           `json:"batch"`
	Budget time.Duration `json:"budget"`
}

// GCItem records a candidate and the proof or reason for its disposition.
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

// GCRoot records a retained control or resource reference.
type GCRoot struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	Path string `json:"path,omitempty"`
}

// GCReport distinguishes candidates from verified collection and deferred work.
// Deferred or blocked items retain their resources and accounting responsibility.
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

// ManagementRequest is a node-scoped intent authenticated by local peer credentials.
// Key is required for mutations; OperationID identifies the original cleanup source.
type ManagementRequest struct {
	Action       string       `json:"action"`
	Key          string       `json:"key"`
	Import       *ImageImport `json:"import,omitempty"`
	GC           *GCOptions   `json:"gc,omitempty"`
	OperationID  string       `json:"operation_id,omitempty"`
	CacheBuilder string       `json:"cache_builder,omitempty"`
	CacheDiffID  string       `json:"cache_diff_id,omitempty"`
}

// ManagementResult is the frozen delivery of a node-scoped operation.
// ResourceReady proves resource preparation, not business or guest authorization.
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

// ManagementOperation exposes only node-scoped operation status and delivery.
type ManagementOperation struct {
	ID     string           `json:"id"`
	Key    string           `json:"key"`
	Status string           `json:"status"`
	Result ManagementResult `json:"result"`
}

// ManagementResponse carries either an admitted operation, a GC report, or both.
type ManagementResponse struct {
	Operation *ManagementOperation `json:"operation,omitempty"`
	GC        *GCReport            `json:"gc,omitempty"`
}
