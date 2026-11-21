package execution

import (
	"context"
	"sync"
)

// Guard is a shared serialization capability. Callers retain the workflow's
// acquisition order and duration; the registry never holds its mutex while a
// caller waits for a guard. Guard implementations and their storage stay private.
type Guard interface {
	// Lock waits until the caller owns this serialization domain.
	Lock()
	// Unlock releases the domain after the protected work completes.
	Unlock()
	// TryLock acquires the domain if immediately available and reports success.
	TryLock() bool
}

type guardKind uint8

const (
	storeMutationGuard guardKind = iota
	imageArtifactsGuard
	gcAdmissionGuard
	tenantIdempotencyKeyGuard
	nodeIdempotencyKeyGuard
	vmLifecycleGuard
	sandboxConfigurationGuard
	sandboxLifecycleGuard
	sandboxWakeupGuard
	templateSupplyGuard
	imageImportGuard
	imageCleanupGuard
	gcOperationGuard
	supplyCleanupGuard
	forkBatchGuard
	snapshotDeletionGuard
	serviceAdmissionGuard
)

// Separate fields preserve the request scope/key pair without string prefixes
// or serialized tuples. VM, sandbox and configuration identities are distinct
// lock domains even when their identifiers happen to contain the same bytes.
type guardKey struct {
	kind  guardKind
	owner string
	id    string
}

// Guards owns all workflow serialization domains for one application instance.
// Entries remain stable until the application closes; eviction could let two
// runners protect the same object with different guards.
type Guards struct {
	mu      sync.Mutex
	entries map[guardKey]Guard
	capture chan struct{}
}

func NewGuards() *Guards {
	return &Guards{
		entries: make(map[guardKey]Guard),
		capture: make(chan struct{}, 1),
	}
}

func (g *Guards) get(kind guardKind, owner, id string) Guard {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := guardKey{
		kind:  kind,
		owner: owner,
		id:    id,
	}
	guard := g.entries[key]
	if guard == nil {
		guard = &sync.Mutex{}
		g.entries[key] = guard
	}
	return guard
}

// ForServiceAdmission returns the lock shared by drain, shutdown and connection registration.
func (g *Guards) ForServiceAdmission() Guard {
	return g.get(serviceAdmissionGuard, "", "")
}

// ForStoreMutation returns the node-wide lock for durable admission, state publication and final store close.
func (g *Guards) ForStoreMutation() Guard {
	return g.get(storeMutationGuard, "", "")
}

// ForImageArtifacts returns the lock shared by image publication, cleanup and GC inventory.
func (g *Guards) ForImageArtifacts() Guard {
	return g.get(imageArtifactsGuard, "", "")
}

// ForGCAdmission returns the node-wide lock for admitting a collection and taking inventory.
func (g *Guards) ForGCAdmission() Guard {
	return g.get(gcAdmissionGuard, "", "")
}

// ForTenantIdempotencyKey returns the lock held from replay lookup through admission of a new operation.
func (g *Guards) ForTenantIdempotencyKey(tenantID, key string) Guard {
	return g.get(tenantIdempotencyKeyGuard, tenantID, key)
}

// ForNodeIdempotencyKey returns the lock shared by admissions using this node's key.
func (g *Guards) ForNodeIdempotencyKey(nodeID, key string) Guard {
	return g.get(nodeIdempotencyKeyGuard, nodeID, key)
}

// ForVMLifecycle returns the lock for host effects and completion checks on one logical VM.
func (g *Guards) ForVMLifecycle(vmID string) Guard {
	return g.get(vmLifecycleGuard, "", vmID)
}

// ForSandboxConfiguration returns the lock shared by settings changes, revocation and final data admission.
func (g *Guards) ForSandboxConfiguration(sandboxID string) Guard {
	return g.get(sandboxConfigurationGuard, "", sandboxID)
}

// ForSandboxLifecycle returns the lock for lifecycle transitions and readiness checks across VM generations.
func (g *Guards) ForSandboxLifecycle(sandboxID string) Guard {
	return g.get(sandboxLifecycleGuard, "", sandboxID)
}

// ForSandboxWakeup returns the lock that coalesces data requests waking the same paused sandbox.
func (g *Guards) ForSandboxWakeup(sandboxID string) Guard {
	return g.get(sandboxWakeupGuard, "", sandboxID)
}

// ForTemplateSupply returns the lock shared by build, refill and cleanup for one template.
func (g *Guards) ForTemplateSupply(templateID string) Guard {
	return g.get(templateSupplyGuard, "", templateID)
}

// ForImageImport returns the lock for advancing one image import operation.
func (g *Guards) ForImageImport(operationID string) Guard {
	return g.get(imageImportGuard, "", operationID)
}

// ForImageCleanup returns the lock for advancing one image cleanup operation.
func (g *Guards) ForImageCleanup(operationID string) Guard {
	return g.get(imageCleanupGuard, "", operationID)
}

// ForGCOperation returns the lock for advancing one admitted collection operation.
func (g *Guards) ForGCOperation(operationID string) Guard {
	return g.get(gcOperationGuard, "", operationID)
}

// ForSupplyCleanup returns the lock for retiring the supply allocation on one candidate VM.
func (g *Guards) ForSupplyCleanup(vmID string) Guard {
	return g.get(supplyCleanupGuard, "", vmID)
}

// ForForkBatch returns the lock for advancing the branches of one fork operation.
func (g *Guards) ForForkBatch(operationID string) Guard {
	return g.get(forkBatchGuard, "", operationID)
}

// ForSnapshotDeletion returns the lock for retiring one snapshot.
func (g *Guards) ForSnapshotDeletion(snapshotID string) Guard {
	return g.get(snapshotDeletionGuard, "", snapshotID)
}

// AcquireCaptureSlot waits for the process-wide capture slot and returns its release function.
func (g *Guards) AcquireCaptureSlot(ctx context.Context) (release func(), err error) {
	select {
	case g.capture <- struct{}{}:
		return func() {
			<-g.capture
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
