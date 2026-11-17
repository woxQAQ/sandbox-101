package execution

import (
	"context"
	"sync"
)

// Guard is a shared serialization capability. Callers retain the workflow's
// acquisition order and duration; the registry never holds its mutex while a
// caller waits for a guard. Guard implementations and their storage stay private.
type Guard interface {
	Lock()
	Unlock()
	TryLock() bool
}

type guardKind uint8

const (
	admissionGuard guardKind = iota
	artifactGuard
	gcAdmissionGuard
	tenantRequestGuard
	nodeRequestGuard
	vmGuard
	configurationGuard
	sandboxGuard
	autoResumeGuard
	templateGuard
	imageImportGuard
	imageCleanupGuard
	gcOperationGuard
	supplyCleanupGuard
	forkGroupGuard
	snapshotGuard
	registrationGuard
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

// Registration serializes admission checks, data connection registration and
// shutdown's transition to closing using the same short guard.
func (g *Guards) Registration() Guard {
	return g.get(registrationGuard, "", "")
}

func (g *Guards) Admission() Guard {
	return g.get(admissionGuard, "", "")
}

func (g *Guards) Artifacts() Guard {
	return g.get(artifactGuard, "", "")
}

func (g *Guards) GarbageCollection() Guard {
	return g.get(gcAdmissionGuard, "", "")
}

func (g *Guards) TenantRequest(tenant, key string) Guard {
	return g.get(tenantRequestGuard, tenant, key)
}

func (g *Guards) NodeRequest(node, key string) Guard {
	return g.get(nodeRequestGuard, node, key)
}

func (g *Guards) VM(id string) Guard {
	return g.get(vmGuard, "", id)
}

func (g *Guards) Configuration(sandbox string) Guard {
	return g.get(configurationGuard, "", sandbox)
}

func (g *Guards) Sandbox(id string) Guard {
	return g.get(sandboxGuard, "", id)
}

func (g *Guards) AutoResume(sandbox string) Guard {
	return g.get(autoResumeGuard, "", sandbox)
}

func (g *Guards) TemplateWork(template string) Guard {
	return g.get(templateGuard, "", template)
}

func (g *Guards) ImageImport(operation string) Guard {
	return g.get(imageImportGuard, "", operation)
}

func (g *Guards) ImageCleanup(operation string) Guard {
	return g.get(imageCleanupGuard, "", operation)
}

func (g *Guards) GCOperation(operation string) Guard {
	return g.get(gcOperationGuard, "", operation)
}

func (g *Guards) SupplyCleanup(vm string) Guard {
	return g.get(supplyCleanupGuard, "", vm)
}

func (g *Guards) ForkGroup(operation string) Guard {
	return g.get(forkGroupGuard, "", operation)
}

func (g *Guards) Snapshot(id string) Guard {
	return g.get(snapshotGuard, "", id)
}

// Capture reserves the process-wide single capture slot until release is called.
// Cancellation interrupts the wait, not a slot already held by another capture.
func (g *Guards) Capture(ctx context.Context) (release func(), err error) {
	select {
	case g.capture <- struct{}{}:
		return func() {
			<-g.capture
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
