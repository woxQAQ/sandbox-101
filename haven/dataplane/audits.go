package dataplane

import (
	"context"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type auditLog interface {
	AppendAudit(context.Context, []byte) error
}

// Audits commits sanitized proxy events before credential substitution.
type Audits struct {
	journal   auditLog
	admission execution.Guard
	lifetime  *execution.Lifetime
	reviewed  bool
}

func NewAudits(state *store.State, admission execution.Guard, lifetime *execution.Lifetime) *Audits {
	return &Audits{
		journal:   state,
		admission: admission,
		lifetime:  lifetime,
		reviewed:  state.Reviewed(),
	}
}

func (a *Audits) Audit(ctx context.Context, event any) error {
	if a.reviewed || !a.lifetime.Begin() {
		return fmt.Errorf("%w: sandbox data admission is closed", errs.ErrConflict)
	}
	defer a.lifetime.End()
	a.admission.Lock()
	defer a.admission.Unlock()
	body := operation.Encode(struct {
		Time  time.Time `json:"time"`
		Event any       `json:"event"`
	}{time.Now().UTC(), event})
	return a.journal.AppendAudit(ctx, body)
}
