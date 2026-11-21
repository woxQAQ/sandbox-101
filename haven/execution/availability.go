package execution

// Availability owns the process-local draining decision. It shares the short
// registration guard used by admission and shutdown; restart begins available.
type Availability struct {
	registrationMu Guard
	draining       bool
}

func NewAvailability(registrationMu Guard) *Availability {
	return &Availability{
		registrationMu: registrationMu,
	}
}

func (a *Availability) Drain() {
	a.registrationMu.Lock()
	a.draining = true
	a.registrationMu.Unlock()
}

func (a *Availability) IsDraining() bool {
	a.registrationMu.Lock()
	draining := a.draining
	a.registrationMu.Unlock()
	return draining
}
