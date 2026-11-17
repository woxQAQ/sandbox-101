package execution

// Availability owns the process-local draining decision. It shares the short
// registration guard used by admission and shutdown; restart begins available.
type Availability struct {
	registration Guard
	draining     bool
}

func NewAvailability(registration Guard) *Availability {
	return &Availability{
		registration: registration,
	}
}

func (a *Availability) Drain() {
	a.registration.Lock()
	a.draining = true
	a.registration.Unlock()
}

func (a *Availability) IsDraining() bool {
	a.registration.Lock()
	draining := a.draining
	a.registration.Unlock()
	return draining
}
