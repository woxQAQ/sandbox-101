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

func (service *Availability) Drain() {
	service.registration.Lock()
	service.draining = true
	service.registration.Unlock()
}

func (service *Availability) IsDraining() bool {
	service.registration.Lock()
	draining := service.draining
	service.registration.Unlock()
	return draining
}
