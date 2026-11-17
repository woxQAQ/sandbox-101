package machine

import "github.com/woxQAQ/haven/execution"

// VMGuards borrows the shared admission and per-VM lock domains.
type VMGuards interface {
	Admission() execution.Guard
	VM(string) execution.Guard
}
