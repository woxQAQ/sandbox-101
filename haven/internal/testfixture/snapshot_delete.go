package testfixture

import (
	"sync/atomic"
)

type SnapshotFaults struct {
	current atomic.Pointer[func(string) error]
}

func (faults *SnapshotFaults) Check(point string) error {
	if current := faults.current.Load(); current != nil {
		return (*current)(point)
	}
	return nil
}

func (faults *SnapshotFaults) Set(fault func(string) error) {
	if fault == nil {
		faults.current.Store(nil)
		return
	}
	faults.current.Store(&fault)
}
