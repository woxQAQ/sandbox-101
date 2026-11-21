package quota

// SlotBudget fixes the allocation's quota category and units independently of
// its physical network slot. Unclaimed warm allocations consume zero units.
type SlotBudget struct {
	Kind  string `json:"kind"`
	Units int64  `json:"units"`
}
