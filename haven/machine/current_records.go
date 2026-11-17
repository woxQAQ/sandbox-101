package machine

// Current separates the mutable VM row's ownership from its persisted body.
// The body is decoded by the workflow that verifies the frozen resource input;
// this row never supplies a missing historical witness.
type Current struct {
	Instance string
	State    string
	Owner    string
	Owned    bool // Preserves SQL NULL independently of the owner string.
	Body     []byte
}
