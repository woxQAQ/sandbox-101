package authorization

// Injection is a credential descriptor, never its secret value.
type Injection struct {
	Hosts       []string `json:"hosts"`
	Header      string   `json:"header"`
	Placeholder string   `json:"placeholder,omitempty"`
}
