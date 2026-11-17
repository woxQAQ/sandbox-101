package operation

import (
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/types"
)

// AcceptedCommand reads the sanitized immutable request envelope. It does not
// infer an action from the current object or a later operation.
func AcceptedCommand(request []byte) (types.Command, error) {
	var command types.Command
	var envelope struct {
		Request json.RawMessage
		Claim   json.RawMessage
	}
	if err := json.Unmarshal(request, &envelope); err != nil {
		return command, err
	}
	payload := envelope.Request
	if len(envelope.Claim) > 0 {
		payload = envelope.Claim
	}
	if err := json.Unmarshal(payload, &command); err != nil {
		return command, err
	}
	if command.Action == "" {
		return command, fmt.Errorf("%w: public operation lacks accepted command", errs.ErrManualRecovery)
	}
	if command.Action == "checkpoint_delete" {
		command.Action = "delete_checkpoint"
	}
	return command, nil
}
