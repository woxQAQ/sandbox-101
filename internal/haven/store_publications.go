package haven

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/woxQAQ/haven/api"
)

// Validate only immutable admission/terminal identities. Current sandbox state
// cannot recreate a historical delivery or repair a missing publication.
func (state *store) validatePublications(database *sql.DB) error {
	rows, err := database.Query(`SELECT p.body,o.id,o.idem_key,o.scope_kind,o.scope_id,o.request,o.status,o.body,m.created_at,m.finished_at FROM operation_publications p JOIN operations o ON o.id=p.operation_id JOIN operation_metadata m ON m.operation_id=o.id ORDER BY o.rowid`)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		var body, request, resultBody []byte
		var id, key, kind, owner, status, created, finished string
		if err = rows.Scan(&body, &id, &key, &kind, &owner, &request, &status, &resultBody, &created, &finished); err != nil {
			return err
		}
		var view api.Operation
		var result operationResult
		if err = json.Unmarshal(body, &view); err != nil {
			return err
		}
		if err = json.Unmarshal(resultBody, &result); err != nil {
			return err
		}
		command, err := acceptedCommand(request)
		if err != nil {
			return err
		}
		start, err := time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return err
		}
		end, err := time.Parse(time.RFC3339Nano, finished)
		if err != nil {
			return err
		}
		ids := result.SandboxIDs
		if len(ids) == 0 && result.SandboxID != "" {
			ids = []string{
				result.SandboxID,
			}
		}
		if kind != "tenant" || !terminal(status) || view.ID != id || view.Key != key || view.TenantID != owner || view.Status != status || view.Result.OperationID != id || view.Result.Tokens != nil || !view.CreatedAt.Equal(start) || !view.FinishedAt.Equal(end) || !reflect.DeepEqual(view.Command, command) || !slices.Equal(view.IDs, ids) {
			return fmt.Errorf("frozen delivery differs from accepted operation %s", id)
		}
		if view.Result.Checkpoint != nil && view.Result.Checkpoint.ID != result.CheckpointID {
			return fmt.Errorf("frozen checkpoint delivery differs from terminal result")
		}
		if view.Result.Template != nil && view.Result.Template.ID != result.TemplateID {
			return fmt.Errorf("frozen template delivery differs from terminal result")
		}
		sandboxes := view.Result.Sandboxes
		if view.Result.Sandbox != nil {
			sandboxes = append(append([]api.Sandbox(nil), sandboxes...), *view.Result.Sandbox)
		}
		for _, sandbox := range sandboxes {
			if !slices.Contains(ids, sandbox.ID) || sandbox.Epoch == "" || sandbox.CreatedAt.IsZero() {
				return fmt.Errorf("frozen sandbox delivery identity is incomplete")
			}
		}
	}
	return rows.Err()
}
