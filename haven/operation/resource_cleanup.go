package operation

import (
	"context"
)

func (r *Repository) ResultState(ctx context.Context, id string) (status string, body []byte, err error) {
	row, err := r.queries.GetResultState(ctx, id)
	return row.Status, row.Body, err
}
