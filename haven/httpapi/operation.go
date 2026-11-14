package httpapi

import "github.com/woxQAQ/haven/types"

// operationResponse adds transport error data to a copy of the frozen business
// view. Internal diagnostics never determine HTTP categories or enter this view.
func operationResponse(view types.Operation) types.Operation {
	view.Error = nil
	if view.Status == "failed" {
		view.Error = &types.ApiError{
			Code:    types.ErrInternal,
			Message: "internal service error",
		}
	}
	return view
}
