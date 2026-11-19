// Package management adapts authenticated local administration to Haven.
package management

import (
	"net/http"
	"strings"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/httpwire"
	"github.com/woxQAQ/haven/localuds"
	"github.com/woxQAQ/haven/node"
	"github.com/woxQAQ/haven/types"
)

func New(service *node.Management) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !localuds.Authorized(r.Context()) {
			httpwire.NewFailure(http.StatusForbidden, types.CodeForbidden, "local administrator peer required").Write(w)
			return
		}
		principal := authorization.Principal{
			Root:     true,
			TenantID: "node-admin",
		}
		switch {
		case r.URL.Path == "/v1/status" && r.Method == http.MethodGet:
			value, err := service.Status(r.Context(), principal)
			httpwire.Respond(w, value, err)
		case r.URL.Path == "/v1/manage" && r.Method == http.MethodPost:
			var request types.ManagementRequest
			if err := httpwire.ReadJSON(w, r, &request); err != nil {
				httpwire.Error(w, err)
				return
			}
			value, err := service.Manage(r.Context(), principal, request)
			httpwire.Respond(w, value, err)
		case strings.HasPrefix(r.URL.Path, "/v1/operations/") && r.Method == http.MethodGet:
			id := strings.TrimPrefix(r.URL.Path, "/v1/operations/")
			if id == "" || strings.Contains(id, "/") {
				httpwire.NotFound().Write(w)
				return
			}
			value, err := service.ManagementOperation(r.Context(), principal, id)
			httpwire.Respond(w, value, err)
		default:
			httpwire.NotFound().Write(w)
		}
	})
}
