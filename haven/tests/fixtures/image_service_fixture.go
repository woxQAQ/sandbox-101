package fixtures

import (
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom/imageimport"
)

// This private recovery fixture starts with a frozen plan whose source vanished
// before staging. The real runner prepares owned directories and records failure;
// no EROFS materialization, namespace or VM behavior is represented by this fixture.
func PendingImageRecovery(t *testing.T) (*application.App, string, imageimport.ImageImportOptions) {
	t.Helper()
	service, _ := ColdService(t, nil)
	id, options := testfixture.SeedPendingImageImport(t, service.State)
	return service, id, options
}
