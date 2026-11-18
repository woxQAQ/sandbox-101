package fixtures

import (
	"context"
	"testing"

	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
)

func ImportSharedFixture(t *testing.T, layers ...[]TarEntry) (*nativeImageFixture, string, host.ImageManifest) {
	t.Helper()
	RequireEROFS(t)
	layout := t.TempDir()
	var fixtures []OCILayerFixture
	for i, entries := range layers {
		fixtures = append(fixtures, PutOCILayer(t, layout, entries, i%2 == 0))
	}
	image := PutOCIManifest(t, layout, "shared", fixtures)
	e := NewNativeImageFixture(t)
	t.Cleanup(func() {
		e.Close()
	})
	m, err := e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:   layout,
			Digest:   image,
			MaxBytes: 8 << 20,
			MaxFiles: 1000,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return e, layout, m
}
