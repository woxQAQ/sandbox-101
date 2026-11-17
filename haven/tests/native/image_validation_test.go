package native

import (
	"archive/tar"
	"bytes"
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestLayerValidationRejectsTraversalSpecialFilesAndBounds(t *testing.T) {
	for _, h := range []*tar.Header{
		{
			Name:     "../../escape",
			Typeflag: tar.TypeReg,
		},
		{
			Name:     "/absolute",
			Typeflag: tar.TypeReg,
		},
		{
			Name:     "bad//path",
			Typeflag: tar.TypeReg,
		},
		{
			Name:     "dev/evil",
			Typeflag: tar.TypeChar,
		},
		{
			Name:     "pipe",
			Typeflag: tar.TypeFifo,
		},
		{
			Name:     "link",
			Typeflag: tar.TypeLink,
			Linkname: "../outside",
		},
		{
			Name:     ".wh.hidden",
			Typeflag: tar.TypeReg,
			Size:     1,
		},
	} {
		file, diff := layerFixture(t, h)
		if _, _, err := validateLayer(layerInput{
			Path:      file,
			MediaType: v1.MediaTypeImageLayer,
			DiffID:    diff,
		}, layerLimits{
			MaxBytes: 1 << 20,
			MaxFiles: 100,
		}); err == nil {
			t.Fatalf("unsafe header accepted %+v", h)
		}
	}
	file, diff := layerFixture(t, &tar.Header{
		Name:     "ok",
		Typeflag: tar.TypeReg,
		Size:     1024,
	})
	if _, _, err := validateLayer(layerInput{
		Path:      file,
		MediaType: v1.MediaTypeImageLayer,
		DiffID:    diff,
	}, layerLimits{
		MaxBytes: 100,
		MaxFiles: 100,
	}); err == nil {
		t.Fatal("expanded-byte bound ignored")
	}
	if _, _, err := validateLayer(layerInput{
		Path:      file,
		MediaType: v1.MediaTypeImageLayer,
		DiffID:    diff,
	}, layerLimits{
		MaxBytes: 1 << 20,
		MaxFiles: 0,
	}); err == nil {
		t.Fatal("file count bound ignored")
	}
	if _, _, err := validateLayer(
		layerInput{
			Path:      file,
			MediaType: v1.MediaTypeImageLayer,
			DiffID:    "sha256:" + string(bytes.Repeat([]byte("0"), 64)),
		},
		layerLimits{
			MaxBytes: 1 << 20,
			MaxFiles: 100,
		},
	); err == nil {
		t.Fatal("diff_id mismatch accepted")
	}
}

func TestLayerValidationKeepsSafeLinksAndWhiteouts(t *testing.T) {
	file, diff := layerFixture(
		t,
		&tar.Header{
			Name:     "etc/",
			Typeflag: tar.TypeDir,
		},
		&tar.Header{
			Name:     "etc/.wh.removed",
			Typeflag: tar.TypeReg,
		},
		&tar.Header{
			Name:     "etc/.wh..wh..opq",
			Typeflag: tar.TypeReg,
		},
		&tar.Header{
			Name:     "regular",
			Typeflag: tar.TypeReg,
		},
		&tar.Header{
			Name:     "hard",
			Typeflag: tar.TypeLink,
			Linkname: "regular",
		},
		&tar.Header{
			Name:     "symbolic",
			Typeflag: tar.TypeSymlink,
			Linkname: "/absolute/guest/path",
			Mode:     0o777,
		},
	)
	if _, n, err := validateLayer(layerInput{
		Path:      file,
		MediaType: v1.MediaTypeImageLayer,
		DiffID:    diff,
	}, layerLimits{
		MaxBytes: 1 << 20,
		MaxFiles: 100,
	}); err != nil || n != 6 {
		t.Fatalf("valid layer rejected n=%d %v", n, err)
	}
}

func TestLayerValidationPreservesLinuxBackslashNames(t *testing.T) {
	file, diff := layerFixture(
		t,
		&tar.Header{
			Name:     `system-systemd\x2dcryptsetup.slice`,
			Typeflag: tar.TypeReg,
			Mode:     0o644,
		},
		&tar.Header{
			Name:     "alias",
			Typeflag: tar.TypeLink,
			Linkname: `system-systemd\x2dcryptsetup.slice`,
			Mode:     0o644,
		},
	)
	if _, n, err := validateLayer(layerInput{
		Path:      file,
		MediaType: v1.MediaTypeImageLayer,
		DiffID:    diff,
	}, layerLimits{
		MaxBytes: 1 << 20,
		MaxFiles: 10,
	}); err != nil || n != 2 {
		t.Fatalf("valid Linux name rejected: entries=%d %v", n, err)
	}
	for _, name := range []string{
		`../system\x2dslice`,
		`safe/../system\x2dslice`,
		`/system\x2dslice`,
	} {
		file, diff := layerFixture(t, &tar.Header{
			Name:     name,
			Typeflag: tar.TypeReg,
		})
		if _, _, err := validateLayer(layerInput{
			Path:      file,
			MediaType: v1.MediaTypeImageLayer,
			DiffID:    diff,
		}, layerLimits{
			MaxBytes: 1 << 20,
			MaxFiles: 10,
		}); err == nil {
			t.Fatalf("traversal accepted: %q", name)
		}
	}
}
