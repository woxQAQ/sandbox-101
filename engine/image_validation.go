package engine

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"hash"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/woxQAQ/sandbox-engine/api"
)

type countedHash struct {
	h     hash.Hash
	bytes int64
}

func newSHA256() *countedHash { return &countedHash{h: sha256.New()} }

func (h *countedHash) Write(b []byte) (int, error) {
	n, e := h.h.Write(b)
	h.bytes += int64(n)
	return n, e
}

func shaName(s string) (string, error) {
	d, err := digest.Parse(s)
	if err != nil || d.Algorithm() != digest.SHA256 {
		return "", api.Fail(
			api.Invalid,
			"only canonical sha256 OCI descriptors accepted",
		)
	}
	return d.Encoded(), nil
}

func readBlobJSON(file, want string, v any) error {
	f, e := os.Open(file)
	if e != nil {
		return e
	}
	defer func() { _ = f.Close() }()
	b, e := io.ReadAll(io.LimitReader(f, (16<<20)+1))
	if e != nil {
		return e
	}
	if len(b) > 16<<20 {
		return api.Fail(api.Exhausted, "OCI metadata exceeds 16 MiB")
	}
	sum := sha256.Sum256(b)
	if "sha256:"+hex.EncodeToString(sum[:]) != want {
		return api.Fail(api.Invalid, "OCI metadata checksum mismatch")
	}
	return json.Unmarshal(b, v)
}

func blob(root, digest string) (string, error) {
	n, e := shaName(digest)
	if e != nil {
		return "", e
	}
	p := filepath.Join(root, "blobs", "sha256", n)
	if e = regular(p); e != nil {
		return "", e
	}
	hash, e := digestFile(p)
	if e != nil {
		return "", e
	}
	if hash != digest {
		return "", api.Fail(api.Invalid, "OCI blob checksum mismatch")
	}
	return p, nil
}

func tarPath(s string) error {
	if before, ok := strings.CutSuffix(s, "/"); ok {
		s = before
	}
	if s == "" || strings.HasPrefix(s, "/") {
		return api.Fail(api.Invalid, "unsafe OCI tar path")
	}
	for part := range strings.SplitSeq(s, "/") {
		if part == "" || part == ".." {
			return api.Fail(api.Invalid, "unsafe OCI tar path")
		}
	}
	return nil
}

func validateLayer(
	file, media, diffID string,
	maxBytes, maxFiles int64,
) (int64, int64, error) {
	return validateLayerFor(file, media, diffID, maxBytes, maxFiles, false)
}

func validateLayerFor(
	file, media, diffID string,
	maxBytes, maxFiles int64,
	merged bool,
) (int64, int64, error) {
	f, err := os.Open(file)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = f.Close() }()
	var stream io.Reader = f
	switch media {
	case v1.MediaTypeImageLayerGzip,
		"application/vnd.docker.image.rootfs.diff.tar.gzip":
		g, e := gzip.NewReader(f)
		if e != nil {
			return 0, 0, e
		}
		defer func() { _ = g.Close() }()
		stream = g
	case v1.MediaTypeImageLayer:
	default:
		return 0, 0, api.Fail(
			api.Incompatible,
			"only uncompressed/gzip OCI layers supported, got %s",
			media,
		)
	}
	// Validate the uncompressed digest and tar's complete stream (including padding).
	hash := newSHA256()
	limited := io.LimitReader(stream, maxBytes+1)
	tee := io.TeeReader(limited, hash)
	tr := tar.NewReader(tee)
	var size, count int64
	for {
		h, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return 0, 0, e
		}
		count++
		if count > maxFiles {
			return 0, 0, api.Fail(api.Exhausted, "OCI file count exceeds bound")
		}
		if e = tarPath(h.Name); e != nil {
			return 0, 0, e
		}
		switch h.Typeflag {
		case tar.TypeReg, tar.TypeDir, tar.TypeSymlink:
		case tar.TypeLink:
			if e = tarPath(h.Linkname); e != nil {
				return 0, 0, e
			}
		default:
			return 0, 0, api.Fail(
				api.Invalid,
				"unsupported OCI tar special file %q",
				h.Name,
			)
		}
		if h.Size < 0 || h.Size > maxBytes-size {
			return 0, 0, api.Fail(
				api.Exhausted,
				"OCI expanded size exceeds bound",
			)
		}
		size += h.Size
		if strings.HasPrefix(path.Base(h.Name), ".wh.") &&
			(h.Typeflag != tar.TypeReg || h.Size != 0) {
			return 0, 0, api.Fail(api.Invalid, "invalid whiteout marker")
		}
		e = validateMergedOCIEntry(h)
		if e != nil {
			return 0, 0, e
		}
		if _, e = io.Copy(io.Discard, tr); e != nil {
			return 0, 0, e
		}
	}
	rest, err := io.Copy(io.Discard, tee)
	if err != nil {
		return 0, 0, err
	}
	_ = rest
	if hash.bytes > maxBytes {
		return 0, 0, api.Fail(api.Exhausted, "OCI decompression bound exceeded")
	}
	if "sha256:"+hex.EncodeToString(hash.h.Sum(nil)) != diffID {
		return 0, 0, api.Fail(api.Invalid, "OCI diff_id mismatch")
	}
	return hash.bytes, count, nil
}
