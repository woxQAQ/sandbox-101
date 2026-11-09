package engine

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	ocidigest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/woxQAQ/sandbox-engine/api"
)

const (
	// This is self-contained OCI compatibility, not the shared fsmeta layout or
	// the historical, unversioned merged builder.
	MergedOCIEROFSLayout = "merged-oci-erofs-v1"
	mergedOCIProfile     = "umoci-native-merged-v2;unix-paths;raw-unpack;umask=077;uid0-mapped-userns;tmpfs-root;bwrap;caps=chown,dac_override,fowner,fsetid,setfcap;4096;noinline_data;workers=1;uuid=manifest;mkfs-time=0;mtime=preserve;xattrs=native;boot=fsck-openroot"
)

func mergedOCIBuilder(tools map[string]string) string {
	var profile strings.Builder
	profile.WriteString(mergedOCIProfile)
	for _, tool := range []string{"umoci", "unshare", "bwrap", "mkfs.erofs", "fsck.erofs"} {
		profile.WriteString("\n" + tool + "=" + tools[tool])
	}
	sum := sha256.Sum256([]byte(profile.String()))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func mergedOCITools(
	ctx context.Context,
	tools map[string]string,
) (map[string]string, error) {
	for _, tool := range []string{"umoci", "unshare"} {
		if _, err := exec.LookPath(tool); err != nil {
			return nil, api.Fail(
				api.Incompatible,
				"required OCI compatibility tool missing: %s",
				tool,
			)
		}
		out, err := command(ctx, tool, "--version")
		if err != nil {
			return nil, err
		}
		tools[tool] = strings.TrimSpace(string(out))
	}
	if !supportedUmoci(tools["umoci"]) {
		return nil, api.Fail(
			api.Incompatible,
			"OCI compatibility materializer requires umoci 0.6.0",
		)
	}
	return tools, nil
}

func supportedUmoci(version string) bool {
	return strings.HasPrefix(version, "umoci version 0.6.0\n") ||
		version == "umoci version 0.6.0"
}

// Security/resource/digest validation is shared with native layered preflight.
// Only restrictions imposed by independently converting each tar are relaxed.
//
//nolint:staticcheck // Validate both legacy tar headers and the deprecated xattr map accepted by archive/tar.
func validateMergedOCIEntry(h *tar.Header) error {
	bad := func(reason string) error {
		return api.Fail(
			api.Incompatible,
			"unsupported OCI compatibility entry %q: %s",
			h.Name,
			reason,
		)
	}
	if path.Clean(h.Name) == "." && h.Typeflag != tar.TypeDir {
		return api.Fail(api.Invalid, "OCI root entry must be a directory")
	}
	if h.Uid < 0 || uint64(h.Uid) >= 1<<32-1 || h.Gid < 0 ||
		uint64(h.Gid) >= 1<<32-1 ||
		h.Mode < 0 ||
		h.Mode & ^int64(0o7777) != 0 ||
		h.ModTime.Unix() < 0 {
		return bad("metadata outside supported range")
	}
	if h.Typeflag == tar.TypeSymlink && h.Mode != 0o777 {
		return bad("Linux symlinks require mode 0777")
	}
	if h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA && h.Size != 0 {
		return api.Fail(api.Invalid, "OCI nonregular entry has data")
	}
	if base := path.Base(
		path.Clean(h.Name),
	); strings.HasPrefix(base, ".wh.") &&
		base != ".wh..wh..opq" {
		if victim := strings.TrimPrefix(
			base,
			".wh.",
		); victim == "" || victim == "." ||
			victim == ".." {
			return api.Fail(api.Invalid, "invalid OCI whiteout target")
		}
	}
	for key := range h.PAXRecords {
		if after, ok := strings.CutPrefix(key, "SCHILY.xattr."); ok {
			if !supportedLayerXattr(after) {
				return bad("unsupported xattr namespace")
			}
			continue
		}
		switch key {
		case "path",
			"linkpath",
			"size",
			"uid",
			"gid",
			"uname",
			"gname",
			"mtime":
		default:
			return bad("unsupported PAX attribute " + key)
		}
	}
	for key := range h.Xattrs {
		if !supportedLayerXattr(key) {
			return bad("unsupported xattr namespace")
		}
	}
	return nil
}

func (m ImageManifest) validateMergedOCI() error {
	bad := func() error { return api.Fail(api.Incompatible, "invalid OCI merged EROFS materialization identity") }
	if m.Format != 3 || m.Layout != MergedOCIEROFSLayout ||
		m.Architecture != runtime.GOARCH ||
		len(m.Artifacts) != 0 ||
		len(m.DiffIDs) == 0 ||
		len(m.DiffIDs) > MaxImageDataDevices ||
		len(m.Layers) != len(m.DiffIDs) ||
		len(m.Files) != 3 {
		return bad()
	}
	if _, err := shaName(m.Digest); err != nil {
		return bad()
	}
	if _, err := shaName(
		m.Builder,
	); err != nil || m.Builder != mergedOCIBuilder(m.Tools) || !supportedUmoci(m.Tools["umoci"]) || m.Tools["bwrap"] == "" || m.Tools["unshare"] == "" || !supportedEROFSVersion(m.Tools["mkfs.erofs"]) || !supportedEROFSVersion(m.Tools["fsck.erofs"]) ||
		erofsVersion.FindString(
			m.Tools["mkfs.erofs"],
		) != erofsVersion.FindString(
			m.Tools["fsck.erofs"],
		) {
		return bad()
	}
	for _, name := range []string{"kernel", "initramfs", "root.erofs"} {
		if _, err := shaName(m.Files[name]); err != nil {
			return bad()
		}
	}
	for i, diff := range m.DiffIDs {
		if _, err := shaName(diff); err != nil {
			return bad()
		}
		if _, err := shaName(m.Layers[i]); err != nil {
			return bad()
		}
	}
	return nil
}

func (m ImageManifest) verifyMergedOCI(dir, want string) error {
	if m.Digest != want {
		return api.Fail(api.Incompatible, "image digest mismatch")
	}
	if err := m.validateMergedOCI(); err != nil {
		return err
	}
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir ||
		filepath.Base(dir) != m.Digest[7:] ||
		filepath.Base(filepath.Dir(dir)) != "images" {
		return api.Fail(
			api.Incompatible,
			"image directory does not match manifest",
		)
	}
	for _, parent := range []string{filepath.Dir(dir), dir} {
		st, err := os.Lstat(parent)
		if err != nil {
			return err
		}
		if !st.IsDir() {
			return api.Fail(
				api.Incompatible,
				"image directory traverses a symlink or nondirectory",
			)
		}
	}
	return m.verifyMergedOCIArtifacts(dir)
}

func (m ImageManifest) verifyMergedOCIArtifacts(dir string) error {
	if err := m.validateMergedOCI(); err != nil {
		return err
	}
	for rel, hash := range m.Files {
		if err := verifyImmutable(dir, rel, hash); err != nil {
			return err
		}
	}
	sb, size, err := erofsSuper(filepath.Join(dir, "root.erofs"))
	if err != nil {
		return err
	}
	if binary.LittleEndian.Uint16(sb[86:]) != 0 ||
		binary.LittleEndian.Uint32(sb[80:]) != 0 ||
		uint64(binary.LittleEndian.Uint32(sb[36:])) != uint64(size/4096) ||
		hex.EncodeToString(sb[48:64]) != m.Digest[7:39] {
		return api.Fail(
			api.Incompatible,
			"invalid self-contained OCI merged EROFS superblock",
		)
	}
	return nil
}

func materializeMergedOCI(
	ctx context.Context,
	input, out, digest string,
	expandedLimit int64,
) error {
	// Construct a minimal trusted index. No source index tags, URLs, annotations
	// or runtime configuration can select a different image for unpacking.
	if err := writeJSON(
		filepath.Join(input, "oci-layout"),
		v1.ImageLayout{Version: "1.0.0"},
	); err != nil {
		return err
	}
	n, _ := shaName(digest)
	st, err := os.Stat(filepath.Join(input, "blobs", "sha256", n))
	if err != nil {
		return err
	}
	index := v1.Index{
		Versioned: specs.Versioned{SchemaVersion: 2},
		Manifests: []v1.Descriptor{
			{
				MediaType:   v1.MediaTypeImageManifest,
				Digest:      ocidigest.Digest(digest),
				Size:        st.Size(),
				Annotations: map[string]string{v1.AnnotationRefName: "compat"},
			},
		},
	}
	if err = writeJSON(filepath.Join(input, "index.json"), index); err != nil {
		return err
	}
	reads := []imageMount{{input, "/input"}}
	for _, tool := range []string{"umoci", "mkfs.erofs", "sh"} {
		binary, x := exec.LookPath(tool)
		if x != nil {
			return x
		}
		binary, x = filepath.EvalSymlinks(binary)
		if x != nil {
			return x
		}
		reads = append(reads, imageMount{binary, "/tools/" + tool})
	}
	args, err := imageToolSandbox(out, reads, true)
	if err != nil {
		return err
	}
	args = append(
		args,
		"--size",
		strconv.FormatInt(expandedLimit, 10),
		"--tmpfs",
		"/merged",
		"--",
		"/tools/sh",
		"-euc",
		`umask 077
/tools/umoci raw unpack --image /input:compat /merged/root
exec /tools/mkfs.erofs --quiet --workers=1 -b4096 -E noinline_data -T0 --mkfs-time --preserve-mtime -U "$1" /work/root.erofs /merged/root`,
		"builder",
		layerUUID(digest),
	)
	if err = runMappedImageTool(ctx, args); err != nil {
		return err
	}
	return extractImageBoot(
		ctx,
		out,
		[]imageMount{{filepath.Join(out, "root.erofs"), "/root.erofs"}},
		nil,
		"/root.erofs",
	)
}

func runMappedImageTool(ctx context.Context, sandboxArgs []string) error {
	bwrap, err := exec.LookPath("bwrap")
	if err != nil {
		return err
	}
	bwrap, err = filepath.EvalSymlinks(bwrap)
	if err != nil {
		return err
	}
	args := []string{"--user", "--map-auto", "--map-root-user", "--keep-caps"}
	if os.Geteuid() == 0 {
		// A root daemon can identity-map all valid Linux IDs, still inside a new
		// user namespace and the same readonly bubblewrap filesystem boundary.
		args = []string{
			"--user",
			"--map-users=0:0:4294967295",
			"--map-groups=0:0:4294967295",
			"--keep-caps",
		}
	}
	args = append(args, bwrap)
	args = append(args, sandboxArgs...)
	_, err = command(ctx, "unshare", args...)
	return err
}
