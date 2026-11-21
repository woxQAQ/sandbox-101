package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

var erofsVersion = regexp.MustCompile(`\b(\d+)\.(\d+)\.(\d+)\b`)

func supportedEROFSVersion(s string) bool {
	v := erofsVersion.FindStringSubmatch(s)
	if len(v) != 4 {
		return false
	}
	major, _ := strconv.Atoi(v[1])
	minor, _ := strconv.Atoi(v[2])
	patch, _ := strconv.Atoi(v[3])
	return major == 1 && (minor > 9 || minor == 9 && patch >= 4)
}

func imageTools(ctx context.Context) (map[string]string, error) {
	tools := map[string]string{
		"go-erofs": sharedEROFSVersion,
	}
	for _, tool := range []string{
		"mkfs.erofs",
		"fsck.erofs",
	} {
		if _, err := exec.LookPath(tool); err != nil {
			return nil, fmt.Errorf("required OCI tool missing: %s", tool)
		}
		v, err := command(ctx, tool, "--version")
		if err != nil {
			return nil, err
		}
		tools[tool] = strings.TrimSpace(string(v))
		if !supportedEROFSVersion(tools[tool]) {
			return nil, errors.New("layered materializer requires erofs-utils >=1.9.4 and <2")
		}
	}
	if erofsVersion.FindString(tools["mkfs.erofs"]) != erofsVersion.FindString(tools["fsck.erofs"]) {
		return nil, errors.New("mkfs.erofs and fsck.erofs versions must match")
	}
	return tools, nil
}

func supportedLayerMedia(media string) bool {
	return media == v1.MediaTypeImageLayer || media == v1.MediaTypeImageLayerGzip ||
		media == "application/vnd.docker.image.rootfs.diff.tar.gzip"
}

// Tools operate directly on explicit host paths in a private work directory.
// Executables come from the administrator's PATH, never from image contents.
func runImageTool(ctx context.Context, work, tool string, args ...string) error {
	binary, err := exec.LookPath(tool)
	if err != nil {
		return err
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir = work
	cmd.Env = []string{
		"PATH=" + hostToolPath(),
		"LC_ALL=C",
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %w: %.4096s", tool, err, strings.TrimSpace(string(output)))
	}
	return nil
}

type layerCache struct {
	Format  int    `json:"format"`
	Profile string `json:"profile"`
	Builder string `json:"builder"`
	DiffID  string `json:"diff_id"`
	SHA256  string `json:"sha256"`
	Bytes   int64  `json:"tar_bytes"`
	Entries int64  `json:"entries"`
}

func loadLayerCache(state, builder, diff string) (layerCache, error) {
	var cached layerCache
	for _, d := range []string{
		builder,
		diff,
	} {
		if _, err := shaName(d); err != nil {
			return cached, err
		}
	}
	rel := layerRelativePath(builder, diff)
	dir := filepath.Join(state, filepath.Dir(rel))
	current := state
	for part := range strings.SplitSeq(filepath.Dir(rel), string(filepath.Separator)) {
		current = filepath.Join(current, part)
		st, err := os.Lstat(current)
		if err != nil {
			return cached, err
		}
		if !st.IsDir() {
			return cached, errors.New("layer cache traverses a symlink or nondirectory")
		}
	}
	// An existing but incomplete cache is corruption, not a cache miss. Never
	// replace evidence or silently regenerate bytes under a published identity.
	bad := func() error {
		return fmt.Errorf("missing/corrupt layer cache receipt or artifact: %s", diff)
	}
	receipt := filepath.Join(dir, "layer.json")
	if regular(receipt) != nil || readJSON(receipt, &cached) != nil || cached.Format != 1 ||
		cached.Profile != layerProfile && cached.Profile != sharedLayerProfile ||
		cached.Builder != builder ||
		cached.DiffID != diff ||
		cached.Bytes < 0 ||
		cached.Entries < 0 {
		return cached, bad()
	}
	if _, err := shaName(cached.SHA256); err != nil {
		return cached, bad()
	}
	if err := verifyImmutable(state, rel, cached.SHA256); err != nil {
		return cached, bad()
	}
	// Receipt permissions and each path component must be safe too.
	hash, err := digestFile(receipt)
	if err != nil || verifyImmutable(state, filepath.Join(filepath.Dir(rel), "layer.json"), hash) != nil {
		return cached, bad()
	}
	return cached, nil
}

func (c layerCache) artifact(state string, index int) (ImageArtifact, error) {
	receipt := filepath.Join(state, filepath.Dir(layerRelativePath(c.Builder, c.DiffID)), "layer.json")
	hash, err := digestFile(receipt)
	return DataArtifact(LayerArtifactIdentity{
		Builder:     c.Builder,
		DiffID:      c.DiffID,
		SHA256:      c.SHA256,
		CacheSHA256: hash,
	}, index), err
}

func BuildLayerCache(ctx context.Context, state string, request LayerCacheRequest) (LayerCache, error) {
	input, media := request.Input.Path, request.Input.MediaType
	builder, diff := request.Builder, request.Input.DiffID
	cached := layerCache{
		Format:  1,
		Profile: layerProfile,
		Builder: builder,
		DiffID:  diff,
		Bytes:   request.Bytes,
		Entries: request.Entries,
	}
	parent := filepath.Join(state, "layer-cache", builder[7:])
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return cached, err
	}
	stage, err := os.MkdirTemp(parent, ".pending-")
	if err != nil {
		return cached, err
	}
	defer func() {
		_ = os.RemoveAll(stage)
	}()
	args := []string{
		"--quiet",
		"--workers=1",
		"--tar=f",
		"--aufs",
		"-b4096",
		"-E",
		"noinline_data",
		"-T0",
		"--mkfs-time",
		"--preserve-mtime",
		"-U",
		layerUUID(diff),
	}
	if media == v1.MediaTypeImageLayerGzip || media == "application/vnd.docker.image.rootfs.diff.tar.gzip" {
		args = append(args, "--ungzip")
	}
	args = append(args, filepath.Join(stage, "layer.erofs"), input)
	if err = runImageTool(ctx, stage, "mkfs.erofs", args...); err != nil {
		return cached, err
	}
	if err = runImageTool(ctx, stage, "fsck.erofs", "--extract", filepath.Join(stage, "layer.erofs")); err != nil {
		return cached, err
	}
	return publishLayerCache(state, stage, cached)
}

func MaterializeFsmeta(ctx context.Context, input FsmetaLayers, manifestDigest string) error {
	state, out, artifacts := input.StateRoot, input.Output, input.Artifacts
	args := []string{
		"--quiet",
		"--workers=1",
		"--ovlfs-strip=1",
		"-T0",
		"--mkfs-time",
		"--preserve-mtime",
		"-U",
		layerUUID(manifestDigest),
		filepath.Join(out, "fsmeta.erofs"),
	}
	var layers, diffs, devices []string
	for _, a := range artifacts {
		host := filepath.Join(state, a.Path)
		args = append(args, host)
		devices = append(devices, "--device="+host)
		layers = append(layers, host)
		diffs = append(diffs, a.DiffID)
	}
	if err := runImageTool(ctx, out, "mkfs.erofs", args...); err != nil {
		return err
	}
	if err := verifyDeviceTable(filepath.Join(out, "fsmeta.erofs"), layers, diffs); err != nil {
		return err
	}
	// Native fsck reads every merged file through its external block mapping.
	// Extraction is temporary; it is not the published lower.
	return extractImageBoot(ctx, out, devices, filepath.Join(out, "fsmeta.erofs"))
}

func extractImageBoot(ctx context.Context, out string, devices []string, image string) error {
	check, err := os.MkdirTemp(filepath.Dir(out), "check-")
	if err != nil {
		return err
	}
	defer func() {
		_ = os.RemoveAll(check)
	}()
	args := append(
		[]string{
			"--extract=" + filepath.Join(check, "tree"),
			"--no-preserve-owner",
			"--preserve-perms",
		},
		devices...)
	args = append(args, image)
	if err = runImageTool(ctx, check, "fsck.erofs", args...); err != nil {
		return err
	}
	root, err := os.OpenRoot(filepath.Join(check, "tree"))
	if err != nil {
		return err
	}
	defer func() {
		_ = root.Close()
	}()
	for _, required := range []string{
		"boot/vmlinux",
		"boot/initramfs",
		"usr/lib/systemd/systemd",
		"usr/local/libexec/sandbox-agent",
		"etc/systemd/system/sysinit.target.wants/sandbox-agent.service",
	} {
		st, x := root.Stat(required)
		if x != nil || !st.Mode().IsRegular() || st.Size() == 0 {
			return fmt.Errorf("image boot contract missing %s", required)
		}
	}
	for _, item := range []struct {
		src string
		dst string
	}{
		{"boot/vmlinux", "kernel"},
		{"boot/initramfs", "initramfs"},
	} {
		st, x := root.Lstat(item.src)
		if x != nil || !st.Mode().IsRegular() {
			return errors.New("boot materials must be regular nonsymlink files")
		}
		in, x := root.Open(item.src)
		if x != nil {
			return x
		}
		x = copyBootFile(in, filepath.Join(out, item.dst))
		_ = in.Close()
		if x != nil {
			return x
		}
	}
	return nil
}

func copyBootFile(in io.Reader, dest string) error {
	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o400)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	closeErr := out.Close()
	if err != nil {
		return err
	}
	return closeErr
}
