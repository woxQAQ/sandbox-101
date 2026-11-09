package engine

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/woxQAQ/sandbox-engine/api"
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
	tools := map[string]string{"go-erofs": sharedEROFSVersion}
	for _, tool := range []string{"bwrap", "mkfs.erofs", "fsck.erofs"} {
		if _, err := exec.LookPath(tool); err != nil {
			return nil, api.Fail(
				api.Incompatible,
				"required OCI tool missing: %s",
				tool,
			)
		}
		v, err := command(ctx, tool, "--version")
		if err != nil {
			return nil, err
		}
		tools[tool] = strings.TrimSpace(string(v))
		if tool != "bwrap" && !supportedEROFSVersion(tools[tool]) {
			return nil, api.Fail(
				api.Incompatible,
				"layered materializer requires erofs-utils >=1.9.4 and <2",
			)
		}
	}
	if erofsVersion.FindString(
		tools["mkfs.erofs"],
	) != erofsVersion.FindString(
		tools["fsck.erofs"],
	) {
		return nil, api.Fail(
			api.Incompatible,
			"mkfs.erofs and fsck.erofs versions must match",
		)
	}
	return tools, nil
}

func supportedLayerMedia(media string) bool {
	return media == v1.MediaTypeImageLayer ||
		media == v1.MediaTypeImageLayerGzip ||
		media == "application/vnd.docker.image.rootfs.diff.tar.gzip"
}

type imageMount struct{ host, guest string }

// All archive interpretation and filesystem extraction happen in a private
// mount/user/network namespace. Only system tool roots and explicitly configured
// runtime dependencies are exposed readonly. Image paths are never host cwd.
func runImageTool(
	ctx context.Context,
	work string,
	reads []imageMount,
	tool string,
	args ...string,
) error {
	binary, err := exec.LookPath(tool)
	if err != nil {
		return err
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		return err
	}
	bargs, err := imageToolSandbox(work, reads, false)
	if err != nil {
		return err
	}
	bargs = append(bargs, "--", binary)
	bargs = append(bargs, args...)
	_, err = command(ctx, "bwrap", bargs...)
	return err
}

func imageToolSandbox(
	work string,
	reads []imageMount,
	mapped bool,
) ([]string, error) {
	bargs := []string{"--die-with-parent", "--new-session"}
	if mapped {
		// Keep the enclosing subordinate-ID mapping and its namespace-only
		// capabilities, so native unpack/chown/xattrs and mkfs see identical IDs.
		bargs = append(
			bargs,
			"--unshare-ipc",
			"--unshare-pid",
			"--unshare-net",
			"--unshare-uts",
			"--unshare-cgroup",
		)
		for _, capability := range []string{"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FOWNER", "CAP_FSETID", "CAP_SETFCAP"} {
			bargs = append(bargs, "--cap-add", capability)
		}
	} else {
		bargs = append(bargs, "--unshare-all", "--uid", "0", "--gid", "0")
	}
	bargs = append(
		bargs,
		"--clearenv",
		"--setenv",
		"LC_ALL",
		"C",
		"--setenv",
		"PATH",
		"/usr/bin:/bin",
		"--proc",
		"/proc",
		"--dev",
		"/dev",
		"--tmpfs",
		"/tmp",
	)
	for _, dir := range []string{"/usr", "/bin", "/lib", "/lib64"} {
		if _, x := os.Stat(dir); x == nil {
			bargs = append(bargs, "--ro-bind", dir, dir)
		}
	}
	// Alternate toolchains declare their dependency roots, not their package
	// manager. This is trusted process configuration, never an image input.
	for _, dir := range filepath.SplitList(os.Getenv("SANDBOX_IMAGE_TOOL_ROOTS")) {
		if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || dir == "/" {
			return nil, fmt.Errorf(
				"invalid SANDBOX_IMAGE_TOOL_ROOTS directory: %q",
				dir,
			)
		}
		for _, reserved := range []string{"/proc", "/dev", "/tmp", "/work", "/tools", "/input", "/merged"} {
			if dir == reserved || strings.HasPrefix(dir, reserved+"/") ||
				strings.HasPrefix(reserved, dir+"/") {
				return nil, fmt.Errorf(
					"tool root %q overlaps sandbox path %q",
					dir,
					reserved,
				)
			}
		}
		info, err := os.Stat(dir)
		if err != nil {
			return nil, fmt.Errorf("image tool root %q: %w", dir, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("image tool root %q is not a directory", dir)
		}
		bargs = append(bargs, "--ro-bind", dir, dir)
	}
	for _, mount := range reads {
		bargs = append(bargs, "--ro-bind", mount.host, mount.guest)
	}
	return append(bargs, "--bind", work, "/work", "--chdir", "/work"), nil
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
	for _, d := range []string{builder, diff} {
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
			return cached, api.Fail(
				api.Incompatible,
				"layer cache traverses a symlink or nondirectory",
			)
		}
	}
	// An existing but incomplete cache is corruption, not a cache miss. Never
	// replace evidence or silently regenerate bytes under a published identity.
	bad := func() error {
		return api.Fail(
			api.Incompatible,
			"missing/corrupt layer cache receipt or artifact: %s",
			diff,
		)
	}
	receipt := filepath.Join(dir, "layer.json")
	if regular(receipt) != nil || readJSON(receipt, &cached) != nil ||
		cached.Format != 1 ||
		cached.Profile != layerProfile &&
			cached.Profile != sharedLayerProfile ||
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
	if err != nil ||
		verifyImmutable(
			state,
			filepath.Join(filepath.Dir(rel), "layer.json"),
			hash,
		) != nil {
		return cached, bad()
	}
	return cached, nil
}

func (c layerCache) artifact(state string, index int) (ImageArtifact, error) {
	receipt := filepath.Join(
		state,
		filepath.Dir(layerRelativePath(c.Builder, c.DiffID)),
		"layer.json",
	)
	hash, err := digestFile(receipt)
	return dataArtifact(c.Builder, c.DiffID, c.SHA256, hash, index), err
}

func buildLayerCache(
	ctx context.Context,
	state, input, media, builder, diff string,
	size, entries int64,
) (layerCache, error) {
	cached := layerCache{
		Format:  1,
		Profile: layerProfile,
		Builder: builder,
		DiffID:  diff,
		Bytes:   size,
		Entries: entries,
	}
	parent := filepath.Join(state, "layer-cache", builder[7:])
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return cached, err
	}
	stage, err := os.MkdirTemp(parent, ".pending-")
	if err != nil {
		return cached, err
	}
	defer func() { _ = os.RemoveAll(stage) }()
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
	if media == v1.MediaTypeImageLayerGzip ||
		media == "application/vnd.docker.image.rootfs.diff.tar.gzip" {
		args = append(args, "--ungzip")
	}
	args = append(args, "/work/layer.erofs", "/input.tar")
	if err = runImageTool(
		ctx,
		stage,
		[]imageMount{{input, "/input.tar"}},
		"mkfs.erofs",
		args...); err != nil {
		return cached, err
	}
	if err = runImageTool(
		ctx,
		stage,
		nil,
		"fsck.erofs",
		"--extract",
		"/work/layer.erofs",
	); err != nil {
		return cached, err
	}
	cached.SHA256, err = digestFile(filepath.Join(stage, "layer.erofs"))
	if err != nil {
		return cached, err
	}
	if err = writeJSON(filepath.Join(stage, "layer.json"), cached); err != nil {
		return cached, err
	}
	if err = sealTree(stage); err != nil {
		return cached, err
	}
	if err = syncTree(stage); err != nil {
		return cached, err
	}
	dest := filepath.Join(parent, diff[7:])
	if _, err = os.Lstat(dest); !os.IsNotExist(err) {
		return cached, api.Fail(
			api.Incompatible,
			"layer cache destination already exists",
		)
	}
	if err = os.Rename(stage, dest); err != nil {
		return cached, err
	}
	for _, dir := range []string{parent, filepath.Dir(parent), state} {
		if err = syncDir(dir); err != nil {
			return cached, err
		}
	}
	return loadLayerCache(state, builder, diff)
}

func materializeFsmeta(
	ctx context.Context,
	state, out string,
	artifacts []ImageArtifact,
	manifestDigest string,
) error {
	args := []string{
		"--quiet",
		"--workers=1",
		"--ovlfs-strip=1",
		"-T0",
		"--mkfs-time",
		"--preserve-mtime",
		"-U",
		layerUUID(manifestDigest),
		"/work/fsmeta.erofs",
	}
	var reads []imageMount
	var layers, diffs, devices []string
	for i, a := range artifacts {
		host := filepath.Join(state, a.Path)
		guest := fmt.Sprintf("/data/%04d.erofs", i)
		reads = append(reads, imageMount{host, guest})
		args = append(args, guest)
		devices = append(devices, "--device="+guest)
		layers = append(layers, host)
		diffs = append(diffs, a.DiffID)
	}
	if err := runImageTool(ctx, out, reads, "mkfs.erofs", args...); err != nil {
		return err
	}
	if err := verifyDeviceTable(
		filepath.Join(out, "fsmeta.erofs"),
		layers,
		diffs,
	); err != nil {
		return err
	}
	// Native fsck reads every merged file through its external block mapping.
	// Extraction is temporary and isolated; it is not the published lower.
	reads = append(
		reads,
		imageMount{filepath.Join(out, "fsmeta.erofs"), "/fsmeta.erofs"},
	)
	return extractImageBoot(ctx, out, reads, devices, "/fsmeta.erofs")
}

func extractImageBoot(
	ctx context.Context,
	out string,
	reads []imageMount,
	devices []string,
	image string,
) error {
	check, err := os.MkdirTemp(filepath.Dir(out), "check-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(check) }()
	args := append(
		[]string{
			"--extract=/work/tree",
			"--no-preserve-owner",
			"--preserve-perms",
		},
		devices...)
	args = append(args, image)
	if err = runImageTool(
		ctx,
		check,
		reads,
		"fsck.erofs",
		args...); err != nil {
		return err
	}
	root, err := os.OpenRoot(filepath.Join(check, "tree"))
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	for _, required := range []string{"boot/vmlinux", "boot/initramfs", "usr/lib/systemd/systemd", "usr/local/libexec/sandbox-agent", "etc/systemd/system/sysinit.target.wants/sandbox-agent.service"} {
		st, x := root.Stat(required)
		if x != nil || !st.Mode().IsRegular() || st.Size() == 0 {
			return api.Fail(
				api.Incompatible,
				"image boot contract missing %s",
				required,
			)
		}
	}
	for _, item := range []struct{ src, dst string }{{"boot/vmlinux", "kernel"}, {"boot/initramfs", "initramfs"}} {
		st, x := root.Lstat(item.src)
		if x != nil || !st.Mode().IsRegular() {
			return api.Fail(
				api.Incompatible,
				"boot materials must be regular nonsymlink files",
			)
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
