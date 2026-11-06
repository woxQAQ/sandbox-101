package engine

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/woxQAQ/sandbox-engine/api"
)

const (
	LayeredEROFSLayout  = "layered-erofs-v1"
	MaxImageDataDevices = 16
	// Changing the supported tar subset, flags, root policy or block mapping
	// requires a new profile. Exact tool versions also participate in the key.
	layerProfile = "erofs-native-rebuild-v2;unix-paths;tar=f;aufs;4096;noinline_data;workers=1;uuid=diff_id;mkfs-time=0;mtime=preserve;root=0:0:0777:0;explicit-parents;backward-regular-hardlinks;ovlfs-strip=1"
)

// ImageArtifact paths are relative to the engine state directory. DeviceID is
// zero for fsmeta and 1-based for device_table slots, in OCI bottom-to-top order.
// CacheSHA256 authenticates the data device's adjacent layer.json receipt.
type ImageArtifact struct {
	Role        string `json:"role"`
	Path        string `json:"path"`
	SHA256      string `json:"sha256"`
	DiffID      string `json:"diff_id,omitempty"`
	DeviceID    int    `json:"device_id"`
	GuestPath   string `json:"guest_path"`
	Serial      string `json:"serial"`
	CacheSHA256 string `json:"cache_sha256,omitempty"`
}

// ImageBinding describes a verified host file to bind read-only into the VMM
// namespace and attach as a readonly raw block device. Order is significant.
type ImageBinding struct {
	HostPath  string
	GuestPath string
	Serial    string
	SHA256    string
	Role      string
	DiffID    string
	DeviceID  int
}

func (m ImageManifest) ReadOnlyBindings(
	imageDir string,
) ([]ImageBinding, error) {
	if err := m.Verify(imageDir, m.Digest); err != nil {
		return nil, err
	}
	if m.Layout != LayeredEROFSLayout {
		return []ImageBinding{
			{
				HostPath:  filepath.Join(imageDir, "root.erofs"),
				GuestPath: "/images/root.erofs",
				Serial:    "sandbox-root",
				SHA256:    m.Files["root.erofs"],
				Role:      "merged",
			},
		}, nil
	}
	state := filepath.Dir(filepath.Dir(imageDir))
	bindings := make([]ImageBinding, 0, len(m.Artifacts))
	for _, a := range m.Artifacts {
		bindings = append(
			bindings,
			ImageBinding{
				HostPath:  filepath.Join(state, a.Path),
				GuestPath: a.GuestPath,
				Serial:    a.Serial,
				SHA256:    a.SHA256,
				Role:      a.Role,
				DiffID:    a.DiffID,
				DeviceID:  a.DeviceID,
			},
		)
	}
	return bindings, nil
}

// DataSerials is the ordered external-device list for initramfs device= options.
// It validates the manifest structure; ReadOnlyBindings verifies the files too.
func (m ImageManifest) DataSerials() (string, error) {
	if m.Layout == "merged-erofs-v1" && m.Format == 1 &&
		len(m.Artifacts) == 0 &&
		m.Builder == "" {
		return "", nil
	}
	if m.Layout == MergedOCIEROFSLayout {
		return "", m.validateMergedOCI()
	}
	if err := m.validateLayered(); err != nil {
		return "", err
	}
	serials := make([]string, 0, len(m.DiffIDs))
	for _, a := range m.Artifacts[1:] {
		serials = append(serials, a.Serial)
	}
	return strings.Join(serials, ","), nil
}

func (m ImageManifest) DataKernelParameter() (string, error) {
	s, err := m.DataSerials()
	if err != nil || s == "" {
		return "", err
	}
	return "sandbox.data_serials=" + s, nil
}

func imageBuilder(tools map[string]string) string {
	profile := layerProfile
	if tools["go-erofs"] != "" {
		profile = sharedLayerProfile + "\n" + tools["go-erofs"]
	}
	sum := sha256.Sum256(
		[]byte(
			profile + "\n" + tools["mkfs.erofs"] + "\n" + tools["fsck.erofs"],
		),
	)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func layerRelativePath(builder, diff string) string {
	return filepath.Join("layer-cache", builder[7:], diff[7:], "layer.erofs")
}

func layerUUID(diff string) string {
	s := diff[7:39]
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}

func dataArtifact(
	builder, diff, hash, receipt string,
	index int,
) ImageArtifact {
	return ImageArtifact{
		Role:        "layer",
		Path:        layerRelativePath(builder, diff),
		SHA256:      hash,
		DiffID:      diff,
		DeviceID:    index + 1,
		GuestPath:   fmt.Sprintf("/sandbox/ro/data-%04d.erofs", index),
		Serial:      fmt.Sprintf("sandbox-data-%04d", index),
		CacheSHA256: receipt,
	}
}

func (m ImageManifest) validateLayered() error {
	bad := func() error {
		return api.Fail(
			api.Incompatible,
			"invalid layered EROFS materialization identity/order",
		)
	}
	if m.Format != 2 || m.Layout != LayeredEROFSLayout ||
		m.Architecture != runtime.GOARCH ||
		len(m.DiffIDs) < 1 ||
		len(m.DiffIDs) > MaxImageDataDevices ||
		len(m.Layers) != len(m.DiffIDs) ||
		len(m.Artifacts) != len(m.DiffIDs)+1 {
		return bad()
	}
	name, err := shaName(m.Digest)
	if err != nil {
		return bad()
	}
	if _, err = shaName(
		m.Builder,
	); err != nil || m.Builder != imageBuilder(m.Tools) || m.Tools["go-erofs"] != "" && m.Tools["go-erofs"] != sharedEROFSVersion || !supportedEROFSVersion(m.Tools["mkfs.erofs"]) || !supportedEROFSVersion(m.Tools["fsck.erofs"]) ||
		erofsVersion.FindString(
			m.Tools["mkfs.erofs"],
		) != erofsVersion.FindString(
			m.Tools["fsck.erofs"],
		) {
		return bad()
	}
	if len(m.Files) != 3 {
		return bad()
	}
	for _, file := range []string{"kernel", "initramfs", "fsmeta.erofs"} {
		if _, err = shaName(m.Files[file]); err != nil {
			return bad()
		}
	}
	meta := ImageArtifact{
		Role:      "fsmeta",
		Path:      filepath.Join("images", name, "fsmeta.erofs"),
		SHA256:    m.Files["fsmeta.erofs"],
		GuestPath: "/sandbox/ro/fsmeta.erofs",
		Serial:    "sandbox-root",
	}
	if m.Artifacts[0] != meta {
		return bad()
	}
	for i, diff := range m.DiffIDs {
		a := m.Artifacts[i+1]
		for _, value := range []string{diff, m.Layers[i], a.SHA256, a.CacheSHA256} {
			if _, err = shaName(value); err != nil {
				return bad()
			}
		}
		if a != dataArtifact(m.Builder, diff, a.SHA256, a.CacheSHA256, i) {
			return bad()
		}
	}
	return nil
}

func (m ImageManifest) verifyLayered(dir, want string) error {
	if m.Digest != want {
		return api.Fail(api.Incompatible, "image digest mismatch")
	}
	if err := m.validateLayered(); err != nil {
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
	for _, p := range []string{filepath.Dir(dir), dir} {
		st, err := os.Lstat(p)
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
	return m.verifyLayeredArtifacts(filepath.Dir(filepath.Dir(dir)), dir)
}

func (m ImageManifest) verifyLayeredArtifacts(state, imageDir string) error {
	if err := m.validateLayered(); err != nil {
		return err
	}
	for rel, hash := range m.Files {
		if err := verifyImmutable(imageDir, rel, hash); err != nil {
			return err
		}
	}
	var layers []string
	verified := map[string]ImageArtifact{}
	for _, a := range m.Artifacts[1:] {
		if previous, ok := verified[a.DiffID]; ok {
			if previous.SHA256 != a.SHA256 ||
				previous.CacheSHA256 != a.CacheSHA256 {
				return api.Fail(
					api.Incompatible,
					"repeated layer cache identity mismatch",
				)
			}
		} else {
			if err := verifyImmutable(
				state,
				filepath.Join(filepath.Dir(a.Path), "layer.json"),
				a.CacheSHA256,
			); err != nil {
				return err
			}
			cached, err := loadLayerCache(state, m.Builder, a.DiffID)
			if err != nil {
				return err
			}
			if cached.SHA256 != a.SHA256 ||
				(cached.Profile == sharedLayerProfile) != m.sharedLayered() {
				return api.Fail(
					api.Incompatible,
					"layer cache receipt mismatch",
				)
			}
			verified[a.DiffID] = a
		}
		layers = append(layers, filepath.Join(state, a.Path))
	}
	return verifyImageDeviceTable(
		filepath.Join(imageDir, "fsmeta.erofs"),
		layers,
		m.DiffIDs,
		m.sharedLayered(),
	)
}

// Refuse symlink components and writable artifacts, not only a final symlink.
// The daemon owns the private state directory and serializes all mutations.
func verifyImmutable(root, rel, want string) error {
	if filepath.IsAbs(rel) || rel == "." || filepath.Clean(rel) != rel ||
		strings.HasPrefix(rel, "../") ||
		strings.Contains(rel, "\\") {
		return api.Fail(api.Incompatible, "invalid immutable artifact path")
	}
	file := root
	parts := strings.Split(rel, string(filepath.Separator))
	for i, p := range parts {
		file = filepath.Join(file, p)
		st, err := os.Lstat(file)
		if err != nil {
			return err
		}
		if i < len(parts)-1 {
			if !st.IsDir() {
				return api.Fail(
					api.Incompatible,
					"artifact traverses non-directory",
				)
			}
		} else if !st.Mode().IsRegular() || st.Mode().Perm()&0o222 != 0 || st.Size() == 0 {
			return api.Fail(
				api.Incompatible,
				"artifact must be nonempty, regular and readonly",
			)
		}
	}
	got, err := digestFile(file)
	if err == nil && got != want {
		err = api.Fail(
			api.Incompatible,
			"immutable artifact checksum mismatch: %s",
			rel,
		)
	}
	return err
}

// This is a bounded format reader, never an EROFS writer. Native mkfs owns all
// inodes, xattrs and chunk indexes; native fsck checks them before publication.
func erofsSuper(file string) ([]byte, int64, error) {
	if err := regular(file); err != nil {
		return nil, 0, err
	}
	f, err := os.Open(file)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	sb := make([]byte, 128)
	if _, err = f.ReadAt(sb, 1024); err != nil {
		return nil, 0, err
	}
	if binary.LittleEndian.Uint32(sb) != 0xe0f5e1e2 || sb[12] != 12 ||
		sb[13] != 0 ||
		st.Size()%4096 != 0 {
		return nil, 0, api.Fail(
			api.Incompatible,
			"unsupported EROFS superblock",
		)
	}
	return sb, st.Size(), nil
}

func verifyDeviceTable(meta string, layers, diffs []string) error {
	return verifyImageDeviceTable(meta, layers, diffs, false)
}

func verifyImageDeviceTable(
	meta string,
	layers, diffs []string,
	shared bool,
) error {
	sb, size, err := erofsSuper(meta)
	if err != nil {
		return err
	}
	bad := func() error { return api.Fail(api.Incompatible, "EROFS device_table/order/block identity mismatch") }
	// The materializer never emits 48-bit, compressed or alternate-block layouts.
	features := binary.LittleEndian.Uint32(sb[80:])
	if features&0x8 == 0 || features & ^uint32(0xc) != 0 ||
		int(binary.LittleEndian.Uint16(sb[86:])) != len(layers) ||
		binary.LittleEndian.Uint64(sb[16:]) <= 1 {
		return bad()
	}
	offset := int64(binary.LittleEndian.Uint16(sb[88:])) * 128
	if offset < 1152 || offset+int64(len(layers))*128 > size {
		return bad()
	}
	f, err := os.Open(meta)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	mapped := uint64(size / 4096)
	for i, layer := range layers {
		lsb, layerSize, x := erofsSuper(layer)
		if x != nil {
			return x
		}
		if binary.LittleEndian.Uint16(lsb[86:]) != 0 ||
			binary.LittleEndian.Uint32(lsb[80:]) != 0 ||
			uint64(
				binary.LittleEndian.Uint32(lsb[36:]),
			) != uint64(
				layerSize/4096,
			) ||
			hex.EncodeToString(lsb[48:64]) != diffs[i][7:39] {
			return api.Fail(
				api.Incompatible,
				"EROFS layer block identity mismatch: layer=%d diff=%s",
				i,
				diffs[i],
			)
		}
		var slot [128]byte
		if _, err = f.ReadAt(slot[:], offset+int64(i)*128); err != nil {
			return err
		}
		wantTag := make([]byte, 64)
		wantMapped := mapped
		if shared {
			// go-erofs emits explicit device IDs, zero UUID/tags and no
			// concatenated-address mapping. The catalog binds device identity.
			wantMapped = 0
			if !bytes.Equal(sb[48:64], make([]byte, 16)) {
				return bad()
			}
		} else {
			copy(wantTag, diffs[i][7:39])
		}
		if !bytes.Equal(slot[:64], wantTag) ||
			uint64(
				binary.LittleEndian.Uint32(slot[64:]),
			) != uint64(
				layerSize/4096,
			) ||
			uint64(binary.LittleEndian.Uint32(slot[68:])) != wantMapped ||
			!bytes.Equal(slot[72:], make([]byte, 56)) {
			return api.Fail(
				api.Incompatible,
				"EROFS device_table slot mismatch: slot=%d blocks=%d mapped=%d",
				i,
				layerSize/4096,
				mapped,
			)
		}
		mapped += uint64(layerSize / 4096)
	}
	if uint64(size/4096) != uint64(binary.LittleEndian.Uint32(sb[36:])) ||
		mapped > 1<<32-1 {
		return bad()
	}
	return nil
}

//nolint:staticcheck // OCI input validation includes legacy tar headers and their xattr representation.
func validateLayerEntry(
	h *tar.Header,
	entries map[string]byte,
	whiteouts map[string]bool,
) error {
	unsupported := func(reason string) error {
		return api.Fail(
			api.Incompatible,
			"unsupported OCI layer entry %q: %s",
			h.Name,
			reason,
		)
	}
	n := path.Clean(h.Name)
	if _, exists := entries[n]; exists {
		return unsupported("duplicate path")
	}
	// Rebuild currently synthesizes its root instead of importing its metadata.
	if n == "." {
		if h.Typeflag != tar.TypeDir || h.Mode != 0o777 || h.Uid != 0 ||
			h.Gid != 0 ||
			h.ModTime.Unix() != 0 ||
			h.ModTime.Nanosecond() != 0 ||
			len(h.Xattrs) != 0 ||
			len(h.PAXRecords) != 0 {
			return unsupported(
				"root metadata must match synthetic 0:0 0777 mtime=0 root",
			)
		}
	}
	if h.Uid < 0 || uint64(h.Uid) > 1<<32-1 || h.Gid < 0 ||
		uint64(h.Gid) > 1<<32-1 ||
		h.Mode < 0 ||
		h.Mode & ^int64(0o7777) != 0 ||
		h.ModTime.Unix() < 0 {
		return unsupported("metadata outside supported range")
	}
	if parent := path.Dir(n); parent != "." && entries[parent] != tar.TypeDir {
		return unsupported(
			"all nonroot parent directories must appear earlier in the layer",
		)
	}
	if h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA && h.Size != 0 {
		return unsupported("nonregular entry has data")
	}
	for key := range h.PAXRecords {
		if after, ok := strings.CutPrefix(key, "SCHILY.xattr."); ok {
			if !supportedLayerXattr(after) {
				return unsupported("unsupported xattr namespace")
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
			return unsupported("unsupported PAX attribute " + key)
		}
	}
	for key := range h.Xattrs {
		if !supportedLayerXattr(key) {
			return unsupported("unsupported xattr namespace")
		}
	}
	kind := h.Typeflag
	if kind == tar.TypeRegA {
		kind = tar.TypeReg
	}
	if kind == tar.TypeLink {
		if entries[path.Clean(h.Linkname)] != tar.TypeReg {
			return unsupported(
				"cross-layer, forward or nonregular hardlink target",
			)
		}
		kind = tar.TypeReg
	}
	if base := path.Base(n); strings.HasPrefix(base, ".wh.") {
		victim := strings.TrimPrefix(base, ".wh.")
		if base == ".wh..wh..opq" {
			if path.Dir(n) == "." {
				return unsupported(
					"root opaque marker is not supported by native rebuild",
				)
			}
		} else if victim == "" || victim == "." || victim == ".." {
			return unsupported("invalid whiteout target")
		} else {
			victim = path.Join(path.Dir(n), victim)
			if _, exists := entries[victim]; exists {
				return unsupported("whiteout and replacement in the same layer")
			}
			whiteouts[victim] = true
		}
		kind = tar.TypeChar // internal marker; cannot be a hardlink target
	} else if whiteouts[n] {
		return unsupported("whiteout and replacement in the same layer")
	}
	entries[n] = kind
	return nil
}

func supportedLayerXattr(name string) bool {
	return strings.HasPrefix(name, "user.") && len(name) > len("user.") &&
		!strings.HasPrefix(name, "user.overlay.") ||
		name == "security.capability" ||
		name == "system.posix_acl_access" ||
		name == "system.posix_acl_default"
}
