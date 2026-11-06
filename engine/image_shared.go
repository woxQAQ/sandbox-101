package engine

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	erofs "github.com/erofs/go-erofs"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/woxQAQ/sandbox-engine/api"
)

const (
	sharedEROFSVersion = "github.com/erofs/go-erofs@v0.3.3-0.20261101155608-4c135619312a+sandbox-devtable-v1"
	sharedLayerProfile = "erofs-shared-oci-v1;unix-paths;payloads=all-regular-entry-versions;headers=gob-sorted-tar.Header;4096;noinline_data;workers=1;uuid=diff_id;mkfs-time=0;mtime=preserve;fsmeta=go-erofs-metadataonly;whiteouts=before-entries;hardlinks=ordered-deferred;parents=inherit;symlinks=nofollow"
)

func (m ImageManifest) sharedLayered() bool { return m.Tools["go-erofs"] == sharedEROFSVersion }

// The cache keeps every regular entry version, including a version replaced
// later in the same tar. Headers retain OCI operations separately from payload
// names, so a cache never has to resolve a lower-layer or forward hardlink.
// Native mkfs owns payload extents; the library owns final inode/chunk encoding.
func buildSharedLayerCache(ctx context.Context, state, input, media, builder, diff string, size, entries int64) (layerCache, error) {
	cached := layerCache{Format: 1, Profile: sharedLayerProfile, Builder: builder, DiffID: diff, Bytes: size, Entries: entries}
	parent := filepath.Join(state, "layer-cache", builder[7:])
	if err := os.MkdirAll(parent, 0700); err != nil {
		return cached, err
	}
	stage, err := os.MkdirTemp(parent, ".pending-")
	if err != nil {
		return cached, err
	}
	defer os.RemoveAll(stage)
	normalized := filepath.Join(stage, "payload.tar")
	if err = normalizeLayerPayloads(input, media, normalized); err != nil {
		return cached, err
	}
	args := []string{"--quiet", "--workers=1", "--tar=f", "-b4096", "-E", "noinline_data", "-T0", "--mkfs-time", "--preserve-mtime", "-U", layerUUID(diff), "/work/layer.erofs", "/work/payload.tar"}
	if err = runImageTool(ctx, stage, nil, "mkfs.erofs", args...); err != nil {
		return cached, err
	}
	if err = runImageTool(ctx, stage, nil, "fsck.erofs", "--extract", "/work/layer.erofs"); err != nil {
		return cached, err
	}
	if err = os.Remove(normalized); err != nil {
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
	if _, err = os.Lstat(filepath.Join(parent, diff[7:])); !os.IsNotExist(err) {
		return cached, api.Fail(api.Incompatible, "layer cache destination already exists")
	}
	if err = os.Rename(stage, filepath.Join(parent, diff[7:])); err != nil {
		return cached, err
	}
	for _, dir := range []string{parent, filepath.Dir(parent), state} {
		if err = syncDir(dir); err != nil {
			return cached, err
		}
	}
	return loadLayerCache(state, builder, diff)
}

func normalizeLayerPayloads(input, media, output string) error {
	f, err := os.Open(input)
	if err != nil {
		return err
	}
	defer f.Close()
	var r io.Reader = f
	if media == v1.MediaTypeImageLayerGzip || media == "application/vnd.docker.image.rootfs.diff.tar.gzip" {
		g, x := gzip.NewReader(f)
		if x != nil {
			return x
		}
		defer g.Close()
		r = g
	}
	out, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer out.Close()
	tw := tar.NewWriter(out)
	if err = tw.WriteHeader(&tar.Header{Name: "data", Typeflag: tar.TypeDir, Mode: 0755, ModTime: time.Unix(0, 0)}); err != nil {
		return err
	}
	var headers []cachedHeader
	tr := tar.NewReader(r)
	for {
		h, x := tr.Next()
		if x == io.EOF {
			break
		}
		if x != nil {
			return x
		}
		headers = append(headers, cacheHeader(*h))
		if h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA || strings.HasPrefix(path.Base(h.Name), ".wh.") {
			continue
		}
		payload := *h
		payload.Name = payloadName(len(headers) - 1)
		payload.PAXRecords = nil // tar.Writer regenerates path/time/xattr PAX fields.
		if err = tw.WriteHeader(&payload); err != nil {
			return err
		}
		if _, err = io.Copy(tw, tr); err != nil {
			return err
		}
	}
	// gob preserves arbitrary xattr bytes in tar.Header strings. JSON would
	// silently replace non-UTF8 capability and ACL bytes.
	var metadata bytes.Buffer
	if err = gob.NewEncoder(&metadata).Encode(headers); err != nil {
		return err
	}
	if err = tw.WriteHeader(&tar.Header{Name: "headers.gob", Typeflag: tar.TypeReg, Mode: 0400, Size: int64(metadata.Len()), ModTime: time.Unix(0, 0)}); err != nil {
		return err
	}
	if _, err = metadata.WriteTo(tw); err != nil {
		return err
	}
	if err = tw.Close(); err != nil {
		return err
	}
	return out.Close()
}

func payloadName(i int) string { return fmt.Sprintf("data/%08x", i) }

type headerAttribute struct{ Name, Value string }
type cachedHeader struct {
	Header tar.Header
	Xattrs []headerAttribute
	PAX    []headerAttribute
}

func sortedHeaderAttributes(m map[string]string) []headerAttribute {
	var attrs []headerAttribute
	for k, v := range m {
		attrs = append(attrs, headerAttribute{k, v})
	}
	sort.Slice(attrs, func(i, j int) bool { return attrs[i].Name < attrs[j].Name })
	return attrs
}

func cacheHeader(h tar.Header) cachedHeader {
	r := cachedHeader{Header: h, Xattrs: sortedHeaderAttributes(h.Xattrs), PAX: sortedHeaderAttributes(h.PAXRecords)}
	r.Header.Xattrs, r.Header.PAXRecords = nil, nil
	return r
}

func restoreHeaderAttributes(attrs []headerAttribute) (map[string]string, error) {
	if len(attrs) == 0 {
		return nil, nil
	}
	m := make(map[string]string, len(attrs))
	for i, a := range attrs {
		if i > 0 && attrs[i-1].Name >= a.Name {
			return nil, api.Fail(api.Incompatible, "noncanonical cached OCI attributes")
		}
		m[a.Name] = a.Value
	}
	return m, nil
}

func sharedHeaders(img fs.FS, receipt layerCache) ([]tar.Header, error) {
	f, err := img.Open("headers.gob")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	limit := receipt.Bytes*4 + receipt.Entries*4096 + 65536
	if st.Size() <= 0 || st.Size() > limit {
		return nil, api.Fail(api.Incompatible, "cached OCI index exceeds authenticated bounds")
	}
	dec := gob.NewDecoder(io.LimitReader(f, limit))
	var records []cachedHeader
	if err = dec.Decode(&records); err != nil {
		return nil, err
	}
	if int64(len(records)) != receipt.Entries {
		return nil, api.Fail(api.Incompatible, "cached OCI entry count mismatch")
	}
	var extra any
	if err = dec.Decode(&extra); err != io.EOF {
		return nil, api.Fail(api.Incompatible, "trailing cached OCI index data")
	}
	headers := make([]tar.Header, len(records))
	for i, r := range records {
		h := r.Header
		if h.Xattrs != nil || h.PAXRecords != nil || h.Size < 0 || h.Size > receipt.Bytes {
			return nil, api.Fail(api.Incompatible, "invalid cached OCI header")
		}
		if err = tarPath(h.Name); err != nil {
			return nil, err
		}
		switch h.Typeflag {
		case tar.TypeReg, tar.TypeRegA, tar.TypeDir, tar.TypeSymlink:
		case tar.TypeLink:
			if err = tarPath(h.Linkname); err != nil {
				return nil, err
			}
		default:
			return nil, api.Fail(api.Incompatible, "unsupported cached OCI file type")
		}
		if h.Xattrs, err = restoreHeaderAttributes(r.Xattrs); err != nil {
			return nil, err
		}
		if h.PAXRecords, err = restoreHeaderAttributes(r.PAX); err != nil {
			return nil, err
		}
		if err = validateMergedOCIEntry(&h); err != nil {
			return nil, err
		}
		if strings.HasPrefix(path.Base(h.Name), ".wh.") && (h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA || h.Size != 0) {
			return nil, api.Fail(api.Incompatible, "invalid cached OCI whiteout")
		}
		headers[i] = h
	}
	return headers, nil
}

// metadataFS adapts already parsed metadata to the library's public fs.FS
// interface. It never extracts archive paths or writes EROFS structures.
// Open is deliberately unavailable: MetadataOnly must not copy file data.
type metadataFS struct {
	nodes  map[string]fs.FileInfo
	blocks uint64
}

type metadataInfo struct {
	fs.FileInfo
	name string
	sys  any
}

func (i metadataInfo) Name() string { return i.name }
func (i metadataInfo) Sys() any {
	if i.sys != nil {
		return i.sys
	}
	return i.FileInfo.Sys()
}
func (i metadataInfo) DataRange() []erofs.DataRange {
	if r, ok := i.FileInfo.(interface{ DataRange() []erofs.DataRange }); ok {
		return r.DataRange()
	}
	return nil
}
func (m metadataFS) Open(n string) (fs.File, error) {
	return nil, &fs.PathError{Op: "open", Path: n, Err: fs.ErrPermission}
}
func (m metadataFS) Stat(n string) (fs.FileInfo, error) {
	if i, ok := m.nodes[n]; ok {
		return i, nil
	}
	return nil, &fs.PathError{Op: "stat", Path: n, Err: fs.ErrNotExist}
}
func (m metadataFS) ReadDir(n string) ([]fs.DirEntry, error) {
	var entries []fs.DirEntry
	for p, i := range m.nodes {
		if p != "." && path.Dir(p) == n {
			entries = append(entries, fs.FileInfoToDirEntry(i))
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}
func (m metadataFS) BlockSize() uint32    { return 4096 }
func (m metadataFS) DeviceBlocks() uint64 { return m.blocks }

func currentMetadata(w *erofs.Writer, n string) (fs.FileInfo, error) {
	info, err := w.Stat(n)
	if err != nil {
		return nil, err
	}
	// CopyFrom accepts its public on-disk Stat metadata; WriterStat is a
	// separate view and otherwise loses ownership, timestamps and xattrs.
	st := info.Sys().(*erofs.WriterStat)
	return metadataInfo{FileInfo: info, name: path.Base(n), sys: &erofs.Stat{UID: st.UID, GID: st.GID, Mtime: st.Mtime, MtimeNs: st.MtimeNs, Nlink: int(st.Nlink), Xattrs: st.Xattrs}}, nil
}

func materializeSharedFsmeta(ctx context.Context, state, out string, artifacts []ImageArtifact, builder string) error {
	f, err := os.OpenFile(filepath.Join(out, "fsmeta.erofs"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	w := erofs.Create(f, erofs.WithBlockSize(4096), erofs.WithBuildTime(0, 0))
	var reads []imageMount
	var layers, diffs, devices []string
	for i, a := range artifacts {
		if err = ctx.Err(); err != nil {
			return err
		}
		file := filepath.Join(state, a.Path)
		layer, x := os.Open(file)
		if x != nil {
			return x
		}
		defer layer.Close()
		img, x := erofs.Open(layer)
		if x != nil {
			return x
		}
		receipt, x := loadLayerCache(state, builder, a.DiffID)
		if x != nil {
			return x
		}
		headers, x := sharedHeaders(img, receipt)
		if x != nil {
			return x
		}
		// Whiteouts operate only on the view preceding this layer, regardless
		// of their position relative to same-layer replacement entries.
		if err = applySharedWhiteouts(w, headers); err != nil {
			return err
		}
		prefix := ".sandbox-payload-"
		for {
			_, x = w.Stat(prefix)
			collision := !errors.Is(x, fs.ErrNotExist)
			for _, h := range headers {
				collision = collision || strings.HasPrefix(path.Clean(h.Name), prefix) || strings.HasPrefix(path.Clean(h.Linkname), prefix)
			}
			if !collision {
				break
			}
			prefix += "x"
		}
		root, x := currentMetadata(w, ".")
		if x != nil {
			return x
		}
		st, x := layer.Stat()
		if x != nil {
			return x
		}
		source := metadataFS{nodes: map[string]fs.FileInfo{".": root}, blocks: uint64(st.Size() / 4096)}
		for j, h := range headers {
			if (h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA) || strings.HasPrefix(path.Base(h.Name), ".wh.") {
				continue
			}
			info, x := fs.Stat(img, payloadName(j))
			if x != nil {
				return x
			}
			if !info.Mode().IsRegular() || info.Size() != h.Size || info.Sys().(*erofs.Stat).InodeLayout != 0 {
				return api.Fail(api.Incompatible, "cached OCI payload geometry mismatch")
			}
			name := fmt.Sprintf("%s%08x", prefix, j)
			source.nodes[name] = metadataInfo{FileInfo: info, name: name}
		}
		if err = w.CopyFrom(source, erofs.MetadataOnly()); err != nil {
			return err
		}
		if err = applySharedEntries(w, headers, prefix); err != nil {
			return err
		}
		for n := range source.nodes {
			if n != "." {
				if err = w.Remove(n); err != nil {
					return err
				}
			}
		}
		guest := fmt.Sprintf("/data/%04d.erofs", i)
		reads = append(reads, imageMount{file, guest})
		devices = append(devices, "--device="+guest)
		layers, diffs = append(layers, file), append(diffs, a.DiffID)
	}
	// CopyFrom preserves source nlinks; merged directory counts are derived
	// from the final namespace, including implicit parents and replacements.
	if err = fs.WalkDir(w, ".", func(n string, d fs.DirEntry, x error) error {
		if x != nil || !d.IsDir() {
			return x
		}
		children, x := fs.ReadDir(w, n)
		if x != nil {
			return x
		}
		links := uint32(2)
		for _, child := range children {
			if child.IsDir() {
				links++
			}
		}
		return w.SetNlink(n, links)
	}); err != nil {
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = verifyImageDeviceTable(filepath.Join(out, "fsmeta.erofs"), layers, diffs, true); err != nil {
		return err
	}
	reads = append(reads, imageMount{filepath.Join(out, "fsmeta.erofs"), "/fsmeta.erofs"})
	return extractImageBoot(ctx, out, reads, devices, "/fsmeta.erofs")
}

func applySharedWhiteouts(w *erofs.Writer, headers []tar.Header) error {
	for _, h := range headers {
		n := path.Clean(h.Name)
		base := path.Base(n)
		if !strings.HasPrefix(base, ".wh.") {
			continue
		}
		if base != ".wh..wh..opq" {
			if err := w.RemoveAll(path.Join(path.Dir(n), strings.TrimPrefix(base, ".wh."))); err != nil {
				return err
			}
			continue
		}
		children, err := fs.ReadDir(w, path.Dir(n))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		for _, child := range children {
			if err = w.RemoveAll(path.Join(path.Dir(n), child.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func sharedParents(w *erofs.Writer, n string) error {
	parent := path.Dir(n)
	if parent == "." {
		return nil
	}
	if err := sharedParents(w, parent); err != nil {
		return err
	}
	info, err := w.Stat(parent)
	if err == nil {
		if !info.IsDir() {
			return api.Fail(api.Invalid, "OCI path traverses nondirectory %q", parent)
		}
		return nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	// The shared profile fixes missing-directory metadata by inheriting the
	// nearest parent's owner, permission and timestamp until an explicit header.
	ancestor, err := w.Stat(path.Dir(parent))
	if err != nil {
		return err
	}
	owner := ancestor.Sys().(*erofs.WriterStat)
	if err = w.Mkdir(parent, ancestor.Mode()); err != nil {
		return err
	}
	if err = w.Chown(parent, int(owner.UID), int(owner.GID)); err != nil {
		return err
	}
	return w.Chtimes(parent, ancestor.ModTime(), ancestor.ModTime())
}

func sharedDirectory(w *erofs.Writer, h tar.Header) error {
	n := path.Clean(h.Name)
	if info, err := w.Stat(n); err == nil && !info.IsDir() {
		if err = w.RemoveAll(n); err != nil {
			return err
		}
	}
	root, err := currentMetadata(w, ".")
	if err != nil {
		return err
	}
	nodes := map[string]fs.FileInfo{".": root}
	for p := path.Dir(n); p != "."; p = path.Dir(p) {
		info, x := currentMetadata(w, p)
		if x != nil {
			return x
		}
		nodes[p] = info
	}
	nodes[n] = metadataInfo{FileInfo: h.FileInfo(), name: path.Base(n), sys: &erofs.Stat{UID: uint32(h.Uid), GID: uint32(h.Gid), Mtime: uint64(h.ModTime.Unix()), MtimeNs: uint32(h.ModTime.Nanosecond()), Xattrs: h.Xattrs}}
	return w.CopyFrom(metadataFS{nodes: nodes})
}

func applySharedEntries(w *erofs.Writer, headers []tar.Header, prefix string) error {
	pending := map[string]string{}
	resolve := func() error {
		for {
			progress := false
			for n, target := range pending {
				if _, waiting := pending[target]; waiting {
					continue
				}
				if _, err := w.Stat(target); errors.Is(err, fs.ErrNotExist) {
					continue
				} else if err != nil {
					return err
				}
				if err := w.Link(target, n); err != nil {
					return err
				}
				delete(pending, n)
				progress = true
			}
			if !progress {
				return nil
			}
		}
	}
	for j, h := range headers {
		n := path.Clean(h.Name)
		if strings.HasPrefix(path.Base(n), ".wh.") {
			continue
		}
		if err := sharedParents(w, n); err != nil {
			return err
		}
		delete(pending, n)
		if h.Typeflag == tar.TypeDir {
			if err := sharedDirectory(w, h); err != nil {
				return err
			}
		} else {
			if h.Typeflag == tar.TypeLink && path.Clean(h.Linkname) == n {
				info, err := w.Stat(n)
				if err != nil {
					return err
				}
				if info.IsDir() {
					return api.Fail(api.Invalid, "OCI hardlink targets a directory")
				}
				continue
			}
			// A later replacement also removes not-yet-resolved names below
			// the old directory. They must not reappear when their target arrives.
			for p := range pending {
				if strings.HasPrefix(p, n+"/") {
					delete(pending, p)
				}
			}
			if err := w.RemoveAll(n); err != nil {
				return err
			}
			switch h.Typeflag {
			case tar.TypeReg, tar.TypeRegA:
				if err := w.Link(fmt.Sprintf("%s%08x", prefix, j), n); err != nil {
					return err
				}
			case tar.TypeLink:
				pending[n] = path.Clean(h.Linkname)
			case tar.TypeSymlink:
				if err := w.Symlink(h.Linkname, n); err != nil {
					return err
				}
				if err := w.Chown(n, h.Uid, h.Gid); err != nil {
					return err
				}
				if err := w.Chtimes(n, h.ModTime, h.ModTime); err != nil {
					return err
				}
				for k, v := range h.Xattrs {
					if err := w.Setxattr(n, k, v); err != nil {
						return err
					}
				}
			default:
				return api.Fail(api.Incompatible, "unsupported cached OCI entry %q", n)
			}
		}
		if err := resolve(); err != nil {
			return err
		}
	}
	if len(pending) != 0 {
		return api.Fail(api.Invalid, "unresolved or cyclic OCI hardlinks")
	}
	return nil
}
