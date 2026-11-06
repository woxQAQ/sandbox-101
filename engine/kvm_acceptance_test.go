//go:build linux && kvm

package engine

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/structpb"

	protocol "github.com/woxQAQ/sandbox-protocol/gen"
)

// This acceptance test runs the actual pinned VMM and guest, independently of
// the engine's privileged cgroup/network launcher. It requires working KVM and
// native image tools; selecting the build tag never silently skips prerequisites.
func TestKVMOCIFileViewAndCOW(t *testing.T) {
	if os.Getuid() == 0 {
		t.Fatal(
			"run this standalone KVM acceptance as an unprivileged device-authorized user",
		)
	}
	hypervisor := kvmRequired(t, "SANDBOX_KVM_HYPERVISOR")
	kernel := kvmRequired(t, "SANDBOX_KVM_KERNEL")
	guest := kvmRequired(t, "SANDBOX_KVM_GUEST_DIR")
	systemd := kvmRequired(t, "SANDBOX_KVM_SYSTEMD")
	shell := kvmRequired(t, "SANDBOX_KVM_SHELL")
	coreutils := kvmRequired(t, "SANDBOX_KVM_COREUTILS")
	attr := kvmRequired(t, "SANDBOX_KVM_ATTR")
	acl := kvmRequired(t, "SANDBOX_KVM_ACL")
	artifacts := kvmRequired(t, "SANDBOX_KVM_ARTIFACTS")
	if err := os.MkdirAll(artifacts, 0o700); err != nil {
		t.Fatal(err)
	}
	run, err := os.MkdirTemp(artifacts, "kvm-acceptance-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("persistent acceptance artifacts: %s", run)
	version, err := exec.Command(hypervisor, "--version").CombinedOutput()
	if err != nil || !strings.Contains(string(version), CloudHypervisorCommit) {
		t.Fatalf("wrong pinned hypervisor: %s: %v", version, err)
	}
	kvmTool(t, "readelf", "-l", hypervisor)
	for _, executable := range []string{hypervisor, filepath.Join(guest, "sandbox-agent"), filepath.Join(guest, "sandbox-init")} {
		for _, args := range [][]string{{"-l", executable}, {"-d", executable}} {
			out := kvmTool(t, "readelf", args...)
			if bytes.Contains(out, []byte("INTERP")) ||
				bytes.Contains(out, []byte("NEEDED")) {
				t.Fatalf("acceptance executable is not static: %s", executable)
			}
		}
	}
	layout := filepath.Join(run, "oci")
	if err := os.Mkdir(layout, 0o700); err != nil {
		t.Fatal(err)
	}
	base := kvmRuntimeLayer(
		t,
		layout,
		kernel,
		guest,
		systemd,
		shell,
		coreutils,
		attr,
		acl,
	)
	meta := kvmMetadataLayers(t, layout)
	cross := hardEntry("acceptance/cross-layer-hard", "acceptance/regular")
	cross.header.Mode = 0o644
	root := dirEntry(".")
	root.header.Uid, root.header.Gid, root.header.Mode = 0, 0, 0o755
	root.header.ModTime = time.Unix(1700000001, 123456789)
	root.header.Xattrs = map[string]string{"user.shared-root": "retained"}
	broaderEntries := []tarEntry{
		root,
		cross,
		fileEntry("acceptance/implicit/deep/value", "implicit"),
		fileEntry(
			"acceptance/duplicate",
			"old",
		),
		fileEntry("acceptance/duplicate", "last"),
	}
	for i := range broaderEntries {
		if broaderEntries[i].header.Typeflag == tar.TypeReg {
			broaderEntries[i].header.Mode = 0o644
		}
	}
	broader := putOCILayer(t, layout, broaderEntries, false)
	layers := append(append([]ociLayerFixture{base}, meta...), broader)
	image := putOCIManifest(t, layout, "actual-systemd-cross-layer-kvm", layers)
	for _, materialization := range []string{LayeredEROFSLayout, MergedOCIEROFSLayout} {
		t.Run(materialization, func(t *testing.T) {
			dir := filepath.Join(run, materialization)
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			// The backend is unused: this engine owns only the offline import store.
			store := filepath.Join(dir, "store")
			if err := os.Mkdir(store, 0o700); err != nil {
				t.Fatal(err)
			}
			e := openTest(t, store, newTestBackend(), true, nil)
			defer e.Close()
			ctx, cancel := context.WithTimeout(
				context.Background(),
				8*time.Minute,
			)
			defer cancel()
			image := image
			if materialization == LayeredEROFSLayout {
				// The shared metadata composer also resolves true forward chains,
				// which the explicit umoci compatibility path does not accept.
				forwardEntries := []tarEntry{
					hardEntry("acceptance/forward-a", "acceptance/forward-b"),
					hardEntry(
						"acceptance/forward-b",
						"acceptance/forward-target",
					),
					fileEntry("acceptance/forward-target", "forward"),
				}
				for i := range forwardEntries {
					forwardEntries[i].header.Mode = 0o644
				}
				forward := putOCILayer(t, layout, forwardEntries, false)
				image = putOCIManifest(
					t,
					layout,
					"actual-systemd-forward-kvm",
					append(append([]ociLayerFixture{}, layers...), forward),
				)
			} else {
				// Compatibility pins umask 077, so its implicit parents are
				// private. Explicit headers make this path workload-readable;
				// the shared path above retains its implicit-parent checks.
				directories := []tarEntry{
					dirEntry("acceptance/implicit"),
					dirEntry("acceptance/implicit/deep"),
				}
				for i := range directories {
					directories[i].header.Mode = 0o755
				}
				readable := putOCILayer(t, layout, directories, false)
				image = putOCIManifest(
					t,
					layout,
					"actual-systemd-compat-parents-kvm",
					append(append([]ociLayerFixture{}, layers...), readable),
				)
			}
			manifest, err := e.ImportOCI(
				ctx,
				ImportOptions{
					Layout:          layout,
					Digest:          image,
					Materialization: materialization,
					MaxBytes:        2 << 30,
					MaxFiles:        100000,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			imageDir := filepath.Join(e.opts.StateDir, "images", image[7:])
			bindings, err := manifest.ReadOnlyBindings(imageDir)
			if err != nil {
				t.Fatal(err)
			}
			upper := filepath.Join(dir, "upper.raw")
			file, err := os.OpenFile(
				upper,
				os.O_CREATE|os.O_EXCL|os.O_RDWR,
				0o600,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err = file.Truncate(256 << 20); err != nil {
				t.Fatal(err)
			}
			file.Close()
			kvmTool(t, "mkfs.ext4", "-q", "-F", upper)
			vm := kvmStart(t, hypervisor, filepath.Join(dir, "source"))
			data, err := manifest.DataKernelParameter()
			if err != nil {
				t.Fatal(err)
			}
			disks := make([]map[string]any, 0, len(bindings)+1)
			for i, binding := range bindings {
				disks = append(
					disks,
					map[string]any{
						"id":         fmt.Sprintf("disk%d", i),
						"path":       binding.HostPath,
						"serial":     binding.Serial,
						"readonly":   true,
						"image_type": "Raw",
						"num_queues": 1,
						"queue_size": 128,
					},
				)
			}
			disks = append(
				disks,
				map[string]any{
					"id":         "upper",
					"path":       upper,
					"serial":     "sandbox-write",
					"readonly":   false,
					"image_type": "Raw",
					"num_queues": 1,
					"queue_size": 128,
				},
			)
			vm.api(t, "vm.create", map[string]any{
				"cpus": map[string]any{"boot_vcpus": 1, "max_vcpus": 1},
				"memory": map[string]any{
					"size":      512 << 20,
					"shared":    false,
					"hugepages": false,
					"mergeable": false,
				},
				"payload": map[string]any{
					"kernel":    filepath.Join(imageDir, "kernel"),
					"initramfs": filepath.Join(imageDir, "initramfs"),
					"cmdline":   "console=ttyS0 panic=-1 reboot=k sandbox.lower_serial=sandbox-root sandbox.upper_serial=sandbox-write " + data,
				},
				"disks": disks,
				"vsock": map[string]any{
					"id":     "vsock0",
					"cid":    3,
					"socket": vm.vsock,
				},
				"console": map[string]any{
					"mode": "Null",
				},
				"serial": map[string]any{
					"mode": "File",
					"file": filepath.Join(vm.dir, "serial.log"),
				},
			})
			vm.api(t, "vm.boot", nil)
			vm.ready(t, ctx, false)
			vm.rpc(
				t,
				2046,
				"activate",
				map[string]any{
					"sandbox_id":     "acceptance",
					"network":        "none",
					"env":            map[string]any{},
					"reset_identity": true,
					"hostname":       "acceptance",
				},
				nil,
			)
			fileViewChecks := kvmFileViewChecks
			if materialization == LayeredEROFSLayout {
				fileViewChecks += `
[ "$(cat /acceptance/forward-a)" = forward ]
[ "$(stat -c %i /acceptance/forward-a)" = "$(stat -c %i /acceptance/forward-b)" ]
[ "$(stat -c %i /acceptance/forward-a)" = "$(stat -c %i /acceptance/forward-target)" ]
`
			}
			vm.exec(t, fileViewChecks)
			vm.treeRoundtrip(t)
			vm.exec(t, "printf 'original' > /workspace/branch; sync")
			capture := vm.capture(t, upper, "first")
			clone := kvmRestore(
				t,
				hypervisor,
				filepath.Join(dir, "clone"),
				capture,
			)
			clone.ready(t, ctx, true)
			if err := os.RemoveAll(capture); err != nil {
				t.Fatal(err)
			}
			clone.exec(
				t,
				"test \"$(cat /workspace/branch)\" = original; printf 'clone' > /workspace/branch; sync",
			)
			vm.exec(
				t,
				"test \"$(cat /workspace/branch)\" = original; printf 'source' > /workspace/branch; sync",
			)
			second := clone.capture(
				t,
				filepath.Join(clone.dir, "upper.raw"),
				"second",
			)
			clone.stop()
			vm.stop()
			grandchild := kvmRestore(
				t,
				hypervisor,
				filepath.Join(dir, "grandchild"),
				second,
			)
			grandchild.ready(t, ctx, true)
			grandchild.exec(t, "test \"$(cat /workspace/branch)\" = clone")
			grandchild.exec(t, fileViewChecks)
			t.Log(
				"PASS actual OCI -> EROFS -> initramfs -> systemd -> vsock file view; COW clone, independent disks and COW resnapshot",
			)
		})
	}
}

// Linux can reject readonly block opens with EACCES or writes with EPERM. Prove
// root identity, writable node permissions and raw reads before checking writes.
const kvmRawWriteChecks = `set -eux
export PATH=/bin LC_ALL=C
umask 022
[ "$(id -u)" = 0 ]
count=0
printf '' > /run/acceptance-raw-proof
for serial_file in /sys/block/*/serial; do
  serial=$(cat "$serial_file")
  case "$serial" in sandbox-root|sandbox-data-*) ;; *) continue ;; esac
  block=${serial_file%/serial}
  device=/dev/${block##*/}
  [ -b "$device" ]
  [ -w "$device" ]
  [ "$(cat "$block/ro")" = 1 ]
  dd if="$device" of=/dev/null bs=512 count=1
  if dd if=/dev/zero of="$device" bs=512 count=1 conv=notrunc 2>/run/acceptance-raw-error; then
    exit 1
  fi
  error=$(cat /run/acceptance-raw-error)
  printf '%s\n' "$error" >&2
  case "$error" in
    *'Read-only file system'*) reason=EROFS ;;
    *'Permission denied'*) reason=EACCES ;;
    *'Operation not permitted'*) reason=EPERM ;;
    *) exit 1 ;;
  esac
  printf 'uid=0 serial=%s readonly=1 rejected=%s\n' "$serial" "$reason" >> /run/acceptance-raw-proof
  count=$((count + 1))
done
[ "$count" -gt 0 ]
printf 'guest-root-raw-write-protection-ok\n' >> /run/acceptance-raw-proof
`

const kvmFileViewChecks = `set -eux
export PATH=/bin
[ "$(id -u)" = 1000 ]
[ "$(stat -c '%u:%g:%a' /)" = '0:0:755' ]
[ "$(cat /proc/1/comm)" = systemd ]
raw_proof=$(cat /run/acceptance-raw-proof)
case "$raw_proof" in *guest-root-raw-write-protection-ok*) ;; *) exit 1 ;; esac
printf '%s\n' "$raw_proof"
[ "$(stat -f -c %T /run/sandbox-boot/lower)" = erofs ]
[ "$(stat -f -c %T /)" = overlayfs ]
[ "$(cat /acceptance/regular)" = contents ]
[ "$(stat -c '%u:%g:%a:%Y' /acceptance/regular)" = '123:456:644:1700000000' ]
[ "$(stat -c '%i' /acceptance/regular)" = "$(stat -c '%i' /acceptance/hard)" ]
[ "$(stat -c '%i' /acceptance/regular)" = "$(stat -c '%i' /acceptance/cross-layer-hard)" ]
[ "$(cat /acceptance/implicit/deep/value)" = implicit ]
[ "$(cat /acceptance/duplicate)" = last ]
[ "$(stat -c '%u:%g:%a:%Y' /run/sandbox-boot/lower)" = '0:0:755:1700000001' ]
[ "$(getfattr --only-values -n user.shared-root /run/sandbox-boot/lower 2>/dev/null)" = retained ]
[ "$(readlink /acceptance/symbolic)" = regular ]
[ "$(getfattr --only-values -n user.fixture /acceptance/regular 2>/dev/null)" = retained ]
[ "$(cat /acceptance/replaced)" = top ]
[ ! -e /acceptance/removed ]
[ ! -e /acceptance/opaque/old ]
[ "$(cat /acceptance/opaque/new)" = new ]
[ "$(stat -c '%a' /acceptance/setid)" = 6755 ]
acl=$(getfacl -cpn /acceptance/acl)
case "$acl" in *'user:1000:r--'*) ;; *) exit 1 ;; esac
getfattr --only-values -n security.capability /acceptance/capability >/tmp/capability
[ "$(stat -c %s /tmp/capability)" = 20 ]
[ ! -w /etc ]
[ ! -w /var/lib ]
[ ! -w /run/sandbox-boot/lower/acceptance/regular ]
if printf forbidden >> /run/sandbox-boot/lower/acceptance/regular 2>/dev/null; then exit 1; fi
printf writable > /workspace/file-view-check
[ "$(cat /workspace/file-view-check)" = writable ]
printf 'guest-file-view-ok\n'
`

func kvmRequired(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" || !filepath.IsAbs(value) {
		t.Fatalf("%s must be an absolute configured path", name)
	}
	return value
}

func kvmTool(t *testing.T, tool string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, tool, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v: %s", tool, err, out)
	}
	return out
}

func kvmRuntimeLayer(
	t *testing.T,
	layout, kernel, guest, systemd, shell, coreutils, attr, acl string,
) ociLayerFixture {
	t.Helper()
	closure := strings.Fields(
		string(
			kvmTool(
				t,
				"nix-store",
				"-qR",
				systemd,
				shell,
				coreutils,
				attr,
				acl,
			),
		),
	)
	sort.Strings(closure)
	archive, err := os.CreateTemp(filepath.Dir(layout), "runtime-layer-*.tar")
	if err != nil {
		t.Fatal(err)
	}
	writer := tar.NewWriter(archive)
	seen := map[string]bool{}
	var directory func(string)
	header := func(h *tar.Header, data io.Reader) {
		h.Format = tar.FormatPAX
		if err := writer.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if data != nil {
			if _, err := io.Copy(writer, data); err != nil {
				t.Fatal(err)
			}
		}
	}
	directory = func(name string) {
		if name == "." || seen[name] {
			return
		}
		directory(path.Dir(name))
		seen[name] = true
		h := &tar.Header{
			Name:     name,
			Typeflag: tar.TypeDir,
			Mode:     0o755,
			ModTime:  time.Unix(0, 0),
		}
		if name == "home/sandbox" || name == "workspace" {
			h.Uid, h.Gid = 1000, 1000
		}
		if name == "tmp" {
			h.Mode = 0o1777
		}
		header(h, nil)
	}
	add := func(name string, h *tar.Header, data io.Reader) {
		directory(path.Dir(name))
		seen[name] = true
		h.Name = name
		header(h, data)
	}
	for _, root := range closure {
		if err := filepath.Walk(
			root,
			func(file string, info os.FileInfo, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				name := strings.TrimPrefix(file, "/")
				if info.IsDir() {
					directory(name)
					return nil
				}
				link := ""
				if info.Mode()&os.ModeSymlink != 0 {
					link, err = os.Readlink(file)
					if err != nil {
						return err
					}
				}
				h, err := tar.FileInfoHeader(info, link)
				if err != nil {
					return err
				}
				h.Uid, h.Gid, h.Uname, h.Gname = 0, 0, "", ""
				h.ModTime = time.Unix(0, 0)
				h.AccessTime, h.ChangeTime = time.Time{}, time.Time{}
				if !info.Mode().IsRegular() {
					add(name, h, nil)
					return nil
				}
				f, err := os.Open(file)
				if err != nil {
					return err
				}
				defer f.Close()
				add(name, h, f)
				return nil
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	text := func(name, value string, mode int64) {
		add(
			name,
			&tar.Header{
				Typeflag: tar.TypeReg,
				Mode:     mode,
				Size:     int64(len(value)),
				ModTime:  time.Unix(0, 0),
			},
			strings.NewReader(value),
		)
	}
	link := func(name, target string) {
		add(
			name,
			&tar.Header{
				Typeflag: tar.TypeSymlink,
				Mode:     0o777,
				Linkname: target,
				ModTime:  time.Unix(0, 0),
			},
			nil,
		)
	}
	for _, name := range []string{"dev", "proc", "sys", "run", "tmp", "var/lib", "var/log", "etc/systemd/system", "home/sandbox", "workspace", "usr/lib/systemd", "usr/local/libexec"} {
		directory(name)
	}
	for _, executable := range []struct{ name, source string }{{"boot/vmlinux", kernel}, {"boot/initramfs", filepath.Join(guest, "initramfs")}, {"usr/local/libexec/sandbox-agent", filepath.Join(guest, "sandbox-agent")}, {"usr/lib/systemd/systemd", filepath.Join(systemd, "lib/systemd/systemd")}} {
		info, err := os.Stat(executable.source)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(executable.source)
		if err != nil {
			t.Fatal(err)
		}
		add(
			executable.name,
			&tar.Header{Typeflag: tar.TypeReg, Mode: 0o755, Size: info.Size()},
			f,
		)
		f.Close()
	}
	link(
		"usr/lib/systemd/system",
		filepath.Join(systemd, "example/systemd/system"),
	)
	link("bin/sh", filepath.Join(shell, "bin/sh"))
	for _, tool := range []string{"cat", "stat", "id", "readlink", "sync", "dd"} {
		link("bin/"+tool, filepath.Join(coreutils, "bin", tool))
	}
	link("bin/getfattr", filepath.Join(attr, "bin/getfattr"))
	link("bin/getfacl", filepath.Join(acl, "bin/getfacl"))
	text("etc/os-release", "ID=sandbox-acceptance\nNAME=Sandbox\n", 0o644)
	text(
		"etc/passwd",
		"root:x:0:0:root:/root:/bin/sh\nsandbox:x:1000:1000:sandbox:/home/sandbox:/bin/sh\n",
		0o644,
	)
	text("etc/group", "root:x:0:\nsandbox:x:1000:\n", 0o644)
	text("etc/machine-id", "", 0o644)
	text("etc/resolv.conf", "", 0o644)
	unit, err := os.ReadFile(
		filepath.Join("..", "guest", "systemd", "sandbox-agent.service"),
	)
	if err != nil {
		t.Fatal(err)
	}
	text("etc/systemd/system/sandbox-agent.service", string(unit), 0o644)
	text(
		"usr/local/libexec/acceptance-raw-protection",
		kvmRawWriteChecks,
		0o644,
	)
	text(
		"etc/systemd/system/sandbox-agent.service.d/console.conf",
		"[Service]\nExecStartPre=/bin/sh /usr/local/libexec/acceptance-raw-protection\nStandardOutput=tty\nStandardError=tty\nTTYPath=/dev/ttyS0\n",
		0o644,
	)
	text(
		"etc/systemd/system/acceptance.target",
		"[Unit]\nDescription=Actual guest acceptance\nRequires=sandbox-agent.service\nAfter=sandbox-agent.service\nAllowIsolate=yes\n",
		0o644,
	)
	link("etc/systemd/system/default.target", "acceptance.target")
	link(
		"etc/systemd/system/sysinit.target.wants/sandbox-agent.service",
		"../sandbox-agent.service",
	)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(archive.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, f)
	if err != nil {
		t.Fatal(err)
	}
	d := digest.Digest("sha256:" + hex.EncodeToString(hash.Sum(nil)))
	blobDir := filepath.Join(layout, "blobs", "sha256")
	if err := os.MkdirAll(blobDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(
		archive.Name(),
		filepath.Join(blobDir, d.Encoded()),
	); err != nil {
		t.Fatal(err)
	}
	return ociLayerFixture{
		descriptor: v1.Descriptor{
			MediaType: v1.MediaTypeImageLayer,
			Digest:    d,
			Size:      size,
		},
		diff: d,
	}
}

func kvmMetadataLayers(t *testing.T, layout string) []ociLayerFixture {
	t.Helper()
	entry := func(name, value string) tarEntry {
		e := fileEntry(name, value)
		e.header.Mode = 0o644
		return e
	}
	dir := func(name string) tarEntry {
		e := dirEntry(name)
		e.header.Mode = 0o755
		return e
	}
	regular := entry("acceptance/regular", "contents")
	regular.header.Xattrs = map[string]string{"user.fixture": "retained"}
	setid := entry("acceptance/setid", "setid")
	setid.header.Mode = 0o6755
	capability := entry("acceptance/capability", "capability")
	capability.header.Xattrs = map[string]string{
		"security.capability": string(
			[]byte{1, 0, 0, 2, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		),
	}
	acl := entry("acceptance/acl", "acl")
	// POSIX access ACL: owner rw, named UID1000 r, group r, mask r, other r.
	acl.header.Xattrs = map[string]string{
		"system.posix_acl_access": string(
			[]byte{
				2,
				0,
				0,
				0,
				1,
				0,
				6,
				0,
				255,
				255,
				255,
				255,
				2,
				0,
				4,
				0,
				232,
				3,
				0,
				0,
				4,
				0,
				4,
				0,
				255,
				255,
				255,
				255,
				16,
				0,
				4,
				0,
				255,
				255,
				255,
				255,
				32,
				0,
				4,
				0,
				255,
				255,
				255,
				255,
			},
		),
	}
	link := entry("acceptance/symbolic", "")
	link.header.Typeflag, link.header.Linkname, link.header.Mode = tar.TypeSymlink, "regular", 0o777
	hard := hardEntry("acceptance/hard", "acceptance/regular")
	hard.header.Mode = 0o644
	base := putOCILayer(
		t,
		layout,
		[]tarEntry{
			dir("acceptance"),
			regular,
			hard,
			link,
			setid,
			capability,
			acl,
			entry("acceptance/removed", "remove"),
			entry("acceptance/replaced", "bottom"),
			dir("acceptance/opaque"),
			entry("acceptance/opaque/old", "old"),
		},
		true,
	)
	top := putOCILayer(
		t,
		layout,
		[]tarEntry{
			dir("acceptance"),
			entry("acceptance/.wh.removed", ""),
			entry("acceptance/replaced", "top"),
			dir("acceptance/opaque"),
			entry("acceptance/opaque/.wh..wh..opq", ""),
			entry("acceptance/opaque/new", "new"),
		},
		false,
	)
	return []ociLayerFixture{base, top}
}

type kvmMachine struct {
	dir, socket, vsock string
	command            *exec.Cmd
	client             *http.Client
	stopped            bool
}

func kvmStart(t *testing.T, hypervisor, dir string) *kvmMachine {
	t.Helper()
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Unix socket names are kept below Linux's 108-byte limit even when the
	// persistent artifact directory has a long pathname.
	sockets, err := os.MkdirTemp("", "sandbox-kvm-")
	if err != nil {
		t.Fatal(err)
	}
	vm := &kvmMachine{
		dir:    dir,
		socket: filepath.Join(sockets, "api"),
		vsock:  filepath.Join(sockets, "vsock"),
	}
	log, err := os.Create(filepath.Join(dir, "hypervisor.log"))
	if err != nil {
		t.Fatal(err)
	}
	vm.command = exec.Command(hypervisor, "--api-socket", vm.socket)
	vm.command.Stdout, vm.command.Stderr = log, log
	if err := vm.command.Start(); err != nil {
		t.Fatal(err)
	}
	log.Close()
	t.Cleanup(func() { vm.stop(); os.RemoveAll(sockets) })
	vm.client = &http.Client{
		Timeout: 45 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", vm.socket)
			},
		},
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(vm.socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("VMM API unavailable; inspect %s", dir)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return vm
}

func (vm *kvmMachine) stop() {
	if !vm.stopped {
		vm.stopped = true
		vm.client.CloseIdleConnections()
		_ = vm.command.Process.Kill()
		_ = vm.command.Wait()
	}
}

func (vm *kvmMachine) api(t *testing.T, endpoint string, body any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(
		http.MethodPut,
		"http://localhost/api/v1/"+endpoint,
		reader,
	)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := vm.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil || response.StatusCode != 204 && response.StatusCode != 200 {
		t.Fatalf(
			"%s: %d %s %v; artifacts %s",
			endpoint,
			response.StatusCode,
			data,
			err,
			vm.dir,
		)
	}
}

func (vm *kvmMachine) ready(t *testing.T, ctx context.Context, restored bool) {
	t.Helper()
	if err := systemAgent(
		ctx,
		vm.vsock,
		"kvm-acceptance",
		restored,
	); err != nil {
		t.Fatalf("real guest readiness: %v; inspect %s", err, vm.dir)
	}
}

func (vm *kvmMachine) rpc(
	t *testing.T,
	port int,
	method string,
	params map[string]any,
	data []byte,
) (map[string]any, string) {
	t.Helper()
	conn, err := net.DialTimeout("unix", vm.vsock, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(45 * time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", port); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "OK ") {
		t.Fatalf("vsock handshake %q: %v", line, err)
	}
	p, err := structpb.NewStruct(params)
	if err != nil {
		t.Fatal(err)
	}
	id := newID("acceptance-")
	first := min(len(data), protocol.ChunkSize)
	if err := protocol.WriteFrame(
		conn,
		&protocol.Frame{
			Id:     id,
			Method: method,
			Params: p,
			Data:   data[:first],
			Eof:    first == len(data),
		},
	); err != nil {
		t.Fatal(err)
	}
	for offset := first; offset < len(data); {
		end := min(offset+protocol.ChunkSize, len(data))
		if err := protocol.WriteFrame(
			conn,
			&protocol.Frame{
				Id:   id,
				Data: data[offset:end],
				Eof:  end == len(data),
			},
		); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	var output strings.Builder
	for {
		frame, err := protocol.ReadFrame(reader)
		if err != nil {
			t.Fatalf(
				"%s: %v; output %q; logs %s",
				method,
				err,
				output.String(),
				vm.dir,
			)
		}
		if frame.Id != id || frame.Error != "" {
			t.Fatalf(
				"%s: invalid/error reply %+v; output %q",
				method,
				frame,
				output.String(),
			)
		}
		output.Write(frame.Data)
		if frame.Eof {
			return frame.Params.AsMap(), output.String()
		}
	}
}

func (vm *kvmMachine) treeRoundtrip(t *testing.T) {
	t.Helper()
	content := bytes.Repeat([]byte("binary\x00data\n"), 16384)
	xattr := []byte("binary\x00attribute\n\xff")
	mtime := time.Unix(1700000000, 123456789)
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for _, header := range []*tar.Header{
		{Name: ".", Typeflag: tar.TypeDir, Mode: 0o750},
		{Name: "alias", Typeflag: tar.TypeLink, Linkname: "file", Mode: 0o6751},
		{Name: "file", Typeflag: tar.TypeReg, Size: int64(len(content)), Mode: 0o6751, PAXRecords: map[string]string{"LIBARCHIVE.xattr.user.fixture": base64.RawStdEncoding.EncodeToString(xattr)}},
		{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "file", Mode: 0o777},
		{Name: "dangling", Typeflag: tar.TypeSymlink, Linkname: "missing", Mode: 0o777},
	} {
		header.Uid, header.Gid, header.ModTime, header.Format = 123, 456, mtime, tar.FormatPAX
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Typeflag == tar.TypeReg {
			if _, err := writer.Write(content); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	vm.rpc(
		t,
		2048,
		"tree_push",
		map[string]any{"path": "/workspace/tree-original"},
		archive.Bytes(),
	)
	_, pulled := vm.rpc(
		t,
		2048,
		"tree_pull",
		map[string]any{"path": "/workspace/tree-original"},
		nil,
	)
	reader := tar.NewReader(strings.NewReader(pulled))
	found := false
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag == tar.TypeReg {
			got, err := io.ReadAll(reader)
			if err != nil || !bytes.Equal(got, content) || header.Uid != 1000 ||
				header.Gid != 1000 ||
				header.Mode != 0o6751 ||
				!header.ModTime.Equal(mtime) {
				t.Fatalf(
					"actual guest tree metadata/content mismatch: %+v: %v",
					header,
					err,
				)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("actual guest pull omitted regular file")
	}
	vm.rpc(
		t,
		2048,
		"tree_push",
		map[string]any{"path": "/workspace/tree-copy"},
		[]byte(pulled),
	)
	vm.exec(t, `set -eu
export PATH=/bin
for tree in /workspace/tree-original /workspace/tree-copy; do
  [ "$(stat -c '%u:%g:%a' "$tree")" = '1000:1000:750' ]
  [ "$(stat -c '%u:%g:%a' "$tree/file")" = '1000:1000:6751' ]
  [ "$(stat -c %i "$tree/file")" = "$(stat -c %i "$tree/alias")" ]
  [ "$(readlink "$tree/link")" = file ]
  [ "$(readlink "$tree/dangling")" = missing ]
  getfattr --only-values -n user.fixture "$tree/file" 2>/dev/null >"$tree/xattr-value"
done
printf 'guest-tree-roundtrip-ok\n'
`)
	_, copied := vm.rpc(
		t,
		2048,
		"fs_read",
		map[string]any{"path": "/workspace/tree-copy/file"},
		nil,
	)
	if !bytes.Equal([]byte(copied), content) {
		t.Fatal("binary file changed during guest tree roundtrip")
	}
	_, raw := vm.rpc(
		t,
		2048,
		"fs_read",
		map[string]any{"path": "/workspace/tree-copy/xattr-value"},
		nil,
	)
	if !bytes.Equal([]byte(raw), xattr) {
		t.Fatal("binary user xattr changed during guest tree roundtrip")
	}
}

func (vm *kvmMachine) exec(t *testing.T, command string) {
	t.Helper()
	params, out := vm.rpc(
		t,
		2048,
		"exec",
		map[string]any{"command": command},
		nil,
	)
	if exit, ok := params["exit_code"].(float64); !ok || exit != 0 {
		t.Fatalf(
			"guest command failed: %v output %q command:\n%s",
			params,
			out,
			command,
		)
	}
	t.Log(strings.TrimSpace(out))
}

func (vm *kvmMachine) capture(t *testing.T, upper, name string) string {
	t.Helper()
	capture := filepath.Join(vm.dir, name)
	if err := os.Mkdir(capture, 0o700); err != nil {
		t.Fatal(err)
	}
	vm.api(t, "vm.pause", nil)
	defer vm.api(t, "vm.resume", nil)
	vm.api(
		t,
		"vm.snapshot",
		map[string]any{"destination_url": "file://" + capture},
	)
	if err := copyFile(
		upper,
		filepath.Join(capture, "disk.raw"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := validateCOWTopology(capture, 512<<20); err != nil {
		t.Fatal(err)
	}
	return capture
}

func kvmRestore(t *testing.T, hypervisor, dir, capture string) *kvmMachine {
	t.Helper()
	vm := kvmStart(t, hypervisor, dir)
	restore := filepath.Join(dir, "restore")
	if err := os.Mkdir(restore, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"config.json", "state.json"} {
		if err := copyFile(
			filepath.Join(capture, name),
			filepath.Join(restore, name),
			0o600,
		); err != nil {
			t.Fatal(err)
		}
	}
	backing := filepath.Join(restore, "memory-ranges")
	if err := os.Link(
		filepath.Join(capture, "memory-ranges"),
		backing,
	); err != nil {
		t.Fatal(err)
	}
	upper := filepath.Join(dir, "upper.raw")
	if err := copyFile(
		filepath.Join(capture, "disk.raw"),
		upper,
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	file := filepath.Join(restore, "config.json")
	data, err := os.ReadFile(file)
	if err != nil || json.Unmarshal(data, &config) != nil {
		t.Fatalf("invalid native snapshot config: %v", err)
	}
	for _, value := range config["disks"].([]any) {
		disk := value.(map[string]any)
		if disk["serial"] == "sandbox-write" {
			disk["path"] = upper
		}
	}
	config["vsock"].(map[string]any)["socket"] = vm.vsock
	config["serial"].(map[string]any)["file"] = filepath.Join(dir, "serial.log")
	data, err = json.Marshal(config)
	if err != nil || os.WriteFile(file, data, 0o600) != nil {
		t.Fatalf("write restore config: %v", err)
	}
	vm.api(
		t,
		"vm.restore",
		map[string]any{
			"source_url":          "file://" + restore,
			"memory_restore_mode": "CopyOnWrite",
			"prefault":            false,
			"resume":              false,
		},
	)
	var stat unix.Stat_t
	if err := unix.Stat(backing, &stat); err != nil {
		t.Fatal(err)
	}
	maps, err := os.ReadFile(
		fmt.Sprintf("/proc/%d/maps", vm.command.Process.Pid),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(dir, "maps.txt"),
		maps,
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	identity, _ := json.Marshal(
		map[string]any{
			"device": stat.Dev,
			"inode":  stat.Ino,
			"bytes":  stat.Size,
		},
	)
	if err := os.WriteFile(
		filepath.Join(dir, "backing.json"),
		identity,
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := verifyCOWFileMappings(
		bytes.NewReader(maps),
		&stat,
		uint64(stat.Size),
	); err != nil {
		t.Fatalf("VMM did not retain private file-backed RAM: %v", err)
	}
	vm.api(t, "vm.resume", nil)
	return vm
}
