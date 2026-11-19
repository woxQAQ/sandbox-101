//go:build linux && guest_image

package integration_test

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
	"strings"
	"testing"
	"time"

	ocidigest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	protocol "github.com/woxQAQ/sandbox-protocol/gen"
)

// This guest image test runs the actual pinned VMM and guest, independently
// of Loom's privileged cgroup/network launcher and the Haven daemon. It proves
// guest file views, RPC and COW restore, not daemon lifecycle or isolation.
// It requires working KVM and native image tools; prerequisites never silently skip.
//
//nolint:staticcheck // Binary OCI xattrs intentionally use archive/tar's retained encoding.
func TestGuestOCIFileViewAndCOW(t *testing.T) {
	if os.Getuid() == 0 {
		t.Fatal("run this standalone guest image test as an unprivileged device-authorized user")
	}
	hypervisor := requiredGuestInput(t, "SANDBOX_GUEST_HYPERVISOR")
	kernel := requiredGuestInput(t, "SANDBOX_GUEST_KERNEL")
	guest := requiredGuestInput(t, "SANDBOX_GUEST_DIR")
	rootfs := requiredGuestInput(t, "SANDBOX_GUEST_ROOTFS")
	artifacts := requiredGuestInput(t, "SANDBOX_GUEST_IMAGE_ARTIFACTS")
	if err := os.MkdirAll(artifacts, 0o700); err != nil {
		t.Fatal(err)
	}
	run, err := os.MkdirTemp(artifacts, "guest-image-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("persistent guest image test artifacts: %s", run)
	version, err := exec.Command(hypervisor, "--version").CombinedOutput()
	if err != nil || !strings.Contains(string(version), host.CloudHypervisorCommit) {
		t.Fatalf("wrong pinned hypervisor: %s: %v", version, err)
	}
	runImageTool(t, "readelf", "-l", hypervisor)
	for _, executable := range []string{
		hypervisor,
		filepath.Join(guest, "sandbox-agent"),
		filepath.Join(guest, "sandbox-init"),
	} {
		for _, args := range [][]string{
			{"-l", executable},
			{"-d", executable},
		} {
			out := runImageTool(t, "readelf", args...)
			if bytes.Contains(out, []byte("INTERP")) || bytes.Contains(out, []byte("NEEDED")) {
				t.Fatalf("guest image fixture executable is not static: %s", executable)
			}
		}
	}
	layout := filepath.Join(run, "oci")
	if err := os.Mkdir(layout, 0o700); err != nil {
		t.Fatal(err)
	}
	base := guestRuntimeLayer(t, layout, guestMaterials{
		Kernel:         kernel,
		GuestDirectory: guest,
		RootFS:         rootfs,
	})
	meta := guestMetadataLayers(t, layout)
	cross := testfixture.HardEntry("fixture/cross-layer-hard", "fixture/regular")
	cross.Header.Mode = 0o644
	root := testfixture.DirEntry(".")
	root.Header.Uid, root.Header.Gid, root.Header.Mode = 0, 0, 0o755
	root.Header.ModTime = time.Unix(1700000001, 123456789)
	root.Header.Xattrs = map[string]string{
		"user.shared-root": "retained",
	}
	broaderEntries := []testfixture.TarEntry{
		root,
		cross,
		testfixture.FileEntry("fixture/implicit/deep/value", "implicit"),
		testfixture.FileEntry("fixture/duplicate", "old"),
		testfixture.FileEntry("fixture/duplicate", "last"),
	}
	for i := range broaderEntries {
		if broaderEntries[i].Header.Typeflag == tar.TypeReg {
			broaderEntries[i].Header.Mode = 0o644
		}
	}
	broader := testfixture.PutOCILayer(t, layout, broaderEntries, false)
	layers := append(append([]testfixture.OCILayerFixture{
		base,
	}, meta...), broader)
	for _, materialization := range []string{
		host.LayeredEROFSLayout,
		host.MergedOCIEROFSLayout,
	} {
		t.Run(materialization, func(t *testing.T) {
			dir := filepath.Join(run, materialization)
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			// The host backend is unused: Haven only performs the durable OCI import.
			store := filepath.Join(dir, "store")
			if err := os.Mkdir(store, 0o700); err != nil {
				t.Fatal(err)
			}
			e := testfixture.NativeImageFixtureAt(t, store)
			defer e.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
			defer cancel()
			var image string
			if materialization == host.LayeredEROFSLayout {
				// The shared metadata composer also resolves true forward chains,
				// which the explicit umoci compatibility path does not accept.
				forwardEntries := []testfixture.TarEntry{
					testfixture.HardEntry("fixture/forward-a", "fixture/forward-b"),
					testfixture.HardEntry("fixture/forward-b", "fixture/forward-target"),
					testfixture.FileEntry("fixture/forward-target", "forward"),
				}
				for i := range forwardEntries {
					forwardEntries[i].Header.Mode = 0o644
				}
				forward := testfixture.PutOCILayer(t, layout, forwardEntries, false)
				image = testfixture.PutOCIManifest(
					t,
					layout,
					"actual-systemd-forward",
					append(append([]testfixture.OCILayerFixture{}, layers...), forward),
				)
			} else {
				// Compatibility pins umask 077, so its implicit parents are
				// private. Explicit headers make this path workload-readable;
				// the shared path above retains its implicit-parent checks.
				directories := []testfixture.TarEntry{
					testfixture.DirEntry("fixture/implicit"),
					testfixture.DirEntry("fixture/implicit/deep"),
				}
				for i := range directories {
					directories[i].Header.Mode = 0o755
				}
				readable := testfixture.PutOCILayer(t, layout, directories, false)
				image = testfixture.PutOCIManifest(
					t,
					layout,
					"actual-systemd-compat-parents",
					append(append([]testfixture.OCILayerFixture{}, layers...), readable),
				)
			}
			manifest, err := e.ImportOCI(
				ctx,
				imageimport.ImageImportOptions{
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
			imageDir := filepath.Join(e.Opts.StateDir, "images", image[7:])
			bindings, err := manifest.ReadOnlyBindings(imageDir)
			if err != nil {
				t.Fatal(err)
			}
			upper := filepath.Join(dir, "upper.raw")
			file, err := os.OpenFile(upper, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if err = file.Truncate(256 << 20); err != nil {
				t.Fatal(err)
			}
			file.Close()
			runImageTool(t, "mkfs.ext4", "-q", "-F", upper)
			vm := startGuestVM(t, hypervisor, filepath.Join(dir, "source"))
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
				"cpus": map[string]any{
					"boot_vcpus": 1,
					"max_vcpus":  1,
				},
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
					"socket": vm.Vsock,
				},
				"console": map[string]any{
					"mode": "Null",
				},
				"serial": map[string]any{
					"mode": "File",
					"file": filepath.Join(vm.Dir, "serial.log"),
				},
			})
			vm.api(t, "vm.boot", nil)
			vm.ready(t, ctx, false)
			vm.rpc(t, guestRequest{
				Port:   2046,
				Method: "activate",
				Params: map[string]any{
					"sandbox_id":     "fixture",
					"network":        "none",
					"env":            map[string]any{},
					"reset_identity": true,
					"hostname":       "fixture",
				},
			})
			fileViewChecks := guestFileViewChecks
			if materialization == host.LayeredEROFSLayout {
				fileViewChecks += `
[ "$(cat /fixture/forward-a)" = forward ]
[ "$(stat -c %i /fixture/forward-a)" = "$(stat -c %i /fixture/forward-b)" ]
[ "$(stat -c %i /fixture/forward-a)" = "$(stat -c %i /fixture/forward-target)" ]
`
			}
			vm.exec(t, fileViewChecks)
			vm.treeRoundtrip(t)
			vm.exec(t, "printf 'original' > /workspace/branch; sync")
			capture := vm.capture(t, upper, "first")
			clone := restoreGuestVM(t, hypervisor, filepath.Join(dir, "clone"), capture)
			clone.ready(t, ctx, true)
			if err := os.RemoveAll(capture); err != nil {
				t.Fatal(err)
			}
			clone.exec(t, "test \"$(cat /workspace/branch)\" = original; printf 'clone' > /workspace/branch; sync")
			vm.exec(t, "test \"$(cat /workspace/branch)\" = original; printf 'source' > /workspace/branch; sync")
			second := clone.capture(t, filepath.Join(clone.Dir, "upper.raw"), "second")
			clone.stop()
			vm.stop()
			grandchild := restoreGuestVM(t, hypervisor, filepath.Join(dir, "grandchild"), second)
			grandchild.ready(t, ctx, true)
			grandchild.exec(t, "test \"$(cat /workspace/branch)\" = clone")
			grandchild.exec(t, fileViewChecks)
			t.Log(
				"PASS actual OCI -> EROFS -> initramfs -> systemd -> vsock file view; COW clone, independent disks and COW resnapshot",
			)
		})
	}
}

// This exercises the userspace handoff through actual OCI materialization,
// without booting a VM or consulting a package database.
func TestGuestRootfsOCI(t *testing.T) {
	testfixture.RequireEROFS(t)
	for _, materialization := range []string{
		host.LayeredEROFSLayout,
		host.MergedOCIEROFSLayout,
	} {
		t.Run(materialization, func(t *testing.T) {
			if materialization == host.MergedOCIEROFSLayout {
				testfixture.RequireMergedOCI(t)
			}
			rootfs, guest, layout := t.TempDir(), t.TempDir(), t.TempDir()
			payload := "self-contained-runtime"
			target := "packages/runtime/executable"
			if err := os.MkdirAll(filepath.Join(rootfs, "packages/runtime"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(rootfs, target), []byte(payload), 0o755); err != nil {
				t.Fatal(err)
			}
			// Absolute links must resolve in the guest, not against the host.
			for _, name := range []string{
				"usr/lib/systemd/systemd",
				"bin/sh",
				"bin/cat",
				"bin/stat",
				"bin/id",
				"bin/readlink",
				"bin/sync",
				"bin/dd",
				"bin/getfattr",
				"bin/getfacl",
			} {
				file := filepath.Join(rootfs, name)
				if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("/"+target, file); err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range []string{
				"kernel",
				"initramfs",
				"sandbox-agent",
			} {
				if err := os.WriteFile(filepath.Join(guest, name), []byte("fixture-"+name), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			layer := guestRuntimeLayer(t, layout, guestMaterials{
				Kernel:         filepath.Join(guest, "kernel"),
				GuestDirectory: guest,
				RootFS:         rootfs,
			})
			image := testfixture.PutOCIManifest(t, layout, "provider-independent-runtime", []testfixture.OCILayerFixture{
				layer,
			})
			e := testfixture.NewNativeImageFixture(t)
			t.Cleanup(func() {
				e.Close()
			})
			m, err := e.ImportOCI(context.Background(), imageimport.ImageImportOptions{
				Layout:          layout,
				Digest:          image,
				Materialization: materialization,
				MaxBytes:        4 << 20,
				MaxFiles:        1000,
			})
			if err != nil {
				t.Fatal(err)
			}
			var tree string
			if materialization == host.MergedOCIEROFSLayout {
				tree = testfixture.ExtractMergedManifest(t, e, m)
			} else {
				tree = testfixture.ExtractManifest(t, e, m)
			}
			for name, expected := range map[string]string{
				target:                            payload,
				"usr/lib/systemd/systemd":         payload,
				"boot/vmlinux":                    "fixture-kernel",
				"boot/initramfs":                  "fixture-initramfs",
				"usr/local/libexec/sandbox-agent": "fixture-sandbox-agent",
			} {
				data, err := os.ReadFile(filepath.Join(tree, name))
				if err != nil || string(data) != expected {
					t.Fatalf("materialized runtime %s: %q: %v", name, data, err)
				}
			}
			link, err := os.Readlink(filepath.Join(tree, "bin/sh"))
			if err != nil || link != "/"+target {
				t.Fatalf("guest absolute symlink changed: %q: %v", link, err)
			}
		})
	}
}

// Linux can reject readonly block opens with EACCES or writes with EPERM. Prove
// root identity, writable node permissions and raw reads before checking writes.
const guestRawWriteChecks = `set -eux
export PATH=/bin LC_ALL=C
umask 022
[ "$(id -u)" = 0 ]
count=0
printf '' > /run/fixture-raw-proof
for serial_file in /sys/block/*/serial; do
  serial=$(cat "$serial_file")
  case "$serial" in sandbox-root|sandbox-data-*) ;; *) continue ;; esac
  block=${serial_file%/serial}
  device=/dev/${block##*/}
  [ -b "$device" ]
  [ -w "$device" ]
  [ "$(cat "$block/ro")" = 1 ]
  dd if="$device" of=/dev/null bs=512 count=1
  if dd if=/dev/zero of="$device" bs=512 count=1 conv=notrunc 2>/run/fixture-raw-error; then
    exit 1
  fi
  error=$(cat /run/fixture-raw-error)
  printf '%s\n' "$error" >&2
  case "$error" in
    *'Read-only file system'*) reason=EROFS ;;
    *'Permission denied'*) reason=EACCES ;;
    *'Operation not permitted'*) reason=EPERM ;;
    *) exit 1 ;;
  esac
  printf 'uid=0 serial=%s readonly=1 rejected=%s\n' "$serial" "$reason" >> /run/fixture-raw-proof
  count=$((count + 1))
done
[ "$count" -gt 0 ]
printf 'guest-root-raw-write-protection-ok\n' >> /run/fixture-raw-proof
`

const guestFileViewChecks = `set -eux
export PATH=/bin
[ "$(id -u)" = 1000 ]
[ "$(stat -c '%u:%g:%a' /)" = '0:0:755' ]
[ "$(cat /proc/1/comm)" = systemd ]
raw_proof=$(cat /run/fixture-raw-proof)
case "$raw_proof" in *guest-root-raw-write-protection-ok*) ;; *) exit 1 ;; esac
printf '%s\n' "$raw_proof"
[ "$(stat -f -c %T /run/sandbox-boot/lower)" = erofs ]
[ "$(stat -f -c %T /)" = overlayfs ]
[ "$(cat /fixture/regular)" = contents ]
[ "$(stat -c '%u:%g:%a:%Y' /fixture/regular)" = '123:456:644:1700000000' ]
[ "$(stat -c '%i' /fixture/regular)" = "$(stat -c '%i' /fixture/hard)" ]
[ "$(stat -c '%i' /fixture/regular)" = "$(stat -c '%i' /fixture/cross-layer-hard)" ]
[ "$(cat /fixture/implicit/deep/value)" = implicit ]
[ "$(cat /fixture/duplicate)" = last ]
[ "$(stat -c '%u:%g:%a:%Y' /run/sandbox-boot/lower)" = '0:0:755:1700000001' ]
[ "$(getfattr --only-values -n user.shared-root /run/sandbox-boot/lower 2>/dev/null)" = retained ]
[ "$(readlink /fixture/symbolic)" = regular ]
[ "$(getfattr --only-values -n user.fixture /fixture/regular 2>/dev/null)" = retained ]
[ "$(cat /fixture/replaced)" = top ]
[ ! -e /fixture/removed ]
[ ! -e /fixture/opaque/old ]
[ "$(cat /fixture/opaque/new)" = new ]
[ "$(stat -c '%a' /fixture/setid)" = 6755 ]
acl=$(getfacl -cpn /fixture/acl)
case "$acl" in *'user:1000:r--'*) ;; *) exit 1 ;; esac
getfattr --only-values -n security.capability /fixture/capability >/tmp/capability
[ "$(stat -c %s /tmp/capability)" = 20 ]
[ ! -w /etc ]
[ ! -w /var/lib ]
[ ! -w /run/sandbox-boot/lower/fixture/regular ]
if printf forbidden >> /run/sandbox-boot/lower/fixture/regular 2>/dev/null; then exit 1; fi
printf writable > /workspace/file-view-check
[ "$(cat /workspace/file-view-check)" = writable ]
printf 'guest-file-view-ok\n'
`

func requiredGuestInput(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" || !filepath.IsAbs(value) {
		t.Fatalf("%s must be an absolute configured path", name)
	}
	return value
}

func runImageTool(t *testing.T, tool string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, tool, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v: %s", tool, err, out)
	}
	return out
}

type guestMaterials struct {
	Kernel         string
	GuestDirectory string
	RootFS         string
}

func guestRuntimeLayer(t *testing.T, layout string, materials guestMaterials) testfixture.OCILayerFixture {
	kernel, guest, rootfs := materials.Kernel, materials.GuestDirectory, materials.RootFS
	t.Helper()
	var err error
	rootfs, err = filepath.EvalSymlinks(rootfs)
	if err != nil {
		t.Fatal(err)
	}
	rootfs, err = filepath.Abs(rootfs)
	if err != nil || rootfs == "/" {
		t.Fatalf("guest image test requires a dedicated rootfs, not the host root: %v", err)
	}
	root, err := os.Open(rootfs)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	// Resolve links relative to the guest root, never against the host. A
	// package symlink alone is not a self-contained guest runtime.
	for _, name := range []string{
		"usr/lib/systemd/systemd",
		"bin/sh",
		"bin/cat",
		"bin/stat",
		"bin/id",
		"bin/readlink",
		"bin/sync",
		"bin/dd",
		"bin/getfattr",
		"bin/getfacl",
	} {
		fd, err := unix.Openat2(int(root.Fd()), name, &unix.OpenHow{
			Flags:   unix.O_PATH | unix.O_CLOEXEC,
			Resolve: unix.RESOLVE_IN_ROOT | unix.RESOLVE_NO_MAGICLINKS,
		})
		if err != nil {
			t.Fatalf("incomplete guest rootfs (%s): %v", name, err)
		}
		file := os.NewFile(uintptr(fd), name)
		info, err := file.Stat()
		file.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
			t.Fatalf("guest rootfs requires an executable at %s", name)
		}
	}
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
	if err := filepath.Walk(
		rootfs,
		func(file string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			name, err := filepath.Rel(rootfs, file)
			if err != nil {
				return err
			}
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
	for _, name := range []string{
		"dev",
		"proc",
		"sys",
		"run",
		"tmp",
		"var/lib",
		"var/log",
		"etc/systemd/system",
		"home/sandbox",
		"workspace",
		"usr/lib/systemd",
		"usr/local/libexec",
	} {
		directory(name)
	}
	addFile := func(name string, file *os.File) {
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		add(name, &tar.Header{
			Typeflag: tar.TypeReg,
			Mode:     0o755,
			Size:     info.Size(),
		}, file)
	}
	for _, executable := range []struct {
		name   string
		source string
	}{
		{"boot/vmlinux", kernel},
		{"boot/initramfs", filepath.Join(guest, "initramfs")},
		{"usr/local/libexec/sandbox-agent", filepath.Join(guest, "sandbox-agent")},
	} {
		file, err := os.Open(executable.source)
		if err != nil {
			t.Fatal(err)
		}
		addFile(executable.name, file)
	}
	// The boot contract requires a regular systemd entry. Resolve a provider's
	// guest-relative link safely and publish its bytes at the canonical path.
	fd, err := unix.Openat2(
		int(root.Fd()),
		"usr/lib/systemd/systemd",
		&unix.OpenHow{
			Flags:   unix.O_RDONLY | unix.O_CLOEXEC,
			Resolve: unix.RESOLVE_IN_ROOT | unix.RESOLVE_NO_MAGICLINKS,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	addFile("usr/lib/systemd/systemd", os.NewFile(uintptr(fd), "systemd"))
	text("etc/os-release", "ID=sandbox-fixture\nNAME=Sandbox\n", 0o644)
	text("etc/passwd", "root:x:0:0:root:/root:/bin/sh\nsandbox:x:1000:1000:sandbox:/home/sandbox:/bin/sh\n", 0o644)
	text("etc/group", "root:x:0:\nsandbox:x:1000:\n", 0o644)
	text("etc/machine-id", "", 0o644)
	text("etc/resolv.conf", "", 0o644)
	unit, err := os.ReadFile(filepath.Join("..", "..", "guest", "systemd", "sandbox-agent.service"))
	if err != nil {
		t.Fatal(err)
	}
	text("etc/systemd/system/sandbox-agent.service", string(unit), 0o644)
	text("usr/local/libexec/fixture-raw-protection", guestRawWriteChecks, 0o644)
	text(
		"etc/systemd/system/sandbox-agent.service.d/console.conf",
		"[Service]\nExecStartPre=/bin/sh /usr/local/libexec/fixture-raw-protection\nStandardOutput=tty\nStandardError=tty\nTTYPath=/dev/ttyS0\n",
		0o644,
	)
	text(
		"etc/systemd/system/fixture.target",
		"[Unit]\nDescription=Guest filesystem fixture\nRequires=sandbox-agent.service\nAfter=sandbox-agent.service\nAllowIsolate=yes\n",
		0o644,
	)
	link("etc/systemd/system/default.target", "fixture.target")
	link("etc/systemd/system/sysinit.target.wants/sandbox-agent.service", "../sandbox-agent.service")
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
	d := ocidigest.Digest("sha256:" + hex.EncodeToString(hash.Sum(nil)))
	blobDir := filepath.Join(layout, "blobs", "sha256")
	if err := os.MkdirAll(blobDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(archive.Name(), filepath.Join(blobDir, d.Encoded())); err != nil {
		t.Fatal(err)
	}
	return testfixture.OCILayerFixture{
		Descriptor: v1.Descriptor{
			MediaType: v1.MediaTypeImageLayer,
			Digest:    d,
			Size:      size,
		},
		Diff: d,
	}
}

//nolint:staticcheck // Preserve binary capability and ACL bytes in the actual OCI fixture.
func guestMetadataLayers(t *testing.T, layout string) []testfixture.OCILayerFixture {
	t.Helper()
	entry := func(name, value string) testfixture.TarEntry {
		e := testfixture.FileEntry(name, value)
		e.Header.Mode = 0o644
		return e
	}
	dir := func(name string) testfixture.TarEntry {
		e := testfixture.DirEntry(name)
		e.Header.Mode = 0o755
		return e
	}
	regular := entry("fixture/regular", "contents")
	regular.Header.Xattrs = map[string]string{
		"user.fixture": "retained",
	}
	setid := entry("fixture/setid", "setid")
	setid.Header.Mode = 0o6755
	capability := entry("fixture/capability", "capability")
	capability.Header.Xattrs = map[string]string{
		"security.capability": string([]byte{
			1,
			0,
			0,
			2,
			0,
			4,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
		}),
	}
	acl := entry("fixture/acl", "acl")
	// POSIX access ACL: owner rw, named UID1000 r, group r, mask r, other r.
	acl.Header.Xattrs = map[string]string{
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
	link := entry("fixture/symbolic", "")
	link.Header.Typeflag, link.Header.Linkname, link.Header.Mode = tar.TypeSymlink, "regular", 0o777
	hard := testfixture.HardEntry("fixture/hard", "fixture/regular")
	hard.Header.Mode = 0o644
	base := testfixture.PutOCILayer(
		t,
		layout,
		[]testfixture.TarEntry{
			dir("fixture"),
			regular,
			hard,
			link,
			setid,
			capability,
			acl,
			entry("fixture/removed", "remove"),
			entry("fixture/replaced", "bottom"),
			dir("fixture/opaque"),
			entry("fixture/opaque/old", "old"),
		},
		true,
	)
	top := testfixture.PutOCILayer(
		t,
		layout,
		[]testfixture.TarEntry{
			dir("fixture"),
			entry("fixture/.wh.removed", ""),
			entry("fixture/replaced", "top"),
			dir("fixture/opaque"),
			entry("fixture/opaque/.wh..wh..opq", ""),
			entry("fixture/opaque/new", "new"),
		},
		false,
	)
	return []testfixture.OCILayerFixture{
		base,
		top,
	}
}

type guestVM struct {
	Dir     string
	socket  string
	Vsock   string
	command *exec.Cmd
	client  *http.Client
	stopped bool
}

func startGuestVM(t *testing.T, hypervisor, dir string) *guestVM {
	t.Helper()
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Unix socket names are kept below Linux's 108-byte limit even when the
	// persistent artifact directory has a long pathname.
	sockets, err := os.MkdirTemp("", "sandbox-vm-")
	if err != nil {
		t.Fatal(err)
	}
	vm := &guestVM{
		Dir:    dir,
		socket: filepath.Join(sockets, "api"),
		Vsock:  filepath.Join(sockets, "vsock"),
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
	t.Cleanup(func() {
		vm.stop()
		os.RemoveAll(sockets)
	})
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

func (vm *guestVM) stop() {
	if !vm.stopped {
		vm.stopped = true
		vm.client.CloseIdleConnections()
		_ = vm.command.Process.Kill()
		_ = vm.command.Wait()
	}
}

func (vm *guestVM) api(t *testing.T, endpoint string, body any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(http.MethodPut, "http://localhost/api/v1/"+endpoint, reader)
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
		t.Fatalf("%s: %d %s %v; artifacts %s", endpoint, response.StatusCode, data, err, vm.Dir)
	}
}

func (vm *guestVM) ready(t *testing.T, ctx context.Context, restored bool) {
	t.Helper()
	if err := loom.SystemAgent(ctx, vm.Vsock, "guest-image-fixture", restored); err != nil {
		t.Fatalf("real guest readiness: %v; inspect %s", err, vm.Dir)
	}
}

type guestRequest struct {
	Port   int
	Method string
	Params map[string]any
	Data   []byte
}

func (vm *guestVM) rpc(t *testing.T, request guestRequest) (map[string]any, string) {
	port, method, params, data := request.Port, request.Method, request.Params, request.Data
	t.Helper()
	conn, err := net.DialTimeout("unix", vm.Vsock, 5*time.Second)
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
	id := testfixture.NewID("fixture-")
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
			t.Fatalf("%s: %v; output %q; logs %s", method, err, output.String(), vm.Dir)
		}
		if frame.Id != id || frame.Error != "" {
			t.Fatalf("%s: invalid/error reply %+v; output %q", method, frame, output.String())
		}
		output.Write(frame.Data)
		if frame.Eof {
			return frame.Params.AsMap(), output.String()
		}
	}
}

func (vm *guestVM) treeRoundtrip(t *testing.T) {
	t.Helper()
	content := bytes.Repeat([]byte("binary\x00data\n"), 16384)
	xattr := []byte("binary\x00attribute\n\xff")
	mtime := time.Unix(1700000000, 123456789)
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for _, header := range []*tar.Header{
		{
			Name:     ".",
			Typeflag: tar.TypeDir,
			Mode:     0o750,
		},
		{
			Name:     "alias",
			Typeflag: tar.TypeLink,
			Linkname: "file",
			Mode:     0o6751,
		},
		{
			Name:     "file",
			Typeflag: tar.TypeReg,
			Size:     int64(len(content)),
			Mode:     0o6751,
			PAXRecords: map[string]string{
				"LIBARCHIVE.xattr.user.fixture": base64.RawStdEncoding.EncodeToString(xattr),
			},
		},
		{
			Name:     "link",
			Typeflag: tar.TypeSymlink,
			Linkname: "file",
			Mode:     0o777,
		},
		{
			Name:     "dangling",
			Typeflag: tar.TypeSymlink,
			Linkname: "missing",
			Mode:     0o777,
		},
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
	vm.rpc(t, guestRequest{
		Port:   2048,
		Method: "tree_push",
		Params: map[string]any{
			"path": "/workspace/tree-original",
		},
		Data: archive.Bytes(),
	})
	_, pulled := vm.rpc(t, guestRequest{
		Port:   2048,
		Method: "tree_pull",
		Params: map[string]any{
			"path": "/workspace/tree-original",
		},
	})
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
			if err != nil || !bytes.Equal(got, content) || header.Uid != 1000 || header.Gid != 1000 ||
				header.Mode != 0o6751 ||
				!header.ModTime.Equal(mtime) {
				t.Fatalf("actual guest tree metadata/content mismatch: %+v: %v", header, err)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("actual guest pull omitted regular file")
	}
	vm.rpc(t, guestRequest{
		Port:   2048,
		Method: "tree_push",
		Params: map[string]any{
			"path": "/workspace/tree-copy",
		},
		Data: []byte(pulled),
	})
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
	_, copied := vm.rpc(t, guestRequest{
		Port:   2048,
		Method: "fs_read",
		Params: map[string]any{
			"path": "/workspace/tree-copy/file",
		},
	})
	if !bytes.Equal([]byte(copied), content) {
		t.Fatal("binary file changed during guest tree roundtrip")
	}
	_, raw := vm.rpc(t, guestRequest{
		Port:   2048,
		Method: "fs_read",
		Params: map[string]any{
			"path": "/workspace/tree-copy/xattr-value",
		},
	})
	if !bytes.Equal([]byte(raw), xattr) {
		t.Fatal("binary user xattr changed during guest tree roundtrip")
	}
}

func (vm *guestVM) exec(t *testing.T, command string) {
	t.Helper()
	params, out := vm.rpc(t, guestRequest{
		Port:   2048,
		Method: "exec",
		Params: map[string]any{
			"command": command,
		},
	})
	if exit, ok := params["exit_code"].(float64); !ok || exit != 0 {
		t.Fatalf("guest command failed: %v output %q command:\n%s", params, out, command)
	}
	t.Log(strings.TrimSpace(out))
}

func (vm *guestVM) capture(t *testing.T, upper, name string) string {
	t.Helper()
	capture := filepath.Join(vm.Dir, name)
	if err := os.Mkdir(capture, 0o700); err != nil {
		t.Fatal(err)
	}
	vm.api(t, "vm.pause", nil)
	defer vm.api(t, "vm.resume", nil)
	vm.api(t, "vm.snapshot", map[string]any{
		"destination_url": "file://" + capture,
	})
	if err := loom.CopyFile(upper, filepath.Join(capture, "disk.raw"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loom.ValidateCOWTopology(capture, 512<<20); err != nil {
		t.Fatal(err)
	}
	return capture
}

func restoreGuestVM(t *testing.T, hypervisor, dir, capture string) *guestVM {
	t.Helper()
	vm := startGuestVM(t, hypervisor, dir)
	restore := filepath.Join(dir, "restore")
	if err := os.Mkdir(restore, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"config.json",
		"state.json",
	} {
		if err := loom.CopyFile(filepath.Join(capture, name), filepath.Join(restore, name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	backing := filepath.Join(restore, "memory-ranges")
	if err := os.Link(filepath.Join(capture, "memory-ranges"), backing); err != nil {
		t.Fatal(err)
	}
	upper := filepath.Join(dir, "upper.raw")
	if err := loom.CopyFile(filepath.Join(capture, "disk.raw"), upper, 0o600); err != nil {
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
	config["vsock"].(map[string]any)["socket"] = vm.Vsock
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
	maps, err := os.ReadFile(fmt.Sprintf("/proc/%d/maps", vm.command.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "maps.txt"), maps, 0o600); err != nil {
		t.Fatal(err)
	}
	identity, _ := json.Marshal(
		map[string]any{
			"device": stat.Dev,
			"inode":  stat.Ino,
			"bytes":  stat.Size,
		},
	)
	if err := os.WriteFile(filepath.Join(dir, "backing.json"), identity, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loom.VerifyCOWFileMappings(bytes.NewReader(maps), &stat, uint64(stat.Size)); err != nil {
		t.Fatalf("VMM did not retain private file-backed RAM: %v", err)
	}
	vm.api(t, "vm.resume", nil)
	return vm
}
