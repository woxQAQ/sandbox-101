//go:build linux && guest_image

package native

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/tests/fixtures"
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
	hypervisor := fixtures.RequiredGuestInput(t, "SANDBOX_GUEST_HYPERVISOR")
	kernel := fixtures.RequiredGuestInput(t, "SANDBOX_GUEST_KERNEL")
	guest := fixtures.RequiredGuestInput(t, "SANDBOX_GUEST_DIR")
	rootfs := fixtures.RequiredGuestInput(t, "SANDBOX_GUEST_ROOTFS")
	artifacts := fixtures.RequiredGuestInput(t, "SANDBOX_GUEST_IMAGE_ARTIFACTS")
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
	fixtures.RunImageTool(t, "readelf", "-l", hypervisor)
	for _, executable := range []string{
		hypervisor,
		filepath.Join(guest, "sandbox-agent"),
		filepath.Join(guest, "sandbox-init"),
	} {
		for _, args := range [][]string{
			{"-l", executable},
			{"-d", executable},
		} {
			out := fixtures.RunImageTool(t, "readelf", args...)
			if bytes.Contains(out, []byte("INTERP")) || bytes.Contains(out, []byte("NEEDED")) {
				t.Fatalf("guest image fixture executable is not static: %s", executable)
			}
		}
	}
	layout := filepath.Join(run, "oci")
	if err := os.Mkdir(layout, 0o700); err != nil {
		t.Fatal(err)
	}
	base := fixtures.GuestRuntimeLayer(t, layout, fixtures.GuestMaterials{
		Kernel:         kernel,
		GuestDirectory: guest,
		RootFS:         rootfs,
	})
	meta := fixtures.GuestMetadataLayers(t, layout)
	cross := fixtures.HardEntry("fixture/cross-layer-hard", "fixture/regular")
	cross.Header.Mode = 0o644
	root := fixtures.DirEntry(".")
	root.Header.Uid, root.Header.Gid, root.Header.Mode = 0, 0, 0o755
	root.Header.ModTime = time.Unix(1700000001, 123456789)
	root.Header.Xattrs = map[string]string{
		"user.shared-root": "retained",
	}
	broaderEntries := []fixtures.TarEntry{
		root,
		cross,
		fixtures.FileEntry("fixture/implicit/deep/value", "implicit"),
		fixtures.FileEntry("fixture/duplicate", "old"),
		fixtures.FileEntry("fixture/duplicate", "last"),
	}
	for i := range broaderEntries {
		if broaderEntries[i].Header.Typeflag == tar.TypeReg {
			broaderEntries[i].Header.Mode = 0o644
		}
	}
	broader := fixtures.PutOCILayer(t, layout, broaderEntries, false)
	layers := append(append([]fixtures.OCILayerFixture{
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
			e := fixtures.NativeImageFixtureAt(t, store)
			defer e.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
			defer cancel()
			var image string
			if materialization == host.LayeredEROFSLayout {
				// The shared metadata composer also resolves true forward chains,
				// which the explicit umoci compatibility path does not accept.
				forwardEntries := []fixtures.TarEntry{
					fixtures.HardEntry("fixture/forward-a", "fixture/forward-b"),
					fixtures.HardEntry("fixture/forward-b", "fixture/forward-target"),
					fixtures.FileEntry("fixture/forward-target", "forward"),
				}
				for i := range forwardEntries {
					forwardEntries[i].Header.Mode = 0o644
				}
				forward := fixtures.PutOCILayer(t, layout, forwardEntries, false)
				image = fixtures.PutOCIManifest(
					t,
					layout,
					"actual-systemd-forward",
					append(append([]fixtures.OCILayerFixture{}, layers...), forward),
				)
			} else {
				// Compatibility pins umask 077, so its implicit parents are
				// private. Explicit headers make this path workload-readable;
				// the shared path above retains its implicit-parent checks.
				directories := []fixtures.TarEntry{
					fixtures.DirEntry("fixture/implicit"),
					fixtures.DirEntry("fixture/implicit/deep"),
				}
				for i := range directories {
					directories[i].Header.Mode = 0o755
				}
				readable := fixtures.PutOCILayer(t, layout, directories, false)
				image = fixtures.PutOCIManifest(
					t,
					layout,
					"actual-systemd-compat-parents",
					append(append([]fixtures.OCILayerFixture{}, layers...), readable),
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
			fixtures.RunImageTool(t, "mkfs.ext4", "-q", "-F", upper)
			vm := fixtures.StartGuestVM(t, hypervisor, filepath.Join(dir, "source"))
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
			vm.Api(t, "vm.create", map[string]any{
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
			vm.Api(t, "vm.boot", nil)
			vm.Ready(t, ctx, false)
			vm.Rpc(t, fixtures.GuestRequest{
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
			fileViewChecks := fixtures.GuestFileViewChecks
			if materialization == host.LayeredEROFSLayout {
				fileViewChecks += `
[ "$(cat /fixture/forward-a)" = forward ]
[ "$(stat -c %i /fixture/forward-a)" = "$(stat -c %i /fixture/forward-b)" ]
[ "$(stat -c %i /fixture/forward-a)" = "$(stat -c %i /fixture/forward-target)" ]
`
			}
			vm.Exec(t, fileViewChecks)
			vm.TreeRoundtrip(t)
			vm.Exec(t, "printf 'original' > /workspace/branch; sync")
			capture := vm.Capture(t, upper, "first")
			clone := fixtures.RestoreGuestVM(t, hypervisor, filepath.Join(dir, "clone"), capture)
			clone.Ready(t, ctx, true)
			if err := os.RemoveAll(capture); err != nil {
				t.Fatal(err)
			}
			clone.Exec(t, "test \"$(cat /workspace/branch)\" = original; printf 'clone' > /workspace/branch; sync")
			vm.Exec(t, "test \"$(cat /workspace/branch)\" = original; printf 'source' > /workspace/branch; sync")
			second := clone.Capture(t, filepath.Join(clone.Dir, "upper.raw"), "second")
			clone.Stop()
			vm.Stop()
			grandchild := fixtures.RestoreGuestVM(t, hypervisor, filepath.Join(dir, "grandchild"), second)
			grandchild.Ready(t, ctx, true)
			grandchild.Exec(t, "test \"$(cat /workspace/branch)\" = clone")
			grandchild.Exec(t, fileViewChecks)
			t.Log(
				"PASS actual OCI -> EROFS -> initramfs -> systemd -> vsock file view; COW clone, independent disks and COW resnapshot",
			)
		})
	}
}

// This exercises the userspace handoff through actual OCI materialization,
// without booting a VM or consulting a package database.
func TestGuestRootfsOCI(t *testing.T) {
	fixtures.RequireEROFS(t)
	for _, materialization := range []string{
		host.LayeredEROFSLayout,
		host.MergedOCIEROFSLayout,
	} {
		t.Run(materialization, func(t *testing.T) {
			if materialization == host.MergedOCIEROFSLayout {
				fixtures.RequireMergedOCI(t)
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
			layer := fixtures.GuestRuntimeLayer(t, layout, fixtures.GuestMaterials{
				Kernel:         filepath.Join(guest, "kernel"),
				GuestDirectory: guest,
				RootFS:         rootfs,
			})
			image := fixtures.PutOCIManifest(t, layout, "provider-independent-runtime", []fixtures.OCILayerFixture{
				layer,
			})
			e := fixtures.NewNativeImageFixture(t)
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
				tree = fixtures.ExtractMergedManifest(t, e, m)
			} else {
				tree = fixtures.ExtractManifest(t, e, m)
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
