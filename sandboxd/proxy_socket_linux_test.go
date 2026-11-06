package sandboxd

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

func proxySocketFixture(
	t *testing.T,
) (*Service, *fakeEngine, egress.Binding, string, os.FileInfo) {
	t.Helper()
	s, engine, _, principal := fixture(t)
	result := claimTest(t, s, principal, "claim", nil)
	dir, err := os.MkdirTemp("", "sbx-proxy-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	base := filepath.Join(dir, "vsock")
	peer, err := net.Listen("unix", base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.Close() })
	if os.Geteuid() == 0 {
		if err = os.Chown(base, 65534, 65534); err != nil {
			t.Fatal(err)
		}
	}
	engine.mu.Lock()
	vm := engine.vms[result.Sandbox.VMID]
	vm.VSockPath = base
	engine.vms[vm.ID] = vm
	engine.mu.Unlock()
	path := base + "_2049"
	listener, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: path, Net: "unix"},
	)
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	t.Cleanup(func() { listener.Close() })
	if err = os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	return s, engine, egress.Binding{
		SandboxID: result.Sandbox.ID,
		TenantID:  principal.TenantID,
		VSockPath: base,
		Mode:      "relay",
	}, path, info
}

func TestProxySocketGrantsOnlyVerifiedVMMIdentity(t *testing.T) {
	s, _, binding, path, before := proxySocketFixture(t)
	if err := s.PrepareProxySocket(
		context.Background(),
		binding,
		path,
		before,
	); err != nil {
		t.Fatal(err)
	}
	after, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := os.Lstat(binding.VSockPath)
	if err != nil {
		t.Fatal(err)
	}
	actual, expected := after.Sys().(*syscall.Stat_t), peer.Sys().(*syscall.Stat_t)
	if !os.SameFile(before, after) || after.Mode().Perm() != 0o600 ||
		actual.Uid != expected.Uid ||
		actual.Gid != expected.Gid {
		t.Fatal("ownership handoff changed the inode, mode, or VMM identity")
	}
}

func TestProxySocketPreservesReplacementAndRejectsUnverifiedVM(t *testing.T) {
	s, engine, binding, path, before := proxySocketFixture(t)
	b, err := s.Get(
		context.Background(),
		Principal{Root: true},
		binding.SandboxID,
	)
	if err != nil {
		t.Fatal(err)
	}
	engine.mu.Lock()
	vm := engine.vms[b.VMID]
	vm.Verified = false
	engine.vms[vm.ID] = vm
	engine.mu.Unlock()
	if err = s.PrepareProxySocket(
		context.Background(),
		binding,
		path,
		before,
	); !isCode(
		err,
		api.Conflict,
	) {
		t.Fatal("unverified VM received proxy access", err)
	}
	engine.mu.Lock()
	vm.Verified = true
	engine.vms[vm.ID] = vm
	engine.mu.Unlock()
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(filepath.Dir(path), "private-file")
	if err = os.WriteFile(target, []byte("retain"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if err = s.PrepareProxySocket(
		context.Background(),
		binding,
		path,
		before,
	); !isCode(
		err,
		api.Conflict,
	) {
		t.Fatal("replacement symlink was accepted", err)
	}
	info, err := os.Stat(target)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatal("replacement target permissions changed", err)
	}
}
