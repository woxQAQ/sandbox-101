package egress

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestOwnedStaleSocketRecoveryAndLiveSocketProtection(t *testing.T) {
	dir, err := os.MkdirTemp("", "egress-owner-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	base := filepath.Join(dir, "vsock")
	stale, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: base + "_2049", Net: "unix"},
	)
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	var proofCalls atomic.Int32
	m, err := New(
		Options{
			RecoverStaleSocket: func(b Binding, path string, info os.FileInfo) bool {
				proofCalls.Add(1)
				return b.SandboxID == "s" && path == base+"_2049" &&
					info.Mode()&os.ModeSocket != 0
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Bind(
		context.Background(),
		"s",
		"t",
		base,
		Policy{Mode: "relay"},
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if proofCalls.Load() != 1 {
		t.Fatal("stale socket was not authenticated")
	}
	// Even an ownership callback cannot authorize removing a live listener.
	other, err := New(
		Options{
			RecoverStaleSocket: func(Binding, string, os.FileInfo) bool { return true },
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.Bind(
		context.Background(),
		"s",
		"t",
		base,
		Policy{Mode: "relay"},
		nil,
	); err == nil {
		t.Fatal("replaced a live socket")
	}
	// Revoke must not unlink a different inode installed at the same pathname.
	if err := os.Remove(base + "_2049"); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: base + "_2049", Net: "unix"},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := m.Revoke("s"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(base + "_2049"); err != nil {
		t.Fatal("revoke removed replacement socket")
	}
}
