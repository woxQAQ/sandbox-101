//go:build linux && kernel_isolation

package engine

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const rootlessChildEnv = "SANDBOX_KERNEL_ISOLATION_CHILD"

// These acceptance tests require real unprivileged namespaces and cgroup v2
// delegation. They deliberately fail, rather than skip, when either is missing.
// Every kernel allocation happens after exec into new user/net/mount/PID
// namespaces. A PID namespace also reaps any owned descendant on abnormal exit.
func rootlessRun(t *testing.T, mode string) bool {
	t.Helper()
	if child := os.Getenv(rootlessChildEnv); child != "" {
		if child != mode {
			t.Skip("another acceptance subprocess")
		}
		rootlessRequireNamespaces(t)
		return true
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	env := os.Environ()
	for _, ns := range []string{"user", "net", "mnt"} {
		id, err := os.Readlink("/proc/self/ns/" + ns)
		if err != nil {
			t.Fatal(err)
		}
		env = append(env, "SANDBOX_KERNEL_PARENT_"+strings.ToUpper(ns)+"="+id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "unshare", "--user", "--map-root-user", "--net", "--mount", "--pid", "--fork", "--kill-child=KILL", "--mount-proc", "--propagation", "private", "--", exe, "-test.run=^"+t.Name()+"$", "-test.v", "-test.timeout=65s")
	cmd.Env = append(env, rootlessChildEnv+"="+mode, "GOMAXPROCS=2")
	out, err := cmd.CombinedOutput()
	t.Logf("private namespace acceptance:\n%s", out)
	if err != nil {
		t.Fatalf("real kernel %s acceptance failed: %v (context: %v)", mode, err, ctx.Err())
	}
	return false
}

func rootlessRequireNamespaces(t *testing.T) {
	t.Helper()
	for _, ns := range []string{"user", "net", "mnt"} {
		parent := os.Getenv("SANDBOX_KERNEL_PARENT_" + strings.ToUpper(ns))
		current, err := os.Readlink("/proc/self/ns/" + ns)
		if err != nil || parent == "" || current == parent {
			t.Fatalf("refusing kernel allocation without a new %s namespace: parent=%q current=%q error=%v", ns, parent, current, err)
		}
	}
}

func rootlessPrivateRun(t *testing.T) {
	t.Helper()
	// /run/current-system is on PATH on NixOS. Resolve it before masking /run;
	// all named netns mounts must live in this private tmpfs, never host /run.
	var paths []string
	for _, p := range filepath.SplitList(hostToolPath()) {
		if resolved, err := filepath.EvalSymlinks(p); err == nil {
			paths = append(paths, resolved)
		}
	}
	if err := os.Setenv("PATH", strings.Join(paths, string(os.PathListSeparator))); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("tmpfs", "/run", "tmpfs", unix.MS_NOSUID|unix.MS_NODEV, "mode=0755,size=4m"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir("/run/netns", 0755); err != nil {
		t.Fatal(err)
	}
}

func rootlessCommand(t *testing.T, name string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := command(ctx, name, args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func rootlessWrite(t *testing.T, path, value string) {
	t.Helper()
	// Do not create regular files in place of missing kernel control files.
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.WriteString(value)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("write %s: %v; close: %v", path, writeErr, closeErr)
	}
}

func rootlessRead(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(data))
}

func rootlessArtifact(t *testing.T, name string, data []byte) {
	t.Helper()
	dir := os.Getenv("SANDBOX_KERNEL_ISOLATION_ARTIFACTS")
	if dir == "" {
		return
	}
	if !filepath.IsAbs(dir) {
		t.Fatal("acceptance artifact directory must be absolute")
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
		t.Fatal(err)
	}
}
