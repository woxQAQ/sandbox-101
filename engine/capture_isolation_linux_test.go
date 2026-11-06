//go:build capture_isolation

package engine

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestCaptureIsolationWriter(t *testing.T) {
	action := os.Getenv("CAPTURE_WRITER_ACTION")
	if action == "" {
		return
	}
	if os.Getuid() != 1 || os.Getgid() != 1 {
		t.Fatal("writer must run with distinct mapped UID/GID 1")
	}
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "CapEff:") ||
			strings.HasPrefix(line, "CapPrm:") {
			if strings.Fields(line)[1] != "0000000000000000" {
				t.Fatal("writer retained capabilities", line)
			}
		}
	}
	path := os.Getenv("CAPTURE_WRITER_PATH")
	switch action {
	case "artifact":
		path = filepath.Join(path, "ch", "memory-ranges")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		var err error
		switch os.Getenv("CAPTURE_WRITER_KIND") {
		case "fifo":
			err = syscall.Mkfifo(path, 0o600)
		case "symlink":
			err = os.Symlink("../publication/disk.raw", path)
		case "directory":
			err = os.Mkdir(path, 0o700)
		case "unknown":
			err = os.WriteFile(
				filepath.Join(filepath.Dir(path), "unexpected"),
				nil,
				0o600,
			)
		case "oversized":
			var f *os.File
			f, err = os.Create(path)
			if err == nil {
				err = f.Truncate(129 << 20)
				f.Close()
			}
		default:
			t.Fatal("unknown artifact kind")
		}
		if err != nil {
			t.Fatal(err)
		}
	case "parents":
		if err := os.RemoveAll(filepath.Join(path, "ch")); err == nil {
			t.Fatal("writer removed leaf directory entry")
		}
		if err := os.WriteFile(
			filepath.Join(path, "intruder"),
			nil,
			0o600,
		); err == nil {
			t.Fatal("writer changed parent")
		}
		if err := os.WriteFile(
			filepath.Join(path, "ch", "config.json"),
			[]byte("writer"),
			0o600,
		); err != nil {
			t.Fatal(err)
		}
	case "retain":
		f, err := os.OpenFile(
			filepath.Join(path, "ch", "memory-ranges"),
			os.O_RDWR,
			0,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		fmt.Fprintln(os.Stdout, "FD_READY")
		if _, err = bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
			t.Fatal(err)
		}
		if _, err = f.WriteAt([]byte("mutated"), 0); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(
			filepath.Join(
				os.Getenv("CAPTURE_PUBLICATION_PATH"),
				"ch",
				"memory-ranges",
			),
			[]byte("bad"),
			0o600,
		); err == nil {
			t.Fatal("writer changed publication")
		} else if !errors.Is(
			err,
			syscall.EACCES,
		) {
			t.Fatalf("expected publication permission denial, got %v", err)
		}
	default:
		t.Fatal("unknown writer action")
	}
}

func isolationFixture(t *testing.T) (Resource, *captureBoundary) {
	t.Helper()
	if os.Getenv("CAPTURE_ISOLATION_INSIDE") != "1" || os.Geteuid() != 0 {
		t.Fatal(
			"run make capture-isolation-e2e: mapped root and writer namespaces required",
		)
	}
	mapping, err := os.ReadFile("/proc/self/uid_map")
	if err != nil || len(strings.Fields(string(mapping))) < 6 {
		t.Fatalf("two UID ranges required: %s %v", mapping, err)
	}
	mapping, err = os.ReadFile("/proc/self/gid_map")
	if err != nil || len(strings.Fields(string(mapping))) < 6 {
		t.Fatalf("two GID ranges required: %s %v", mapping, err)
	}
	dir, err := os.MkdirTemp("", "capture-isolation-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if err = os.Chmod(dir, 0o711); err != nil {
		t.Fatal(err)
	}
	r := Resource{Dir: dir}
	if err = os.Mkdir(filepath.Join(dir, "capture"), 0o700); err != nil {
		t.Fatal(err)
	}
	c, err := prepareCapture(r, "op-0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	backend := &CloudHypervisor{opts: CloudOptions{UID: 1, GID: 1}}
	if err = backend.prepareCaptureWriter(
		r,
		filepath.Join(c.Path, "ch"),
	); err != nil {
		t.Fatal(err)
	}
	return r, c
}

func writerCommand(t *testing.T, c *captureBoundary, action string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// The test binary's host-created enclosing directory need not be traversable
	// by UID 1. A private fixture copy provides an executable visible to it.
	binary := filepath.Join(filepath.Dir(filepath.Dir(c.Path)), "writer.test")
	if _, err = os.Stat(binary); os.IsNotExist(err) {
		if err = copyFile(exe, binary, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(
		binary,
		"-test.run=^TestCaptureIsolationWriter$",
		"-test.v",
	)
	cmd.Env = append(
		os.Environ(),
		"CAPTURE_WRITER_ACTION="+action,
		"CAPTURE_WRITER_PATH="+c.Path,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{Uid: 1, Gid: 1, NoSetGroups: true},
	}
	return cmd
}

func TestCaptureIsolationParentOwnership(t *testing.T) {
	r, c := isolationFixture(t)
	if out, err := writerCommand(t, c, "parents").CombinedOutput(); err != nil {
		t.Fatalf("writer: %v %s", err, out)
	}
	for _, path := range []string{filepath.Join(r.Dir, "capture"), c.Path, filepath.Join(c.Path, "publication")} {
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		s := st.Sys().(*syscall.Stat_t)
		t.Logf(
			"parent=%s uid=%d gid=%d mode=%04o dev=%d inode=%d",
			path,
			s.Uid,
			s.Gid,
			st.Mode().Perm(),
			s.Dev,
			s.Ino,
		)
		if s.Uid != 0 || st.Mode().Perm()&0o022 != 0 {
			t.Fatalf("untrusted parent %s: %+v", path, s)
		}
	}
}

func TestCaptureIsolationRejectsEntryReplacement(t *testing.T) {
	r, c := isolationFixture(t)
	sentinel := filepath.Join(r.Dir, "sentinel")
	if err := os.Mkdir(sentinel, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinelData := filepath.Join(sentinel, "unchanged")
	if err := os.WriteFile(
		sentinelData,
		[]byte("sentinel-content"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(sentinel)
	if err := os.Remove(filepath.Join(c.Path, "ch")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sentinel, filepath.Join(c.Path, "ch")); err != nil {
		t.Fatal(err)
	}
	if err := delegateCapture(
		r,
		filepath.Join(c.Path, "ch"),
		1,
		1,
	); err == nil {
		t.Fatal("followed replacement symlink")
	}
	after, _ := os.Stat(sentinel)
	if gcNode(before) != gcNode(after) ||
		after.Sys().(*syscall.Stat_t).Uid != 0 {
		t.Fatal("sentinel metadata changed")
	}
	data, err := os.ReadFile(sentinelData)
	if err != nil || string(data) != "sentinel-content" {
		t.Fatalf("sentinel content changed: %q %v", data, err)
	}
	if err := delegateCapture(r, sentinel, 1, 1); err == nil {
		t.Fatal("accepted destination outside capture")
	}
}

func writeCaptureFixture(t *testing.T, c *captureBoundary) {
	t.Helper()
	for _, name := range []string{"config.json", "state.json", "memory-ranges"} {
		path := filepath.Join(c.Path, "ch", name)
		if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(path, 1, 1); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCaptureIsolationPublishedFilesAreIndependent(t *testing.T) {
	_, c := isolationFixture(t)
	writeCaptureFixture(t, c)
	publication := filepath.Join(
		filepath.Dir(filepath.Dir(c.Path)),
		"published",
	)
	cmd := writerCommand(t, c, "retain")
	cmd.Env = append(cmd.Env, "CAPTURE_PUBLICATION_PATH="+publication)
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	scanner := bufio.NewScanner(output)
	ready := false
	for scanner.Scan() {
		if scanner.Text() == "FD_READY" {
			ready = true
			break
		}
	}
	if !ready {
		t.Fatal("writer did not retain fd")
	}
	if err = materializeCapture(c, 128); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(c.Path, "ch", "memory-ranges")
	dst := filepath.Join(c.Path, "publication", "ch", "memory-ranges")
	a, _ := os.Stat(src)
	b, _ := os.Stat(dst)
	t.Logf("source=%+v publication=%+v", gcNode(a), gcNode(b))
	if os.SameFile(a, b) || b.Sys().(*syscall.Stat_t).Uid != 0 {
		t.Fatal("publication did not create root-owned independent inode")
	}
	before, err := hashTree(filepath.Join(c.Path, "publication"))
	if err != nil {
		t.Fatal(err)
	}
	if err = sealTree(filepath.Join(c.Path, "publication")); err != nil {
		t.Fatal(err)
	}
	if err = syncTree(filepath.Join(c.Path, "publication")); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(
		filepath.Join(c.Path, "publication"),
		publication,
	); err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(input, "mutate")
	input.Close()
	for scanner.Scan() {
		t.Log(scanner.Text())
	}
	if err = cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	t.Logf("publication hashes after retained source FD mutation: %v", before)
	if err = verifyFiles(publication, before); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(src)
	if string(data) != "mutatedl" {
		t.Fatalf("retained writer did not mutate source: %q", data)
	}
}

func TestCaptureIsolationRejectsNonregularArtifacts(t *testing.T) {
	for _, kind := range []string{"fifo", "symlink", "directory", "unknown", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			_, c := isolationFixture(t)
			writeCaptureFixture(t, c)
			cmd := writerCommand(t, c, "artifact")
			cmd.Env = append(cmd.Env, "CAPTURE_WRITER_KIND="+kind)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("writer failed: %v %s", err, out)
			}
			if err := materializeCapture(c, 128); err == nil {
				t.Fatal("accepted invalid artifact")
			}
		})
	}
}
