package loom

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestCheckLocalReaderIdentitiesUnlinkedReader(t *testing.T) {
	// A reader that outlives every directory entry still pins the artifact
	// inode; the persisted receipt keeps retirement blocked until it exits.
	dir := t.TempDir()
	path := filepath.Join(dir, "memory-ranges")
	if err := os.WriteFile(path, []byte("frozen"), 0o600); err != nil {
		t.Fatal(err)
	}
	intent, err := InspectDeletion(context.Background(), dir, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = reader.Close()
	}()
	if err = os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err = CheckLocalReaderIdentities(intent); err == nil || !strings.Contains(err.Error(), "still open") {
		t.Fatal("unlinked reader escaped the receipt check", err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err = CheckLocalReaderIdentities(intent); err != nil {
		t.Fatal("released reader still blocks retirement", err)
	}
}

func TestCheckLocalReaderIdentitiesRecycledInode(t *testing.T) {
	// Once the receipt names are gone the filesystem is free to recycle their
	// inodes; an unrelated open file owning the identity is not a reader.
	unrelated, err := os.CreateTemp(t.TempDir(), "recycled")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = unrelated.Close()
	}()
	info, err := unrelated.Stat()
	if err != nil {
		t.Fatal(err)
	}
	intent := DeletionIntent{
		Format: 1,
		Path:   filepath.Join(t.TempDir(), "retired"),
		Nodes: map[string]FileIdentity{
			"ch/memory-ranges": IdentityOf(info),
		},
	}
	if err = CheckLocalReaderIdentities(intent); err != nil {
		t.Fatal("recycled inode blocked retirement", err)
	}
}

func TestCheckLocalReaderIdentitiesMappedReader(t *testing.T) {
	// A private mapping of the artifact keeps its pages alive after unlink;
	// the same identity on an unrelated mapping is inode recycling instead.
	mapped, recycled := t.TempDir(), t.TempDir()
	path := filepath.Join(mapped, "memory-ranges")
	if err := os.WriteFile(path, []byte("frozen"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mapping, err := unix.Mmap(int(file.Fd()), 0, len("frozen"), unix.PROT_READ, unix.MAP_PRIVATE)
	if err != nil {
		t.Fatal(err)
	}
	// The mapping alone pins the inode; the descriptor closes right away.
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = unix.Munmap(mapping)
	}()
	intent, err := InspectDeletion(context.Background(), mapped, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if err = CheckLocalReaderIdentities(intent); err == nil || !strings.Contains(err.Error(), "still mapped") {
		t.Fatal("mapped reader escaped the receipt check", err)
	}
	intent.Path = filepath.Join(recycled, "retired")
	intent.Nodes = map[string]FileIdentity{
		"memory-ranges": intent.Nodes["memory-ranges"],
	}
	if err = CheckLocalReaderIdentities(intent); err != nil {
		t.Fatal("recycled mapping blocked retirement", err)
	}
}
