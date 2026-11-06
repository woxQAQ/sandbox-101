package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestCOWSavedTopologyRejectsEagerFallback(t *testing.T) {
	page := uint64(os.Getpagesize())
	for _, name := range []string{"valid", "missing", "unaligned", "spanning", "overlap", "file-offset", "virtio-mem", "zone", "short-file", "empty"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			mappings := []cowRAMMapping{{GPA: 0, Size: page, ZoneID: "mem0"}, {GPA: 4 * page, Size: page, ZoneID: "mem0"}}
			ranges := []cowMemoryRange{{GPA: 0, Length: page}, {GPA: 4 * page, Length: page}}
			length := 2 * page
			switch name {
			case "unaligned":
				ranges[1].GPA++
			case "spanning":
				ranges[0].Length = 2 * page
			case "overlap":
				mappings[1].GPA, ranges[1].GPA = 0, 0
			case "file-offset":
				mappings[0].FileOffset = page
			case "virtio-mem":
				mappings[0].VirtioMem = true
			case "zone":
				mappings[0].ZoneID = "other"
			case "short-file":
				length = page
			case "empty":
				ranges = nil
			}
			state := map[string]any{"memory_ranges": map[string]any{"data": ranges}, "guest_ram_mappings": mappings, "boot_ram": 2 * page, "current_ram": 2 * page}
			stateJSON, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := map[string]any{"snapshots": map[string]any{"memory-manager": map[string]any{"snapshot_data": map[string]any{"state": string(stateJSON)}}}}
			if name == "missing" {
				snapshot = map[string]any{"snapshots": map[string]any{}}
			}
			if err = writeJSON(filepath.Join(dir, "state.json"), snapshot); err != nil {
				t.Fatal(err)
			}
			file, err := os.Create(filepath.Join(dir, "memory-ranges"))
			if err != nil {
				t.Fatal(err)
			}
			err = file.Truncate(int64(length))
			file.Close()
			if err != nil {
				t.Fatal(err)
			}
			err = validateCOWTopology(dir, 2*page)
			if name == "valid" && err != nil {
				t.Fatal(err)
			}
			if name != "valid" && err == nil {
				t.Fatal("ineligible saved topology accepted")
			}
		})
	}
}

func TestCOWActualMappingsRequireCompletePrivateBacking(t *testing.T) {
	for _, name := range []string{"private", "shared", "partial", "eager-copy"} {
		t.Run(name, func(t *testing.T) {
			// memfd provides a native file identity even when the test's temporary
			// directory is overlayfs, whose mmap device can differ from stat.
			fd, err := unix.MemfdCreate("cow-test", unix.MFD_CLOEXEC)
			if err != nil {
				t.Fatal(err)
			}
			file := os.NewFile(uintptr(fd), "backing")
			defer file.Close()
			size := 2 * os.Getpagesize()
			if err = file.Truncate(int64(size)); err != nil {
				t.Fatal(err)
			}
			var stat unix.Stat_t
			if err = unix.Fstat(int(file.Fd()), &stat); err != nil {
				t.Fatal(err)
			}
			if name != "eager-copy" {
				flags, length := unix.MAP_PRIVATE, size
				if name == "shared" {
					flags = unix.MAP_SHARED
				}
				if name == "partial" {
					length /= 2
				}
				mapping, err := unix.Mmap(int(file.Fd()), 0, length, unix.PROT_READ|unix.PROT_WRITE, flags)
				if err != nil {
					t.Fatal(err)
				}
				defer unix.Munmap(mapping)
				mapping[0] = 42
				if name == "private" {
					var original [1]byte
					if _, err = file.ReadAt(original[:], 0); err != nil || original[0] != 0 {
						t.Fatal("private write changed retained backing", err)
					}
				}
			}
			maps, err := os.Open("/proc/self/maps")
			if err != nil {
				t.Fatal(err)
			}
			defer maps.Close()
			err = verifyCOWFileMappings(maps, &stat, uint64(size))
			if name == "private" && err != nil {
				t.Fatal(err)
			}
			if name != "private" && err == nil {
				t.Fatal("non-COW or incomplete actual mappings accepted")
			}
		})
	}
}
