package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/woxQAQ/sandbox-engine/api"
	"golang.org/x/sys/unix"
)

type cowRAMMapping struct {
	GPA        uint64 `json:"gpa"`
	Size       uint64 `json:"size"`
	ZoneID     string `json:"zone_id"`
	VirtioMem  bool   `json:"virtio_mem"`
	FileOffset uint64 `json:"file_offset"`
}
type cowMemoryRange struct {
	GPA    uint64 `json:"gpa"`
	Length uint64 `json:"length"`
}

// These fields mirror the pinned CH MemoryManagerSnapshotData and SnapshotData.
// Static private RAM snapshots must contain one full range per anonymous region;
// this excludes every saved-range condition that triggers CH's eager fallback.
func validateCOWTopology(dir string, memoryBytes uint64) error {
	file, err := os.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (64<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 64<<20 {
		return api.Fail(api.Incompatible, "snapshot state exceeds validation limit")
	}
	var snapshot struct {
		Snapshots map[string]struct {
			Data *struct {
				State string `json:"state"`
			} `json:"snapshot_data"`
		} `json:"snapshots"`
	}
	if err = json.Unmarshal(data, &snapshot); err != nil {
		return api.Fail(api.Incompatible, "invalid snapshot state")
	}
	manager := snapshot.Snapshots["memory-manager"]
	if manager.Data == nil {
		return api.Fail(api.Incompatible, "snapshot memory manager state missing")
	}
	var state struct {
		MemoryRanges struct {
			Data []cowMemoryRange `json:"data"`
		} `json:"memory_ranges"`
		Mappings   []cowRAMMapping `json:"guest_ram_mappings"`
		BootRAM    uint64          `json:"boot_ram"`
		CurrentRAM uint64          `json:"current_ram"`
	}
	if err = json.Unmarshal([]byte(manager.Data.State), &state); err != nil {
		return api.Fail(api.Incompatible, "invalid saved RAM topology")
	}
	if state.BootRAM != memoryBytes || state.CurrentRAM != memoryBytes || len(state.Mappings) == 0 || len(state.Mappings) != len(state.MemoryRanges.Data) {
		return api.Fail(api.Incompatible, "COW requires complete static RAM ranges")
	}
	page := uint64(os.Getpagesize())
	sort.Slice(state.Mappings, func(i, j int) bool { return state.Mappings[i].GPA < state.Mappings[j].GPA })
	var total, end uint64
	for i, mapping := range state.Mappings {
		if mapping.Size == 0 || mapping.GPA%page != 0 || mapping.Size%page != 0 || mapping.GPA+mapping.Size < mapping.GPA || mapping.VirtioMem || mapping.ZoneID != "mem0" || mapping.FileOffset != 0 || (i > 0 && mapping.GPA < end) {
			return api.Fail(api.Incompatible, "unsupported COW RAM region")
		}
		saved := state.MemoryRanges.Data[i]
		if saved.GPA != mapping.GPA || saved.Length != mapping.Size || total+mapping.Size < total {
			return api.Fail(api.Incompatible, "saved range does not cover one private RAM region")
		}
		total += mapping.Size
		end = mapping.GPA + mapping.Size
	}
	info, err := os.Stat(filepath.Join(dir, "memory-ranges"))
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || total != memoryBytes || uint64(info.Size()) != total {
		return api.Fail(api.Incompatible, "COW memory file length does not match saved ranges")
	}
	return nil
}

// Verify the actual retained inode is mapped MAP_PRIVATE over every saved byte.
// An accepted API request alone cannot establish that CH avoided eager copying.
func verifyCOWFileMappings(maps io.Reader, backing *unix.Stat_t, size uint64) error {
	type extent struct{ offset, end uint64 }
	var extents []extent
	scanner := bufio.NewScanner(maps)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			return api.Fail(api.ManualRecovery, "invalid process map entry")
		}
		inode, e := strconv.ParseUint(fields[4], 10, 64)
		if e != nil {
			return e
		}
		majorText, minorText, ok := strings.Cut(fields[3], ":")
		if !ok {
			return api.Fail(api.ManualRecovery, "invalid process map device")
		}
		major, e := strconv.ParseUint(majorText, 16, 32)
		if e != nil {
			return e
		}
		minor, e := strconv.ParseUint(minorText, 16, 32)
		if e != nil {
			return e
		}
		if inode != backing.Ino || unix.Mkdev(uint32(major), uint32(minor)) != uint64(backing.Dev) {
			continue
		}
		startText, endText, ok := strings.Cut(fields[0], "-")
		if !ok || fields[1] != "rw-p" {
			return api.Fail(api.Incompatible, "COW backing is not mapped private writable")
		}
		start, e := strconv.ParseUint(startText, 16, 64)
		if e != nil {
			return e
		}
		end, e := strconv.ParseUint(endText, 16, 64)
		if e != nil {
			return e
		}
		offset, e := strconv.ParseUint(fields[2], 16, 64)
		if e != nil {
			return e
		}
		if end <= start || end-start > size || offset > size-(end-start) {
			return api.Fail(api.Incompatible, "COW mapping exceeds retained memory file")
		}
		extents = append(extents, extent{offset, offset + end - start})
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	sort.Slice(extents, func(i, j int) bool { return extents[i].offset < extents[j].offset })
	var covered uint64
	for _, extent := range extents {
		if extent.offset != covered {
			return api.Fail(api.Incompatible, "COW memory mappings have gaps or aliases")
		}
		covered = extent.end
	}
	if size == 0 || covered != size {
		return api.Fail(api.Incompatible, "VMM did not map the complete retained COW backing")
	}
	return nil
}

func (b *CloudHypervisor) verifyCOWMappings(r Resource) error {
	process, alive, err := b.process(r)
	if err != nil {
		return err
	}
	if !alive {
		return api.Fail(api.ManualRecovery, "restored VMM exited before COW verification")
	}
	var backing unix.Stat_t
	if err = unix.Stat(filepath.Join(r.RestoreDir, "memory-ranges"), &backing); err != nil {
		return err
	}
	maps, err := os.Open(fmt.Sprintf("/proc/%d/maps", process.PID))
	if err != nil {
		return err
	}
	defer maps.Close()
	if err = verifyCOWFileMappings(maps, &backing, uint64(backing.Size)); err != nil {
		return err
	}
	start, _, err := procStart(process.PID)
	if err != nil || start != process.StartTime {
		return api.Fail(api.ManualRecovery, "VMM identity changed during COW verification")
	}
	return nil
}
