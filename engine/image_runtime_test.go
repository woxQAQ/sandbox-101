package engine

import "testing"

func TestRestoreDisksPreserveFsmetaDeviceTableOrder(t *testing.T) {
	bindings := []ImageBinding{
		{GuestPath: "/sandbox/ro/fsmeta.erofs", Serial: "sandbox-root"},
		{GuestPath: "/sandbox/ro/data-0000.erofs", Serial: "sandbox-data-0000"},
		{GuestPath: "/sandbox/ro/data-0001.erofs", Serial: "sandbox-data-0001"},
	}
	disks := cloudDisks(bindings)
	if len(disks) != 4 || disks[0].ID != "root" || disks[1].ID != "data-0000" ||
		disks[2].ID != "data-0001" ||
		disks[3].ID != "upper" ||
		disks[3].ReadOnly {
		t.Fatal("runtime block roles do not preserve fsmeta device order")
	}
	for _, name := range []string{"valid", "swapped", "missing", "writable", "serial", "path"} {
		t.Run(name, func(t *testing.T) {
			saved := append([]cloudDisk(nil), disks...)
			switch name {
			case "swapped":
				saved[1], saved[2] = saved[2], saved[1]
			case "missing":
				saved = saved[:len(saved)-1]
			case "writable":
				saved[1].ReadOnly = false
			case "serial":
				saved[1].Serial = "sandbox-data-0001"
			case "path":
				saved[1].Path = saved[2].Path
			}
			err := validateSnapshotDisks(saved, disks)
			if name == "valid" && err != nil {
				t.Fatal(err)
			}
			if name != "valid" && err == nil {
				t.Fatal("incompatible saved block mapping accepted")
			}
		})
	}
}
