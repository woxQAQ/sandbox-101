package host

import (
	"fmt"
	"os"
	"runtime"
	"strings"
)

// CPUIdentity freezes the same platform fields used by capture/restore compatibility.
func CPUIdentity() string {
	b, _ := os.ReadFile("/proc/cpuinfo")
	first, _, _ := strings.Cut(string(b), "\n\n")
	var fields []string
	for line := range strings.SplitSeq(first, "\n") {
		key, _, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(key) {
		case "vendor_id", "cpu family", "model", "stepping", "microcode", "flags", "Features", "CPU implementer", "CPU architecture", "CPU variant", "CPU part", "CPU revision":
			fields = append(fields, strings.TrimSpace(line))
		}
	}
	if len(fields) > 0 {
		return strings.Join(fields, "\n")
	}
	return fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
}
