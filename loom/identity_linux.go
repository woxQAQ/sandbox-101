package loom

import (
	"encoding/hex"
	"os"
	"strings"
	"syscall"
)

// FileIdentity pins an observed inode and mode. It is evidence for the exact
// object; the caller separately determines whether an effect is authorized.
type FileIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	Mode   uint32 `json:"mode"`
}

func IdentityOf(info os.FileInfo) FileIdentity {
	stat := info.Sys().(*syscall.Stat_t)
	return FileIdentity{
		Device: uint64(stat.Dev),
		Inode:  stat.Ino,
		Mode:   uint32(info.Mode()),
	}
}

// ValidResourceID validates the fixed identifiers used in resource paths. It
// checks syntax only and grants no ownership, recovery or deletion authority.
func ValidResourceID(id, prefix string) bool {
	if !strings.HasPrefix(id, prefix) || len(id) != len(prefix)+32 {
		return false
	}
	_, err := hex.DecodeString(id[len(prefix):])
	return err == nil
}
