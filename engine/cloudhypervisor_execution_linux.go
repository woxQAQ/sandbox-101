package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/sandbox-engine/api"
)

type chHTTPError struct {
	Status         int
	Endpoint, Body string
}

func (e *chHTTPError) Error() string {
	return fmt.Sprintf("CH %s HTTP %d: %.4096s", e.Endpoint, e.Status, e.Body)
}

func (b *CloudHypervisor) Prepare(ctx context.Context, r Resource) error {
	if err := b.Check(ctx, r.Config); err != nil {
		return err
	}
	var image ImageManifest
	if err := readJSON(
		filepath.Join(r.ImageDir, "image.json"),
		&image,
	); err != nil {
		return api.Fail(api.Incompatible, "image not imported: %v", err)
	}
	if err := image.Verify(r.ImageDir, r.Config.Image); err != nil {
		return err
	}
	for _, dir := range []string{r.Dir, filepath.Dir(r.Disk), filepath.Join(r.Dir, "capture")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	if _, err := os.Stat(r.Disk); os.IsNotExist(err) {
		tmp := r.Disk + ".pending"
		_ = os.Remove(tmp)
		f, x := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if x != nil {
			return x
		}
		x = f.Truncate(r.Config.Shape.DiskMiB << 20)
		x = errors.Join(x, f.Close())
		if x != nil {
			return x
		}
		if _, x = command(
			ctx,
			"mkfs.ext4",
			"-q",
			"-F",
			"-L",
			"sandbox-upper",
			tmp,
		); x != nil {
			return x
		}
		if x = syncTree(filepath.Dir(r.Disk)); x != nil {
			return x
		}
		if x = os.Rename(tmp, r.Disk); x != nil {
			return x
		}
		if x = syncDir(filepath.Dir(r.Disk)); x != nil {
			return x
		}
	}
	return b.networkPrepare(ctx, r)
}

func (b *CloudHypervisor) Boot(
	ctx context.Context,
	r Resource,
	restore bool,
) error {
	obs, err := b.Inspect(ctx, r)
	if err != nil {
		return err
	}
	if obs.Booted {
		if restore {
			if err = b.verifyCOWMappings(r); err != nil {
				return err
			}
		}
		if obs.Paused {
			if err = b.arm(r); err != nil {
				return err
			}
			return b.Resume(ctx, r)
		}
		return nil
	}
	tap, err := openTapInNamespace(r)
	if err != nil {
		return err
	}
	defer func() { _ = tap.Close() }()
	if restore {
		if obs.Configured {
			return api.Fail(
				api.ManualRecovery,
				"partially loaded restore state requires inspection before retry",
			)
		}
		if err = b.validateCOW(r); err != nil {
			return err
		}
		body := map[string]any{
			"source_url":          "file:///restore",
			"prefault":            false,
			"memory_restore_mode": "CopyOnWrite",
			"resume":              false,
			"net_fds": []any{
				map[string]any{
					"id":      "net0",
					"num_fds": 1,
					"fds":     []int{int(tap.Fd())},
				},
			},
		}
		if _, err = b.api(
			ctx,
			r,
			"PUT",
			"vm.restore",
			body,
			[]int{int(tap.Fd())},
		); err != nil {
			return err
		}
		if err = b.verifyCOWMappings(r); err != nil {
			return err
		}
		if err = b.arm(r); err != nil {
			return err
		}
		return b.Resume(ctx, r)
	}
	image, bindings, err := runtimeImage(r)
	if err != nil {
		return err
	}
	dataParameter, err := image.DataKernelParameter()
	if err != nil {
		return err
	}
	cmdline := "console=hvc0 panic=-1 reboot=k ip=192.168.127.2::192.168.127.1:255.255.255.252:sandbox:eth0:off sandbox.lower_serial=sandbox-root sandbox.upper_serial=sandbox-write sandbox.dns=1.1.1.1"
	if dataParameter != "" {
		cmdline += " " + dataParameter
	}
	body := map[string]any{
		"cpus": map[string]any{
			"boot_vcpus": r.Config.Shape.VCPUs,
			"max_vcpus":  r.Config.Shape.VCPUs,
		},
		"memory": map[string]any{
			"size":      r.Config.Shape.MemoryMiB << 20,
			"shared":    false,
			"hugepages": false,
			"mergeable": false,
		},
		"payload": map[string]any{
			"kernel":    "/boot/kernel",
			"initramfs": "/boot/initramfs",
			"cmdline":   cmdline,
		},
		"disks": cloudDisks(bindings),
		"vsock": map[string]any{
			"id":     "vsock0",
			"cid":    3,
			"socket": "/run/vsock",
		},
		"rng":     map[string]any{"src": "/dev/urandom"},
		"console": map[string]any{"mode": "Tty"},
		"serial":  map[string]any{"mode": "Null"},
	}
	if !obs.Configured {
		if _, err = b.api(ctx, r, "PUT", "vm.create", body, nil); err != nil {
			return err
		}
	}
	if err = b.ensureColdNetwork(ctx, r, tap); err != nil {
		return err
	}
	if err = b.arm(r); err != nil {
		return err
	}
	_, err = b.api(ctx, r, "PUT", "vm.boot", nil, nil)
	return err
}

// vm.create deliberately discards SCM_RIGHTS in the pinned CH API. Cold-add the
// TAP before boot through vm.add-net, which accepts descriptors. Omitting mtu
// avoids SIOCSIFMTU in the unprivileged VMM; the engine configured it beforehand.
func (b *CloudHypervisor) ensureColdNetwork(
	ctx context.Context,
	r Resource,
	tap *os.File,
) error {
	data, err := b.api(ctx, r, "GET", "vm.info", nil, nil)
	if err != nil {
		return err
	}
	var info struct {
		Config struct {
			Net []struct {
				ID  string `json:"id"`
				Tap string `json:"tap"`
				FDs []int  `json:"fds"`
			} `json:"net"`
		} `json:"config"`
	}
	if err = json.Unmarshal(data, &info); err != nil {
		return err
	}
	if len(info.Config.Net) != 0 {
		// A daemon may have died after CH committed vm.add-net. Preserve the
		// already held queue rather than attaching a duplicate on replay.
		if len(info.Config.Net) != 1 || info.Config.Net[0].ID != "net0" ||
			info.Config.Net[0].Tap != "" ||
			len(info.Config.Net[0].FDs) != 1 ||
			info.Config.Net[0].FDs[0] < 0 {
			return api.Fail(
				api.ManualRecovery,
				"configured cold network has no verified TAP descriptor",
			)
		}
		return nil
	}
	network := map[string]any{
		"id":           "net0",
		"mac":          "02:00:00:00:00:02",
		"num_queues":   2,
		"queue_size":   256,
		"offload_tso":  false,
		"offload_ufo":  false,
		"offload_csum": false,
	}
	_, err = b.api(ctx, r, "PUT", "vm.add-net", network, []int{int(tap.Fd())})
	return err
}

func (b *CloudHypervisor) validateCOW(r Resource) error {
	var cfg map[string]json.RawMessage
	if err := readJSON(
		filepath.Join(r.RestoreDir, "config.json"),
		&cfg,
	); err != nil {
		return err
	}
	for _, k := range []string{"devices", "user_devices", "vdpa", "balloon", "pvmemcontrol", "memory_zones", "platform", "landlock_rules"} {
		if v := cfg[k]; len(v) > 0 && string(v) != "null" &&
			string(v) != "[]" &&
			string(v) != "false" {
			return api.Fail(
				api.Incompatible,
				"unapproved COW device/configuration %s",
				k,
			)
		}
	}
	var mem struct {
		Size        int64           `json:"size"`
		Shared      bool            `json:"shared"`
		Hugepages   bool            `json:"hugepages"`
		Mergeable   bool            `json:"mergeable"`
		HotplugSize int64           `json:"hotplug_size"`
		File        string          `json:"file"`
		Zones       json.RawMessage `json:"zones"`
	}
	if err := json.Unmarshal(cfg["memory"], &mem); err != nil {
		return err
	}
	if mem.Size != r.Config.Shape.MemoryMiB<<20 || mem.Shared ||
		mem.Hugepages ||
		mem.Mergeable ||
		mem.HotplugSize != 0 ||
		mem.File != "" ||
		(len(mem.Zones) > 0 && string(mem.Zones) != "null" && string(mem.Zones) != "[]") {
		return api.Fail(
			api.Incompatible,
			"COW requires static private RAM without zones",
		)
	}
	// Only snapshots of this exact engine layout are accepted. Configuration
	// checks supplement the published manifest's checksums and compatibility ID.
	var disks []cloudDisk
	if err := json.Unmarshal(cfg["disks"], &disks); err != nil {
		return err
	}
	_, bindings, err := runtimeImage(r)
	if err != nil {
		return err
	}
	if err = validateSnapshotDisks(disks, cloudDisks(bindings)); err != nil {
		return err
	}
	st, err := os.Stat(filepath.Join(r.RestoreDir, "memory-ranges"))
	if err != nil {
		return err
	}
	if st.Size() < mem.Size || st.Size()%int64(os.Getpagesize()) != 0 {
		return api.Fail(
			api.Incompatible,
			"unaligned or truncated COW memory file",
		)
	}
	return validateCOWTopology(r.RestoreDir, uint64(mem.Size))
}

func (b *CloudHypervisor) Pause(ctx context.Context, r Resource) error {
	_, err := b.api(ctx, r, "PUT", "vm.pause", nil, nil)
	return err
}

func (b *CloudHypervisor) Resume(ctx context.Context, r Resource) error {
	_, err := b.api(ctx, r, "PUT", "vm.resume", nil, nil)
	return err
}

// prepareCaptureWriter delegates only the pinned CH leaf, before source pause.
func (b *CloudHypervisor) prepareCaptureWriter(r Resource, dst string) error {
	return delegateCapture(r, dst, int(b.opts.UID), int(b.opts.GID))
}

func (b *CloudHypervisor) Capture(
	ctx context.Context,
	r Resource,
	dst string,
) error {
	rel, err := filepath.Rel(filepath.Join(r.Dir, "capture"), dst)
	parts := strings.Split(rel, string(filepath.Separator))
	if err != nil || len(parts) != 2 || !gcID(parts[0], "op-") ||
		parts[1] != "ch" {
		return api.Fail(api.Invalid, "capture outside prepared CH leaf")
	}
	leaf, err := openCaptureDir(dst)
	if err != nil {
		return err
	}
	st, err := leaf.Stat()
	_ = leaf.Close()
	if err != nil {
		return err
	}
	owner := st.Sys().(*syscall.Stat_t)
	if owner.Uid != b.opts.UID || owner.Gid != b.opts.GID ||
		st.Mode().Perm() != 0o700 {
		return api.Fail(
			api.Incompatible,
			"capture writer leaf was not prepared",
		)
	}
	_, err = b.api(
		ctx,
		r,
		"PUT",
		"vm.snapshot",
		map[string]string{
			"destination_url": "file:///capture/" + filepath.ToSlash(rel),
		},
		nil,
	)
	return err
}

func (b *CloudHypervisor) api(
	ctx context.Context,
	r Resource,
	method, endpoint string,
	input any,
	fds []int,
) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(
		ctx,
		"unix",
		filepath.Join(r.RunDir, "io", "api.sock"),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	var body []byte
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil {
			return nil, err
		}
	}
	header := fmt.Sprintf(
		"%s /api/v1/%s HTTP/1.1\r\nHost: localhost\r\nContent-Length: %d\r\n\r\n",
		method,
		endpoint,
		len(body),
	)
	if len(fds) > 0 {
		n, _, x := conn.(*net.UnixConn).WriteMsgUnix(
			[]byte(header),
			unix.UnixRights(fds...),
			nil,
		)
		if x != nil {
			return nil, x
		}
		if n < len(header) {
			if _, err = io.WriteString(conn, header[n:]); err != nil {
				return nil, err
			}
		}
	} else if _, err = io.WriteString(conn, header); err != nil {
		return nil, err
	}
	if len(body) > 0 {
		if _, err = conn.Write(body); err != nil {
			return nil, err
		}
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, &chHTTPError{
			Status:   resp.StatusCode,
			Endpoint: endpoint,
			Body:     string(out),
		}
	}
	return out, nil
}

func (b *CloudHypervisor) Agent(
	ctx context.Context,
	r Resource,
	restore bool,
) error {
	return systemAgent(
		ctx,
		filepath.Join(r.RunDir, "io", "vsock"),
		r.Instance,
		restore,
	)
}
