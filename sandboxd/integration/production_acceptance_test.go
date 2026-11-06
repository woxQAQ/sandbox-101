//go:build linux && privileged

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	engine "github.com/woxQAQ/sandbox-engine"
	"github.com/woxQAQ/sandbox-engine/api"
	engineclient "github.com/woxQAQ/sandbox-engine/client"
	pb "github.com/woxQAQ/sandbox-protocol/gen"
	"github.com/woxQAQ/sandboxd"
	"github.com/woxQAQ/sandboxd/egress"
	"github.com/woxQAQ/sandboxd/sdk"
)

// Run through scripts/run-production-acceptance.sh. Everything, including the
// real daemons, runs in fresh subordinate-ID, mount, PID and network namespaces.
// Missing production prerequisites fail the test; no backend/agent is mocked.
func TestProductionMicroVMAcceptance(t *testing.T) {
	for _, ns := range []string{"user", "net", "mnt"} {
		parent := productionRequired(
			t,
			"SANDBOX_PRODUCTION_PARENT_"+strings.ToUpper(ns),
		)
		current, err := os.Readlink("/proc/self/ns/" + ns)
		if err != nil || current == parent {
			t.Fatalf(
				"refusing production allocations without fresh %s namespace: parent=%s current=%s: %v",
				ns,
				parent,
				current,
				err,
			)
		}
	}
	if os.Geteuid() != 0 {
		t.Fatal(
			"namespace root is required for the unchanged production launcher",
		)
	}
	artifacts := productionRequired(t, "SANDBOX_PRODUCTION_ARTIFACTS")
	materials := productionRequired(t, "SANDBOX_PRODUCTION_MATERIALS")
	layout := productionRequired(t, "SANDBOX_PRODUCTION_OCI_LAYOUT")
	cgroup := productionRequired(t, "SANDBOX_PRODUCTION_CGROUP_ROOT")
	ioMax := productionRequired(t, "SANDBOX_PRODUCTION_IO_MAX")
	memoryMiB := microVMMemoryMiB(t)
	productionArtifact(
		t,
		artifacts,
		"guest-shape.txt",
		[]byte(
			fmt.Sprintf(
				"vcpus=1 memory_mib=%d disk_mib=512\nunchanged production memory.max=(memory_mib+128)MiB\nisolated runner defaults to the original 512MiB lifecycle shape; bounded tmpfs storage is served through bindfs to separate snapshot storage charges from VMM RAM\n",
				memoryMiB,
			),
		),
	)
	t.Logf(
		"explicit production guest shape: memory=%dMiB; unchanged VMM memory.max=%dMiB",
		memoryMiB,
		memoryMiB+128,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 17*time.Minute)
	defer cancel()
	// Keep the runtime socket paths below sockaddr_un's 108-byte limit, including
	// the sandbox proxy suffix. The compiled daemons are executable by the dropped
	// identity, just as their installed /usr/local/bin counterparts would be.
	for _, dir := range []string{"/run/p", "/run/p/bin", "/run/p/e", "/run/p/backing"} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"engined", "sandboxd"} {
		productionCopy(
			t,
			filepath.Join(artifacts, name),
			"/run/p/bin/"+name,
			0o755,
		)
	}
	if err := unix.Mount(
		"tmpfs",
		"/run/p/backing",
		"tmpfs",
		unix.MS_NOSUID|unix.MS_NODEV,
		"mode=0700,size=24g,nr_inodes=300000",
	); err != nil {
		t.Fatalf("bounded private engine storage: %v", err)
	}
	// FUSE provides reclaimable file cache while its owned storage process writes
	// to the bounded tmpfs. Snapshot storage must not consume the VMM's RAM quota.
	filesystem := productionStart(
		t,
		artifacts,
		"bindfs",
		"bindfs",
		"-f",
		"-o",
		"nosuid,nodev",
		"/run/p/backing",
		"/run/p/e",
	)
	t.Cleanup(func() { filesystem.stop(t) })
	productionWait(t, ctx, filesystem, func(context.Context) error {
		var stat unix.Statfs_t
		if err := unix.Statfs("/run/p/e", &stat); err != nil {
			return err
		}
		if stat.Type != unix.FUSE_SUPER_MAGIC {
			return fmt.Errorf("private FUSE storage is not mounted")
		}
		return nil
	})
	t.Cleanup(func() {
		// Preserve the exact state and launcher console even when admission or
		// identity verification failed. All data belongs to this acceptance run.
		entries, err := os.ReadDir(cgroup)
		if err != nil {
			t.Errorf("list owned VMM scopes for cleanup: %v", err)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				if err := os.WriteFile(
					filepath.Join(cgroup, entry.Name(), "cgroup.kill"),
					[]byte("1"),
					0o600,
				); err != nil {
					t.Errorf(
						"kill only owned VMM scope %s: %v",
						entry.Name(),
						err,
					)
				}
			}
		}
		archive := exec.Command(
			"tar",
			"--sparse",
			"--ignore-failed-read",
			"--warning=no-file-ignored",
			"-czf",
			filepath.Join(artifacts, "production-state.tar.gz"),
			"-C",
			"/run/p",
			"e",
			"sandboxd",
		)
		if out, err := archive.CombinedOutput(); err != nil {
			t.Errorf("archive owned engine state: %v: %s", err, out)
		}
	})
	for _, name := range []string{"uid_map", "gid_map", "setgroups", "cgroup", "mountinfo", "status"} {
		data, err := os.ReadFile("/proc/self/" + name)
		if err != nil {
			t.Fatal(err)
		}
		productionArtifact(t, artifacts, "harness-"+name+".txt", data)
	}
	deviceCheck := exec.CommandContext(
		ctx,
		"/run/p/bin/engined",
		"--internal-device-check",
	)
	deviceCheck.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:    65534,
			Gid:    65534,
			Groups: []uint32{},
		},
	}
	deviceOutput, deviceErr := deviceCheck.CombinedOutput()
	productionArtifact(
		t,
		artifacts,
		"device-check.txt",
		[]byte(
			fmt.Sprintf(
				"production --internal-device-check after UID/GID65534 and empty supplementary groups: %v\n%s",
				deviceErr,
				deviceOutput,
			),
		),
	)
	if deviceErr != nil {
		t.Fatalf(
			"production dropped-identity KVM/TUN check: %v: %s",
			deviceErr,
			deviceOutput,
		)
	}
	t.Log(
		"production device check passed after UID/GID65534 and empty supplementary groups",
	)
	productionArtifact(
		t,
		artifacts,
		"host-policy.json",
		[]byte(
			"{\"version\":1,\"storage_quota\":true,\"network_quotas\":true}\n",
		),
	)
	// The private tmpfs has real byte/inode ceilings. The outer fresh netns has
	// zero external interfaces, bounded nft admission sets and forwarding DROP.
	productionArtifact(
		t,
		artifacts,
		"quota-provisioning.txt",
		[]byte(
			"engine storage: bindfs over private tmpfs size=24g,nr_inodes=300000; unchanged permissions and real backing quotas\ntransient service: MemoryMax=8G,MemorySwapMax=0,TasksMax=2048\nnetwork: fresh disconnected netns, forwarding policy DROP, admission sets size=1024\nio.max="+ioMax+" (backing is tmpfs; this suite does not prove block-I/O enforcement)\n",
		),
	)
	backend := engine.NewCloudHypervisor(
		engine.CloudOptions{
			Binary:       filepath.Join(materials, "cloud-hypervisor"),
			BuildReceipt: filepath.Join(materials, "cloud-hypervisor.json"),
			CgroupRoot:   cgroup,
			HostPolicy:   filepath.Join(artifacts, "host-policy.json"),
			UID:          65534,
			GID:          65534,
			IOMax:        ioMax,
		},
	)
	defer backend.Close()
	var index struct {
		Manifests []struct {
			Digest string `json:"digest"`
		} `json:"manifests"`
	}
	raw, err := os.ReadFile(filepath.Join(layout, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(
		raw,
		&index,
	); err != nil ||
		len(index.Manifests) != 1 {
		t.Fatalf("exactly one OCI manifest required: %v", err)
	}
	image := index.Manifests[0].Digest
	store, err := engine.Open(
		engine.Options{
			StateDir:     "/run/p/e",
			Init:         true,
			Backend:      backend,
			MaxMemoryMiB: 4096,
			MaxVCPUs:     8,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, importErr := store.ImportOCI(
		ctx,
		engine.ImportOptions{
			Layout:   layout,
			Digest:   image,
			MaxBytes: 512 << 20,
			MaxFiles: 100000,
		},
	)
	closeErr := store.Close()
	if importErr != nil || closeErr != nil {
		t.Fatalf("production OCI import: %v; close: %v", importErr, closeErr)
	}
	t.Logf("real production image imported: %s", image)

	engineArgs := []string{
		"--state-dir=/run/p/e",
		"--socket=/run/p/engine.sock",
		"--cloud-hypervisor=" + filepath.Join(materials, "cloud-hypervisor"),
		"--build-receipt=" + filepath.Join(materials, "cloud-hypervisor.json"),
		"--cgroup-root=" + cgroup,
		"--host-policy=" + filepath.Join(artifacts, "host-policy.json"),
		"--io-max=" + ioMax,
		"--vmm-uid=65534",
		"--vmm-gid=65534",
		"--max-memory-mib=4096",
		"--max-vcpus=8",
	}
	productionRetainConsoles(t, artifacts, cgroup)
	engined := productionStart(
		t,
		artifacts,
		"engined-1",
		"/run/p/bin/engined",
		engineArgs...)
	t.Cleanup(func() { engined.stop(t) })
	ec := engineclient.New("/run/p/engine.sock")
	defer ec.Close()
	productionWait(
		t,
		ctx,
		engined,
		func(attempt context.Context) error { _, err := ec.ListVMs(attempt); return err },
	)
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		t.Fatal(err)
	}
	token := hex.EncodeToString(secret[:])
	tokenPath := "/run/p/root-token"
	if err := os.WriteFile(tokenPath, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	endpoint := "http://" + address
	sandboxdArgs := []string{
		"--state-dir=/run/p/sandboxd",
		"--engine-socket=/run/p/engine.sock",
		"--listen=" + address,
		"--root-token-file=" + tokenPath,
	}
	sandboxdProcess := productionStart(
		t,
		artifacts,
		"sandboxd-1",
		"/run/p/bin/sandboxd",
		append(sandboxdArgs, "--init")...)
	t.Cleanup(func() { sandboxdProcess.stop(t) })
	client := sdk.New(endpoint, token)
	productionWait(
		t,
		ctx,
		sandboxdProcess,
		func(attempt context.Context) error { _, err := client.List(attempt); return err },
	)
	t.Setenv("SANDBOX_E2E_ENDPOINT", endpoint)
	t.Setenv("SANDBOX_E2E_TOKEN_FILE", tokenPath)
	t.Setenv("SANDBOX_E2E_IMAGE_DIGEST", image)
	t.Setenv("SANDBOX_E2E_ENGINE_SOCKET", "/run/p/engine.sock")

	t.Run("live-daemon-adoption", func(t *testing.T) {
		_, err := client.PutTemplate(
			ctx,
			"adoption-template",
			sandboxd.Template{
				ID:    "adoption",
				Image: image,
				Shape: api.Shape{
					VCPUs:     1,
					MemoryMiB: microVMMemoryMiB(t),
					DiskMiB:   512,
				},
			},
			false,
		)
		if err != nil {
			t.Fatal(err)
		}
		claim, err := client.Claim(
			ctx,
			"adoption-claim",
			sandboxd.Command{
				Action:     "claim",
				TemplateID: "adoption",
				TTLSeconds: 900,
				Policy:     egress.Policy{Mode: "none"},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			cleanup, stop := context.WithTimeout(
				context.Background(),
				time.Minute,
			)
			defer stop()
			if _, err := client.Change(
				cleanup,
				claim.Sandbox.ID,
				"adoption-release",
				sandboxd.Command{Action: "release"},
			); err != nil {
				t.Errorf("release adopted VM: %v", err)
			}
		}()
		before, err := ec.GetVM(ctx, claim.Sandbox.VMID)
		if err != nil || !before.Verified || !before.AgentReady ||
			before.PID <= 0 {
			t.Fatalf("production VMM not verified/ready: %+v: %v", before, err)
		}
		productionVMMProof(t, artifacts, "before", before)
		identity := productionExec(
			t,
			ctx,
			client,
			claim.Sandbox.ID,
			"cat /etc/machine-id",
		)
		conn, err := client.Connect(ctx, claim.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		session, err := conn.Call(ctx, "session_create", map[string]any{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		sessionID := session["session_id"].(string)
		if _, err := conn.Call(
			ctx,
			"exec",
			map[string]any{
				"session_id": sessionID,
				"command":    "ADOPTION_STATE=alive",
			},
			nil,
		); err != nil {
			t.Fatal(err)
		}
		conn.Close()
		sandboxdProcess.stop(t)
		engined.stop(t)
		if err := syscall.Kill(before.PID, 0); err != nil {
			t.Fatalf("VMM died with daemons: %v", err)
		}
		engined = productionStart(
			t,
			artifacts,
			"engined-2",
			"/run/p/bin/engined",
			engineArgs...)
		productionWait(
			t,
			ctx,
			engined,
			func(attempt context.Context) error { _, err := ec.ListVMs(attempt); return err },
		)
		after, err := ec.GetVM(ctx, before.ID)
		if err != nil || !after.Verified || !after.AgentReady ||
			after.PID != before.PID ||
			after.StartTime != before.StartTime ||
			after.Instance != before.Instance {
			t.Fatalf(
				"daemon restart did not adopt the exact live VMM: before=%+v after=%+v: %v",
				before,
				after,
				err,
			)
		}
		productionVMMProof(t, artifacts, "after", after)
		sandboxdProcess = productionStart(
			t,
			artifacts,
			"sandboxd-2",
			"/run/p/bin/sandboxd",
			sandboxdArgs...)
		productionWait(
			t,
			ctx,
			sandboxdProcess,
			func(attempt context.Context) error { _, err := client.List(attempt); return err },
		)
		if actual := productionExec(
			t,
			ctx,
			client,
			claim.Sandbox.ID,
			"cat /etc/machine-id",
		); actual != identity {
			t.Fatalf(
				"daemon restart changed guest identity: before=%q after=%q",
				identity,
				actual,
			)
		}
		conn, err = client.Connect(ctx, claim.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		var stdout bytes.Buffer
		_, err = conn.Call(
			ctx,
			"exec",
			map[string]any{
				"session_id": sessionID,
				"command":    "printf '%s' \"$ADOPTION_STATE\"",
			},
			func(frame *pb.Frame) error { _, err := stdout.Write(frame.Data); return err },
		)
		if err != nil || stdout.String() != "alive" {
			t.Fatalf(
				"daemon adoption lost the live guest shell: %q: %v",
				stdout.String(),
				err,
			)
		}
		t.Logf(
			"SIGKILLed both daemons; adopted PID=%d start=%d instance=%s with unchanged machine-id and live shell state",
			after.PID,
			after.StartTime,
			after.Instance,
		)
	})
	if t.Failed() {
		t.Fatal(
			"production launch/adoption failed; full lifecycle remains unproven",
		)
	}
	if t.Run("TestMicroVMLifecycle", TestMicroVMLifecycle) {
		if t.Run("golden-and-warm-provisioning", func(t *testing.T) {
			productionProvisioningTiers(t, ctx, client, ec, artifacts, image)
		}) {
			t.Run("real-guest-tcp", func(t *testing.T) {
				productionGuestTCP(t, ctx, client, ec, artifacts, image)
			})
		}
	}
}

func productionProvisioningTiers(
	t *testing.T,
	ctx context.Context,
	client *sdk.Client,
	ec *engineclient.Client,
	artifacts, image string,
) {
	t.Helper()
	template := sandboxd.Template{
		ID:    "production-tiers",
		Image: image,
		Shape: api.Shape{
			VCPUs:     1,
			MemoryMiB: microVMMemoryMiB(t),
			DiskMiB:   512,
		},
		Warmup: "printf prepared > /workspace/production-warmup; sync",
	}
	built, err := client.PutTemplate(
		ctx,
		"production-tier-build",
		template,
		true,
	)
	if err != nil || built.Template == nil || built.Template.SnapshotID == "" ||
		built.Template.State != "ready" {
		t.Fatalf(
			"real template_build/prepare_template failed: %+v: %v",
			built.Template,
			err,
		)
	}
	template = *built.Template
	var owned []string
	defer func() {
		cleanup, cancel := context.WithTimeout(
			context.Background(),
			time.Minute,
		)
		defer cancel()
		template.TargetWarm = 0
		if _, err := client.PutTemplate(
			cleanup,
			"production-tier-disable-refill",
			template,
			false,
		); err != nil {
			t.Errorf("disable owned warm pool: %v", err)
		}
		for _, id := range owned {
			if _, err := client.Change(
				cleanup,
				id,
				"production-tier-cleanup-"+id,
				sandboxd.Command{Action: "release"},
			); err != nil {
				t.Errorf("release provisioning tier sandbox: %v", err)
			}
		}
	}()
	golden, err := client.Claim(
		ctx,
		"production-tier-golden",
		sandboxd.Command{
			Action:     "claim",
			TemplateID: template.ID,
			TTLSeconds: 600,
			Policy:     egress.Policy{Mode: "none"},
			Env: map[string]sandboxd.EnvVar{
				"PROVISIONING_TIER": {Value: "golden"},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	owned = append(owned, golden.Sandbox.ID)
	if golden.Sandbox.ProvisionSource != "golden" {
		t.Fatalf(
			"expected real golden clone claim, got %q",
			golden.Sandbox.ProvisionSource,
		)
	}
	if got := productionExec(
		t,
		ctx,
		client,
		golden.Sandbox.ID,
		"cat /workspace/production-warmup; printf ':%s' \"$PROVISIONING_TIER\"",
	); got != "prepared:golden" {
		t.Fatalf("golden clone lost warmup/activation: %q", got)
	}
	goldenIdentity := productionExec(
		t,
		ctx,
		client,
		golden.Sandbox.ID,
		"cat /etc/machine-id",
	)
	if _, err := client.Change(
		ctx,
		golden.Sandbox.ID,
		"production-tier-release-golden",
		sandboxd.Command{Action: "release"},
	); err != nil {
		t.Fatal(err)
	}
	owned = nil
	template.TargetWarm = 1
	if _, err := client.PutTemplate(
		ctx,
		"production-tier-enable-refill",
		template,
		false,
	); err != nil {
		t.Fatal(err)
	}
	// Observe the actual committed ready-pool row read-only; do not infer warm
	// readiness from a sleep or from an unclaimed but still loading VMM.
	db, err := sql.Open(
		"sqlite",
		"file:/run/p/sandboxd/sandboxd.db?mode=ro&_pragma=busy_timeout(1000)",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	poolCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var warmID string
	for {
		err := db.QueryRowContext(poolCtx, "SELECT vm_id FROM warm WHERE template_id=?", template.ID).
			Scan(&warmID)
		if err == nil {
			break
		}
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		select {
		case <-poolCtx.Done():
			t.Fatalf(
				"target_warm=1 never published a ready pool VM: %v",
				poolCtx.Err(),
			)
		case <-time.After(100 * time.Millisecond):
		}
	}
	before, err := ec.GetVM(ctx, warmID)
	if err != nil || !before.Verified || !before.AgentReady ||
		before.NetworkActual ||
		before.NetworkEnabled {
		t.Fatalf(
			"ready pool VMM is not verified/ready/blocked: %+v: %v",
			before,
			err,
		)
	}
	warm, err := client.Claim(
		ctx,
		"production-tier-warm",
		sandboxd.Command{
			Action:     "claim",
			TemplateID: template.ID,
			TTLSeconds: 600,
			Policy:     egress.Policy{Mode: "none"},
			Env: map[string]sandboxd.EnvVar{
				"PROVISIONING_TIER": {Value: "warm"},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	owned = append(owned, warm.Sandbox.ID)
	after, err := ec.GetVM(ctx, warm.Sandbox.VMID)
	if err != nil || warm.Sandbox.ProvisionSource != "warm" ||
		warm.Sandbox.VMID != warmID ||
		after.PID != before.PID ||
		after.StartTime != before.StartTime ||
		after.Instance != before.Instance {
		t.Fatalf(
			"ready-pool claim did not preserve the live VMM: source=%q before=%+v after=%+v: %v",
			warm.Sandbox.ProvisionSource,
			before,
			after,
			err,
		)
	}
	if got := productionExec(
		t,
		ctx,
		client,
		warm.Sandbox.ID,
		"cat /workspace/production-warmup; printf ':%s' \"$PROVISIONING_TIER\"",
	); got != "prepared:warm" {
		t.Fatalf(
			"warm claim lost template data or activation environment: %q",
			got,
		)
	}
	warmIdentity := productionExec(
		t,
		ctx,
		client,
		warm.Sandbox.ID,
		"cat /etc/machine-id",
	)
	if len(strings.TrimSpace(warmIdentity)) != 32 ||
		warmIdentity == goldenIdentity {
		t.Fatal(
			"golden and warm claims did not receive independent guest identities",
		)
	}
	productionVMMProof(t, artifacts, "warm", after)
	proof, err := json.MarshalIndent(
		map[string]any{
			"snapshot_id":     template.SnapshotID,
			"golden_source":   golden.Sandbox.ProvisionSource,
			"warm_source":     warm.Sandbox.ProvisionSource,
			"warm_vm_id":      warmID,
			"warm_pid":        after.PID,
			"warm_start_time": after.StartTime,
			"warm_instance":   after.Instance,
		},
		"",
		"  ",
	)
	if err != nil {
		t.Fatal(err)
	}
	productionArtifact(t, artifacts, "provisioning-tiers.json", proof)
	t.Logf(
		"real template_build warmup+prepare_template, golden clone, and target_warm=1 ready-pool claim passed; warm PID %d was reused",
		after.PID,
	)
}

// sandboxd compensation can remove an exited VM's directory before a failing
// test returns. Hold only diagnostic log FDs so its exact console survives that
// cleanup; disks, memory backings and lifecycle operations are unaffected.
func productionRetainConsoles(t *testing.T, artifacts, cgroup string) {
	t.Helper()
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		logs := map[string]*os.File{}
		var scopes []<-chan struct{}
		watched := map[string]bool{}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				for _, scope := range scopes {
					<-scope
				}
				for path, file := range logs {
					data, err := io.ReadAll(file)
					file.Close()
					if err == nil {
						name := filepath.Base(
							filepath.Dir(filepath.Dir(filepath.Dir(path))),
						) + "-" + filepath.Base(
							filepath.Dir(path),
						)
						_ = os.WriteFile(
							filepath.Join(artifacts, name+"-console.log"),
							data,
							0o600,
						)
					}
				}
				return
			case <-ticker.C:
				entries, _ := os.ReadDir(cgroup)
				for _, entry := range entries {
					if entry.IsDir() && !watched[entry.Name()] {
						watched[entry.Name()] = true
						scopes = append(
							scopes,
							productionRetainScopeMemory(
								artifacts,
								filepath.Join(cgroup, entry.Name()),
								stop,
							),
						)
					}
				}
				// Retain through the backing mount: a FUSE open would create a
				// .fuse_hidden entry on unlink and interfere with real VM removal.
				paths, _ := filepath.Glob(
					"/run/p/backing/vms/*/runs/*/console.log",
				)
				for _, path := range paths {
					if logs[path] != nil {
						continue
					}
					if file, err := os.Open(path); err == nil {
						logs[path] = file
					}
				}
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
}

// memory.events supports POLLPRI notifications. Retain the last readable counters
// before sandboxd compensation removes the owned scope, including an OOM kill.
func productionRetainScopeMemory(
	artifacts, scope string,
	stop <-chan struct{},
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		events, err := os.Open(filepath.Join(scope, "memory.events"))
		if err != nil {
			return
		}
		defer events.Close()
		proof := map[string]string{}
		defer func() {
			data, err := json.MarshalIndent(proof, "", "  ")
			if err == nil {
				_ = os.WriteFile(
					filepath.Join(
						artifacts,
						filepath.Base(scope)+"-memory.json",
					),
					data,
					0o600,
				)
			}
		}()
		for {
			if _, err := events.Seek(0, io.SeekStart); err != nil {
				return
			}
			data, err := io.ReadAll(events)
			if err != nil {
				return
			}
			proof["memory.events"] = string(data)
			for _, name := range []string{"memory.max", "memory.current", "memory.peak", "memory.stat"} {
				if data, err := os.ReadFile(
					filepath.Join(scope, name),
				); err == nil {
					proof[name] = string(data)
				}
			}
			select {
			case <-stop:
				return
			default:
			}
			poll := []unix.PollFd{
				{Fd: int32(events.Fd()), Events: unix.POLLPRI | unix.POLLERR},
			}
			if _, err := unix.Poll(poll, 100); err != nil && err != unix.EINTR {
				return
			}
		}
	}()
	return done
}

func productionRequired(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf(
			"%s is required; use scripts/run-production-acceptance.sh",
			name,
		)
	}
	return value
}

func productionArtifact(t *testing.T, directory, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(
		filepath.Join(directory, name),
		data,
		0o600,
	); err != nil {
		t.Fatal(err)
	}
}

func productionCopy(t *testing.T, source, dest string, mode os.FileMode) {
	t.Helper()
	in, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("copy %s: %v; close: %v", source, copyErr, closeErr)
	}
}

type productionProcess struct {
	cmd     *exec.Cmd
	done    chan error
	stopped bool
}

func productionStart(
	t *testing.T,
	artifacts, name, binary string,
	args ...string,
) *productionProcess {
	t.Helper()
	log, err := os.OpenFile(
		filepath.Join(artifacts, name+".log"),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, args...)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	process := &productionProcess{cmd: cmd, done: make(chan error, 1)}
	go func() { process.done <- cmd.Wait(); log.Close() }()
	return process
}

func (p *productionProcess) stop(t *testing.T) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	if err := p.cmd.Process.Kill(); err != nil && err != os.ErrProcessDone {
		t.Errorf("kill owned daemon: %v", err)
	}
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Errorf("owned daemon did not exit: PID %d", p.cmd.Process.Pid)
	}
}

func productionWait(
	t *testing.T,
	ctx context.Context,
	process *productionProcess,
	ready func(context.Context) error,
) {
	t.Helper()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	var last error
	for {
		attempt, cancel := context.WithTimeout(ctx, time.Second)
		last = ready(attempt)
		cancel()
		if last == nil {
			return
		}
		select {
		case err := <-process.done:
			process.stopped = true
			t.Fatalf(
				"real daemon exited before readiness: %v (last readiness error: %v)",
				err,
				last,
			)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-deadline.C:
			t.Fatalf("real daemon readiness: %v", last)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func productionExec(
	t *testing.T,
	ctx context.Context,
	client *sdk.Client,
	id, command string,
) string {
	t.Helper()
	conn, err := client.Connect(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var stdout, stderr bytes.Buffer
	code, err := conn.Exec(ctx, command, &stdout, &stderr)
	if err != nil || code != 0 {
		t.Fatalf(
			"real guest exec: %v code=%d stderr=%s",
			err,
			code,
			stderr.String(),
		)
	}
	return stdout.String()
}

func productionVMMProof(t *testing.T, artifacts, stage string, vm api.VM) {
	t.Helper()
	data, err := json.MarshalIndent(vm, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	productionArtifact(t, artifacts, "vmm-"+stage+".json", data)
	var status []byte
	for _, name := range []string{"status", "cgroup", "mountinfo", "stat", "maps"} {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/%s", vm.PID, name))
		if err != nil {
			t.Fatalf("inspect actual dropped VMM %s: %v", name, err)
		}
		productionArtifact(t, artifacts, "vmm-"+stage+"-"+name+".txt", data)
		if name == "status" {
			status = data
		}
	}
	for _, want := range []string{"Uid:\t65534\t65534\t65534\t65534", "Gid:\t65534\t65534\t65534\t65534", "CapInh:\t0000000000000000", "CapPrm:\t0000000000000000", "CapEff:\t0000000000000000", "CapBnd:\t0000000000000000", "CapAmb:\t0000000000000000", "NoNewPrivs:\t1", "Seccomp:\t2"} {
		if !bytes.Contains(status, []byte(want)) {
			t.Fatalf("VMM privilege isolation missing %q", want)
		}
	}
	groupsFound := false
	for _, line := range strings.Split(string(status), "\n") {
		if groups, ok := strings.CutPrefix(line, "Groups:"); ok {
			groupsFound = true
			if strings.TrimSpace(groups) != "" {
				t.Fatalf("VMM retained supplementary groups: %s", groups)
			}
		}
	}
	if !groupsFound {
		t.Fatal("VMM supplementary group status missing")
	}
	var namespaces strings.Builder
	for _, name := range []string{"mnt", "pid", "ipc", "uts", "net"} {
		parent, err := os.Readlink("/proc/self/ns/" + name)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/%s", vm.PID, name))
		if err != nil || actual == parent {
			t.Fatalf(
				"VMM %s namespace was not isolated: harness=%s VMM=%s: %v",
				name,
				parent,
				actual,
				err,
			)
		}
		fmt.Fprintf(&namespaces, "%s harness=%s VMM=%s\n", name, parent, actual)
	}
	productionArtifact(
		t,
		artifacts,
		"vmm-"+stage+"-namespaces.txt",
		[]byte(namespaces.String()),
	)
	root := fmt.Sprintf("/proc/%d/root", vm.PID)
	for _, forbidden := range []string{".oldroot", "home", "nix/store", "etc/shadow"} {
		if _, err := os.Lstat(
			filepath.Join(root, forbidden),
		); !os.IsNotExist(
			err,
		) {
			t.Fatalf("pivot_root leaked %s: %v", forbidden, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "dev/kvm")); err != nil {
		t.Fatalf("restricted VMM root lacks KVM bind mount: %v", err)
	}
	if stage == "warm" {
		info, err := os.Stat(filepath.Join(root, "restore/memory-ranges"))
		if err != nil {
			t.Fatalf("inspect actual restored COW backing: %v", err)
		}
		stat := info.Sys().(*syscall.Stat_t)
		productionArtifact(
			t,
			artifacts,
			"vmm-warm-backing-stat.txt",
			[]byte(
				fmt.Sprintf(
					"device=%x:%x inode=%d size=%d mode=%v\n",
					unix.Major(stat.Dev),
					unix.Minor(stat.Dev),
					stat.Ino,
					stat.Size,
					info.Mode(),
				),
			),
		)
	}
}
