//go:build linux && privileged

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"

	engine "github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/sdk"
	"github.com/woxQAQ/haven/types"
	pb "github.com/woxQAQ/sandbox-protocol/gen"
)

// Run through scripts/run-runtime.sh. Everything, including the
// real Haven daemon, runs in fresh subordinate-ID, mount, PID and network namespaces.
// Missing runtime prerequisites fail the test; no backend/agent is mocked.
func TestHavenRuntimeLifecycle(t *testing.T) {
	for _, ns := range []string{
		"user",
		"net",
		"mnt",
	} {
		parent := requireEnv(t, "SANDBOX_RUNTIME_PARENT_"+strings.ToUpper(ns))
		current, err := os.Readlink("/proc/self/ns/" + ns)
		if err != nil || current == parent {
			t.Fatalf(
				"refusing VM allocations without fresh %s namespace: parent=%s current=%s: %v",
				ns,
				parent,
				current,
				err,
			)
		}
	}
	if os.Geteuid() != 0 {
		t.Fatal("namespace root is required for the privileged launcher")
	}
	artifacts := requireEnv(t, "SANDBOX_RUNTIME_ARTIFACTS")
	materials := requireEnv(t, "SANDBOX_RUNTIME_MATERIALS")
	layout := requireEnv(t, "SANDBOX_RUNTIME_OCI_LAYOUT")
	cgroup := requireEnv(t, "SANDBOX_RUNTIME_CGROUP_ROOT")
	ioMax := requireEnv(t, "SANDBOX_RUNTIME_IO_MAX")
	memoryMiB := microVMMemoryMiB(t)
	writeArtifact(
		t,
		artifacts,
		"guest-shape.txt",
		[]byte(
			fmt.Sprintf(
				"vcpus=1 memory_mib=%d disk_mib=512\nunchanged VMM memory.max=(memory_mib+128)MiB\nisolated runner defaults to the original 512MiB lifecycle shape; bounded tmpfs storage is served through bindfs to separate snapshot storage charges from VMM RAM\n",
				memoryMiB,
			),
		),
	)
	t.Logf("explicit guest shape: memory=%dMiB; unchanged VMM memory.max=%dMiB", memoryMiB, memoryMiB+128)
	ctx, cancel := context.WithTimeout(context.Background(), 17*time.Minute)
	defer cancel()
	// Keep the runtime socket paths below sockaddr_un's 108-byte limit, including
	// the sandbox proxy suffix. The compiled daemon is executable by the dropped
	// identity, just as their installed /usr/local/bin counterparts would be.
	for _, dir := range []string{
		"/run/p",
		"/run/p/bin",
		"/run/p/e",
		"/run/p/backing",
	} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{
		"haven",
	} {
		copyFixtureFile(t, filepath.Join(artifacts, name), "/run/p/bin/"+name, 0o755)
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
	filesystem := startProcess(t, artifacts, processCommand{
		Name:   "bindfs",
		Binary: "bindfs",
		Args: []string{
			"-f",
			"-o",
			"nosuid,nodev",
			"/run/p/backing",
			"/run/p/e",
		},
	})
	t.Cleanup(func() {
		filesystem.stop(t)
	})
	awaitProcess(t, ctx, filesystem, func(context.Context) error {
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
		// identity verification failed. All data belongs to this isolated run.
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
					t.Errorf("kill only owned VMM scope %s: %v", entry.Name(), err)
				}
			}
		}
		archive := exec.Command(
			"tar",
			"--sparse",
			"--ignore-failed-read",
			"--warning=no-file-ignored",
			"-czf",
			filepath.Join(artifacts, "runtime-state.tar.gz"),
			"-C",
			"/run/p",
			"e",
		)
		if out, err := archive.CombinedOutput(); err != nil {
			t.Errorf("archive owned engine state: %v: %s", err, out)
		}
	})
	for _, name := range []string{
		"uid_map",
		"gid_map",
		"setgroups",
		"cgroup",
		"mountinfo",
		"status",
	} {
		data, err := os.ReadFile("/proc/self/" + name)
		if err != nil {
			t.Fatal(err)
		}
		writeArtifact(t, artifacts, "harness-"+name+".txt", data)
	}
	deviceCheck := exec.CommandContext(ctx, "/run/p/bin/haven", "--internal-device-check")
	deviceCheck.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:    65534,
			Gid:    65534,
			Groups: []uint32{},
		},
	}
	deviceOutput, deviceErr := deviceCheck.CombinedOutput()
	writeArtifact(
		t,
		artifacts,
		"device-check.txt",
		[]byte(
			fmt.Sprintf(
				"Haven --internal-device-check after UID/GID65534 and empty supplementary groups: %v\n%s",
				deviceErr,
				deviceOutput,
			),
		),
	)
	if deviceErr != nil {
		t.Fatalf("dropped-identity KVM/TUN check: %v: %s", deviceErr, deviceOutput)
	}
	t.Log("device check passed after UID/GID65534 and empty supplementary groups")
	writeArtifact(
		t,
		artifacts,
		"host-policy.json",
		[]byte("{\"version\":1,\"storage_quota\":true,\"network_quotas\":true}\n"),
	)
	// The private tmpfs has real byte/inode ceilings. The outer fresh netns has
	// zero external interfaces, bounded nft admission sets and forwarding DROP.
	writeArtifact(
		t,
		artifacts,
		"quota-provisioning.txt",
		[]byte(
			"engine storage: bindfs over private tmpfs size=24g,nr_inodes=300000; unchanged permissions and real backing quotas\ntransient service: MemoryMax=8G,MemorySwapMax=0,TasksMax=2048\nnetwork: fresh disconnected netns, forwarding policy DROP, admission sets size=1024\nio.max="+ioMax+" (backing is tmpfs; this suite does not prove block-I/O enforcement)\n",
		),
	)
	backend := engine.NewCloudHypervisor(
		engine.CloudHypervisorOptions{
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
	if err := json.Unmarshal(raw, &index); err != nil || len(index.Manifests) != 1 {
		t.Fatalf("exactly one OCI manifest required: %v", err)
	}
	image := index.Manifests[0].Digest
	store, err := startHaven(t, ctx, havenLaunch{
		Artifacts: artifacts,
		Materials: materials,
		Cgroup:    cgroup,
		IOMax:     ioMax,
		Layout:    layout,
		Image:     image,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		store.process.stop(t)
	}()
	ec := newVMInventory(t, "/run/p/e", backend)
	defer ec.close()
	client := store.client
	scenario := runtimeScenario{
		Client:    client,
		Inventory: ec,
		Artifacts: artifacts,
		Image:     image,
	}
	tokenPath := "/run/p/root-token"
	endpoint := client.Endpoint
	// D is each distinct writable disk; S is each distinct SID's D+M charge.
	// This finite capacity is compared with active ledger responsibility. The
	// conservative normal trace retains 6D+4S after life, reaches 7D+5S during
	// build and 8D+5S with claimed W plus one occupied successor Q. Build D/slot
	// retire before publication; G retires before warm enable, and W retires
	// before TCP. Even delayed Q cleanup then fits 11D+7S, below 12D+7S.
	// Permanent template T remains billed. Unknown Q responsibility occupies
	// target1 until retirement; arbitrary failure history can exhaust budget.
	const diskMiB int64 = 512
	snapshotMiB := diskMiB + memoryMiB
	maxSandboxes := 1 + (1 + 2) + (1 + 1) + (1 + 1 + 1)
	maxStorageMiB := diskMiB + // adoption
		(5*diskMiB + 4*snapshotMiB) + // lifecycle
		(2*diskMiB + snapshotMiB) + // golden and claimed warm
		(4*diskMiB + 2*snapshotMiB) // TCP resource sequence
	// Root only bypasses manual admission comparisons; refill uses owner quota.
	store.putOwnerBudget(t, ctx, maxSandboxes, maxStorageMiB)
	client.Tenant = "root"
	t.Setenv("SANDBOX_E2E_TENANT", "root")
	t.Setenv("SANDBOX_E2E_ENDPOINT", endpoint)
	t.Setenv("SANDBOX_E2E_TOKEN_FILE", tokenPath)
	t.Setenv("SANDBOX_E2E_IMAGE_DIGEST", image)
	t.Setenv("SANDBOX_E2E_STATE_DIR", "/run/p/e")
	t.Setenv("SANDBOX_E2E_HYPERVISOR", filepath.Join(materials, "cloud-hypervisor"))
	t.Setenv("SANDBOX_E2E_BUILD_RECEIPT", filepath.Join(materials, "cloud-hypervisor.json"))
	t.Setenv("SANDBOX_E2E_CGROUP_ROOT", cgroup)
	t.Setenv("SANDBOX_E2E_IO_MAX", ioMax)
	t.Setenv("SANDBOX_E2E_VMM_UID", "65534")
	t.Setenv("SANDBOX_E2E_VMM_GID", "65534")

	t.Run("live-daemon-adoption", func(t *testing.T) {
		_, err := client.PutTemplate(
			ctx,
			"adoption-template",
			types.Template{
				ID:    "adoption",
				Image: image,
				Shape: types.Shape{
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
			types.Command{
				Action:     "claim",
				TemplateID: "adoption",
				TTLSeconds: 900,
				Policy: types.Policy{
					Mode: "none",
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if _, err := client.Change(
				cleanup,
				claim.Sandbox.ID,
				"adoption-release",
				types.Command{
					Action: "release",
				},
			); err != nil {
				t.Errorf("release adopted VM: %v", err)
			}
		}()
		before, err := ec.getVM(ctx, claim.Sandbox.VMID)
		if err != nil || !before.Verified || !before.AgentReady || before.PID <= 0 {
			t.Fatalf("VMM not verified/ready: %+v: %v", before, err)
		}
		recordVMMProof(t, artifacts, "before", before)
		identity := scenario.guestExec(t, ctx, claim.Sandbox.ID, "cat /etc/machine-id")
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
		store.process.stop(t)
		if err := syscall.Kill(before.PID, 0); err != nil {
			t.Fatalf("VMM died with Haven: %v", err)
		}
		store.process = startProcess(t, artifacts, processCommand{
			Name:   "haven-2",
			Binary: "/run/p/bin/haven",
			Args:   store.args,
		})
		awaitProcess(t, ctx, store.process, func(attempt context.Context) error {
			_, err := client.List(attempt)
			return err
		})
		after, err := ec.getVM(ctx, before.ID)
		if err != nil || !after.Verified || !after.AgentReady || after.PID != before.PID ||
			after.StartTime != before.StartTime ||
			after.Instance != before.Instance {
			t.Fatalf("daemon restart did not adopt the exact live VMM: before=%+v after=%+v: %v", before, after, err)
		}
		recordVMMProof(t, artifacts, "after", after)
		if actual := scenario.guestExec(t, ctx, claim.Sandbox.ID, "cat /etc/machine-id"); actual != identity {
			t.Fatalf("daemon restart changed guest identity: before=%q after=%q", identity, actual)
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
			func(frame *pb.Frame) error {
				_, err := stdout.Write(frame.Data)
				return err
			},
		)
		if err != nil || stdout.String() != "alive" {
			t.Fatalf("daemon adoption lost the live guest shell: %q: %v", stdout.String(), err)
		}
		t.Logf(
			"SIGKILLed Haven; adopted PID=%d start=%d instance=%s with unchanged machine-id and live shell state",
			after.PID,
			after.StartTime,
			after.Instance,
		)
	})
	if t.Failed() {
		t.Fatal("Haven launch/adoption failed; full lifecycle remains unproven")
	}
	if t.Run("TestMicroVMLifecycle", TestMicroVMLifecycle) {
		if t.Run("golden-and-warm-provisioning", func(t *testing.T) {
			scenario.verifyProvisioningTiers(t, ctx)
		}) {
			t.Run("real-guest-tcp", func(t *testing.T) {
				scenario.probeGuestTCP(t, ctx)
			})
		}
	}
}

func (store *runningHaven) putOwnerBudget(t *testing.T, ctx context.Context, slots int, storageMiB int64) {
	client, rootBearer := store.client, store.token
	t.Helper()
	// Omit Token: the generated ordinary credential is intentionally ignored.
	payload, err := json.Marshal(struct {
		ID      string `json:"id"`
		Slots   int    `json:"max_sandboxes"`
		Storage int64  `json:"max_storage_mib"`
	}{"root", slots, storageMiB})
	if err != nil {
		t.Fatal("owner budget: encode")
	}
	attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(
		attempt,
		http.MethodPost,
		client.Endpoint+"/v1/tenants",
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatal("owner budget: request")
	}
	req.Header.Set("Authorization", "Bearer "+rootBearer)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.HTTP.Do(req)
	if err != nil {
		t.Fatal("owner budget: transport")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("owner budget: status=%d", resp.StatusCode)
	}
	var got struct {
		ID      string `json:"id"`
		Slots   int    `json:"max_sandboxes"`
		Storage int64  `json:"max_storage_mib"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&got) != nil {
		t.Fatal("owner budget: decode")
	}
	if got.ID != "root" || got.Slots != slots || got.Storage != storageMiB {
		t.Fatal("owner budget: response fields")
	}
}

// runtimeScenario keeps a single provisioned-host lifecycle target and its
// owned observations together. Each operation retains its own context budget.
type runtimeScenario struct {
	Client    *sdk.Client
	Inventory *vmInventory
	Artifacts string
	Image     string
}

func (scenario runtimeScenario) verifyProvisioningTiers(t *testing.T, ctx context.Context) {
	client, ec, artifacts, image := scenario.Client, scenario.Inventory, scenario.Artifacts, scenario.Image
	t.Helper()
	template := types.Template{
		ID:    "runtime-tiers",
		Image: image,
		Shape: types.Shape{
			VCPUs:     1,
			MemoryMiB: microVMMemoryMiB(t),
			DiskMiB:   512,
		},
		Warmup: "printf prepared > /workspace/template-warmup; sync",
	}
	built, err := client.PutTemplate(ctx, "tier-build", template, true)
	if err != nil || built.Template == nil || built.Template.SnapshotID == "" || built.Template.State != "ready" {
		t.Fatalf("real template_build/prepare_template failed: %+v: %v", built.Template, err)
	}
	template = *built.Template
	var owned []string
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		template.TargetWarm = 0
		if _, err := client.PutTemplate(cleanup, "tier-disable-refill", template, false); err != nil {
			t.Errorf("disable owned warm pool: %v", err)
		}
		for _, id := range owned {
			if _, err := client.Change(
				cleanup,
				id,
				"tier-cleanup-"+id,
				types.Command{
					Action: "release",
				},
			); err != nil {
				t.Errorf("release provisioning tier sandbox: %v", err)
			}
		}
	}()
	golden, err := client.Claim(
		ctx,
		"tier-golden",
		types.Command{
			Action:     "claim",
			TemplateID: template.ID,
			TTLSeconds: 600,
			Policy: types.Policy{
				Mode: "none",
			},
			Env: map[string]types.EnvVar{
				"PROVISIONING_TIER": {
					Value: "golden",
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	owned = append(owned, golden.Sandbox.ID)
	if golden.Sandbox.ProvisionSource != "golden" {
		t.Fatalf("expected real golden clone claim, got %q", golden.Sandbox.ProvisionSource)
	}
	if got := scenario.guestExec(
		t,
		ctx,
		golden.Sandbox.ID,
		"cat /workspace/template-warmup; printf ':%s' \"$PROVISIONING_TIER\"",
	); got != "prepared:golden" {
		t.Fatalf("golden clone lost warmup/activation: %q", got)
	}
	goldenIdentity := scenario.guestExec(t, ctx, golden.Sandbox.ID, "cat /etc/machine-id")
	if _, err := client.Change(
		ctx,
		golden.Sandbox.ID,
		"tier-release-golden",
		types.Command{
			Action: "release",
		},
	); err != nil {
		t.Fatal(err)
	}
	owned = nil
	template.TargetWarm = 1
	if _, err := client.PutTemplate(ctx, "tier-enable-refill", template, false); err != nil {
		t.Fatal(err)
	}
	// Observe the actual committed ready-pool row read-only; do not infer warm
	// readiness from a sleep or from an unclaimed but still loading VMM.
	db, err := sql.Open("sqlite", "file:/run/p/e/haven.db?mode=ro&_pragma=busy_timeout(1000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	poolCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var warmID string
	for {
		err := db.QueryRowContext(poolCtx, "SELECT vm_id FROM pool_slots WHERE template_id=? AND status='ready'", template.ID).Scan(&warmID)
		if err == nil {
			break
		}
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		select {
		case <-poolCtx.Done():
			t.Fatalf("target_warm=1 never published a ready pool VM: %v", poolCtx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	before, err := ec.getVM(ctx, warmID)
	if err != nil || !before.Verified || !before.AgentReady || before.NetworkActual || before.NetworkEnabled {
		t.Fatalf("ready pool VMM is not verified/ready/blocked: %+v: %v", before, err)
	}
	warm, err := client.Claim(
		ctx,
		"tier-warm",
		types.Command{
			Action:     "claim",
			TemplateID: template.ID,
			TTLSeconds: 600,
			Policy: types.Policy{
				Mode: "none",
			},
			Env: map[string]types.EnvVar{
				"PROVISIONING_TIER": {
					Value: "warm",
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	owned = append(owned, warm.Sandbox.ID)
	after, err := ec.getVM(ctx, warm.Sandbox.VMID)
	if err != nil || warm.Sandbox.ProvisionSource != "warm" || warm.Sandbox.VMID != warmID || after.PID != before.PID ||
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
	if got := scenario.guestExec(
		t,
		ctx,
		warm.Sandbox.ID,
		"cat /workspace/template-warmup; printf ':%s' \"$PROVISIONING_TIER\"",
	); got != "prepared:warm" {
		t.Fatalf("warm claim lost template data or activation environment: %q", got)
	}
	warmIdentity := scenario.guestExec(t, ctx, warm.Sandbox.ID, "cat /etc/machine-id")
	if len(strings.TrimSpace(warmIdentity)) != 32 || warmIdentity == goldenIdentity {
		t.Fatal("golden and warm claims did not receive independent guest identities")
	}
	recordVMMProof(t, artifacts, "warm", after)
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
	writeArtifact(t, artifacts, "provisioning-tiers.json", proof)
	t.Logf(
		"real template_build warmup+prepare_template, golden clone, and target_warm=1 ready-pool claim passed; warm PID %d was reused",
		after.PID,
	)
}

// Haven compensation can remove an exited VM's directory before a failing
// test returns. Hold only diagnostic log FDs so its exact console survives that
// cleanup; disks, memory backings and lifecycle operations are unaffected.
func retainConsoles(t *testing.T, artifacts, cgroup string) {
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
						name := filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(path)))) + "-" + filepath.Base(filepath.Dir(path))
						_ = os.WriteFile(filepath.Join(artifacts, name+"-console.log"), data, 0o600)
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
							retainScopeMemory(artifacts, filepath.Join(cgroup, entry.Name()), stop),
						)
					}
				}
				// Retain through the backing mount: a FUSE open would create a
				// .fuse_hidden entry on unlink and interfere with real VM removal.
				paths, _ := filepath.Glob("/run/p/backing/vms/*/runs/*/console.log")
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
	t.Cleanup(func() {
		close(stop)
		<-done
	})
}

// memory.events supports POLLPRI notifications. Retain the last readable counters
// before Haven compensation removes the owned scope, including an OOM kill.
func retainScopeMemory(artifacts, scope string, stop <-chan struct{}) <-chan struct{} {
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
				_ = os.WriteFile(filepath.Join(artifacts, filepath.Base(scope)+"-memory.json"), data, 0o600)
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
			for _, name := range []string{
				"memory.max",
				"memory.current",
				"memory.peak",
				"memory.stat",
			} {
				if data, err := os.ReadFile(filepath.Join(scope, name)); err == nil {
					proof[name] = string(data)
				}
			}
			select {
			case <-stop:
				return
			default:
			}
			poll := []unix.PollFd{
				{
					Fd:     int32(events.Fd()),
					Events: unix.POLLPRI | unix.POLLERR,
				},
			}
			if _, err := unix.Poll(poll, 100); err != nil && err != unix.EINTR {
				return
			}
		}
	}()
	return done
}

func requireEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required; use scripts/run-runtime.sh", name)
	}
	return value
}

func writeArtifact(t *testing.T, directory, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(directory, name), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func copyFixtureFile(t *testing.T, source, dest string, mode os.FileMode) {
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

type daemonProcess struct {
	cmd     *exec.Cmd
	done    chan error
	stopped bool
}

type processCommand struct {
	Name   string
	Binary string
	Args   []string
}

func startProcess(t *testing.T, artifacts string, command processCommand) *daemonProcess {
	name, binary, args := command.Name, command.Binary, command.Args
	t.Helper()
	log, err := os.OpenFile(filepath.Join(artifacts, name+".log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, args...)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	process := &daemonProcess{
		cmd:  cmd,
		done: make(chan error, 1),
	}
	go func() {
		process.done <- cmd.Wait()
		log.Close()
	}()
	return process
}

func (p *daemonProcess) stop(t *testing.T) {
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

func awaitProcess(t *testing.T, ctx context.Context, process *daemonProcess, ready func(context.Context) error) {
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
			t.Fatalf("real daemon exited before readiness: %v (last readiness error: %v)", err, last)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-deadline.C:
			t.Fatalf("real daemon readiness: %v", last)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (scenario runtimeScenario) guestExec(t *testing.T, ctx context.Context, id, command string) string {
	client := scenario.Client
	t.Helper()
	conn, err := client.Connect(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var stdout, stderr bytes.Buffer
	code, err := conn.Exec(ctx, command, &stdout, &stderr)
	if err != nil || code != 0 {
		t.Fatalf("real guest exec: %v code=%d stderr=%s", err, code, stderr.String())
	}
	return stdout.String()
}

func recordVMMProof(t *testing.T, artifacts, stage string, vm vmObservation) {
	t.Helper()
	data, err := json.MarshalIndent(vm, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeArtifact(t, artifacts, "vmm-"+stage+".json", data)
	var status []byte
	for _, name := range []string{
		"status",
		"cgroup",
		"mountinfo",
		"stat",
		"maps",
	} {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/%s", vm.PID, name))
		if err != nil {
			t.Fatalf("inspect actual dropped VMM %s: %v", name, err)
		}
		writeArtifact(t, artifacts, "vmm-"+stage+"-"+name+".txt", data)
		if name == "status" {
			status = data
		}
	}
	for _, want := range []string{
		"Uid:\t65534\t65534\t65534\t65534",
		"Gid:\t65534\t65534\t65534\t65534",
		"CapInh:\t0000000000000000",
		"CapPrm:\t0000000000000000",
		"CapEff:\t0000000000000000",
		"CapBnd:\t0000000000000000",
		"CapAmb:\t0000000000000000",
		"NoNewPrivs:\t1",
		"Seccomp:\t2",
	} {
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
	for _, name := range []string{
		"mnt",
		"pid",
		"ipc",
		"uts",
		"net",
	} {
		parent, err := os.Readlink("/proc/self/ns/" + name)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/%s", vm.PID, name))
		if err != nil || actual == parent {
			t.Fatalf("VMM %s namespace was not isolated: harness=%s VMM=%s: %v", name, parent, actual, err)
		}
		fmt.Fprintf(&namespaces, "%s harness=%s VMM=%s\n", name, parent, actual)
	}
	writeArtifact(t, artifacts, "vmm-"+stage+"-namespaces.txt", []byte(namespaces.String()))
	root := fmt.Sprintf("/proc/%d/root", vm.PID)
	for _, forbidden := range []string{
		".oldroot",
		"home",
		"nix/store",
		"etc/shadow",
	} {
		if _, err := os.Lstat(filepath.Join(root, forbidden)); !os.IsNotExist(err) {
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
		writeArtifact(
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

// These public addresses exist only on the fresh outer namespace's loopback.
// Every probe crosses the real guest virtio/TAP, namespace policy and SNAT;
// the namespace has no external interface or route to the actual Internet.
func (scenario runtimeScenario) probeGuestTCP(t *testing.T, ctx context.Context) {
	client, ec, artifacts, image := scenario.Client, scenario.Inventory, scenario.Artifacts, scenario.Image
	t.Helper()
	const allowed, denied = "93.184.216.34", "93.184.216.35"
	for _, address := range []string{
		allowed,
		denied,
	} {
		if out, err := exec.CommandContext(ctx, "ip", "addr", "add", address+"/32", "dev", "lo").
			CombinedOutput(); err != nil {
			t.Fatalf("private loopback TCP target: %v: %s", err, out)
		}
	}
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	type observation struct {
		Stage    string `json:"stage"`
		RemoteIP string `json:"remote_ip"`
	}
	observed := make(chan observation, 16)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			line, err := bufio.NewReaderSize(conn, 256).ReadString('\n')
			if err == nil && len(line) < 128 {
				stage := strings.TrimSpace(line)
				_, _ = fmt.Fprintf(conn, "reply-%s\n", stage)
				observed <- observation{
					Stage:    stage,
					RemoteIP: conn.RemoteAddr().(*net.TCPAddr).IP.String(),
				}
			}
			conn.Close()
		}
	}()
	defer func() {
		listener.Close()
		<-serverDone
	}()
	_, err = client.PutTemplate(
		ctx,
		"tcp-template",
		types.Template{
			ID:    "runtime-tcp",
			Image: image,
			Shape: types.Shape{
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
	var owned []string
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for _, id := range owned {
			if _, err := client.Change(
				cleanup,
				id,
				"tcp-release-"+id,
				types.Command{
					Action: "release",
				},
			); err != nil {
				t.Errorf("release TCP sandbox: %v", err)
			}
		}
	}()
	claim := func(key, mode string) types.Sandbox {
		t.Helper()
		policy := types.Policy{
			Mode: mode,
		}
		if mode == "direct" {
			policy.AllowedCIDRs = []string{
				allowed + "/32",
			}
			policy.MaxConnections = 16
		}
		result, err := client.Claim(
			ctx,
			"tcp-claim-"+key,
			types.Command{
				Action:     "claim",
				TemplateID: "runtime-tcp",
				TTLSeconds: 600,
				Policy:     policy,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		owned = append(owned, result.Sandbox.ID)
		return *result.Sandbox
	}
	var proof []observation
	probe := func(sandbox types.Sandbox, stage, address string, want bool) {
		t.Helper()
		// The supplied guest's /bin/sh is real Bash. Locate its existing coreutils
		// timeout through /bin/cat; no guest binary or network operation is mocked.
		script := fmt.Sprintf(
			"exec 9<>/dev/tcp/%s/%d; printf '%s\\n' >&9; IFS= read -r reply <&9; test \"$reply\" = 'reply-%s'",
			address,
			port,
			stage,
			stage,
		)
		command := fmt.Sprintf(
			"tool=$(/bin/readlink /bin/cat); if \"${tool%%/*}/timeout\" 4 /bin/sh -c '%s'; then printf allowed; else printf blocked; fi",
			strings.ReplaceAll(script, "'", "'\\''"),
		)
		got := scenario.guestExec(t, ctx, sandbox.ID, command)
		if got != map[bool]string{
			true:  "allowed",
			false: "blocked",
		}[want] {
			t.Fatalf("actual guest TCP %s: got %q allowed=%v", stage, got, want)
		}
		vm, err := ec.getVM(ctx, sandbox.VMID)
		if err != nil || !vm.Verified {
			t.Fatalf("TCP probe VMM lost verification: %+v: %v", vm, err)
		}
		if !want {
			select {
			case event := <-observed:
				t.Fatalf("denied guest TCP reached listener: %+v", event)
			default:
			}
			return
		}
		if !vm.NetworkActual || !vm.NetworkEnabled || vm.NetworkPolicyInstance != vm.Instance {
			t.Fatalf("allowed TCP has no verified current direct policy: %+v", vm)
		}
		select {
		case event := <-observed:
			if event.Stage != stage || !strings.HasPrefix(event.RemoteIP, "10.200.") {
				t.Fatalf("TCP did not cross owned SNAT: %+v", event)
			}
			proof = append(proof, event)
		case <-time.After(time.Second):
			t.Fatal("guest reported TCP success without a real listener observation")
		}
	}
	direct := claim("direct", "direct")
	probe(direct, "source", allowed, true)
	probe(direct, "denied-cidr", denied, false)
	none := claim("none", "none")
	probe(none, "none-policy", allowed, false)
	if _, err := client.Change(ctx, none.ID, "tcp-release-none", types.Command{
		Action: "release",
	}); err != nil {
		t.Fatal(err)
	}
	owned = owned[:1]
	forked, err := client.Change(ctx, direct.ID, "tcp-fork", types.Command{
		Action: "fork",
		Count:  1,
	})
	if err != nil || len(forked.Sandboxes) != 1 {
		t.Fatalf("capture/fork for real TCP rebind: %+v: %v", forked.Sandboxes, err)
	}
	branch := forked.Sandboxes[0]
	owned = append(owned, branch.ID)
	probe(branch, "fork", allowed, true)
	probe(branch, "fork-denied", denied, false)
	if _, err := client.Change(ctx, direct.ID, "tcp-pause", types.Command{
		Action: "pause",
	}); err != nil {
		t.Fatal(err)
	}
	// Connecting performs the actual sandbox auto-resume and instance rebind.
	scenario.guestExec(t, ctx, direct.ID, "printf resumed")
	resumed, err := client.Get(ctx, direct.ID)
	if err != nil {
		t.Fatal(err)
	}
	probe(resumed, "resume", allowed, true)
	probe(resumed, "resume-denied", denied, false)
	data, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeArtifact(t, artifacts, "guest-tcp.json", data)
	t.Log(
		"real guest virtio/TAP + policy + SNAT TCP passed for source, fork and resume; denied CIDR and none policy blocked",
	)
}

// vmInventory observes only the isolated run's explicitly supplied
// store and verifies its exact resource through the real host inspector. It has
// no SQL writes, admission, lifecycle operations or daemon control protocol.
type vmInventory struct {
	db           *sql.DB
	backend      *engine.CloudHypervisor
	ownedBackend bool
}

type vmObservation struct {
	ID                    string
	Instance              string
	PID                   int
	StartTime             uint64
	Verified              bool
	AgentReady            bool
	NetworkEnabled        bool
	NetworkActual         bool
	NetworkPolicyInstance string
}

func newVMInventory(t *testing.T, state string, backends ...*engine.CloudHypervisor) *vmInventory {
	t.Helper()
	inventory := &vmInventory{}
	if len(backends) == 1 {
		inventory.backend = backends[0]
	} else {
		inventory.ownedBackend = true
		inventory.backend = engine.NewCloudHypervisor(engine.CloudHypervisorOptions{
			Binary:       requireEnv(t, "SANDBOX_E2E_HYPERVISOR"),
			BuildReceipt: requireEnv(t, "SANDBOX_E2E_BUILD_RECEIPT"),
			CgroupRoot:   requireEnv(t, "SANDBOX_E2E_CGROUP_ROOT"),
			IOMax:        requireEnv(t, "SANDBOX_E2E_IO_MAX"),
			UID:          vmmIdentity(t, "SANDBOX_E2E_VMM_UID"),
			GID:          vmmIdentity(t, "SANDBOX_E2E_VMM_GID"),
		})
	}
	var err error
	inventory.db, err = sql.Open("sqlite", "file:"+filepath.Join(state, "haven.db")+"?mode=ro&_pragma=busy_timeout(1000)")
	if err != nil {
		t.Fatal(err)
	}
	return inventory
}

func vmmIdentity(t *testing.T, name string) uint32 {
	t.Helper()
	value, err := strconv.ParseUint(requireEnv(t, name), 10, 32)
	if err != nil || value == 0 {
		t.Fatalf("%s must be a nonzero 32-bit VMM identity", name)
	}
	return uint32(value)
}

func (inventory *vmInventory) close() {
	_ = inventory.db.Close()
	if inventory.ownedBackend {
		_ = inventory.backend.Close()
	}
}

func (inventory *vmInventory) getVM(ctx context.Context, id string) (vmObservation, error) {
	var vm vmObservation
	current, err := machine.LoadOwnership(ctx, inventory.db, id)
	if err != nil {
		return vm, err
	}
	record := current.Record
	if record.Resource.VMID != id || record.Resource.Instance == "" {
		return vm, fmt.Errorf("VM observation lacks exact resource identity")
	}
	observation, err := inventory.backend.Inspect(ctx, record.Resource)
	if err != nil || !observation.Alive {
		return vm, fmt.Errorf("actual VM inspection: alive=%v: %w", observation.Alive, err)
	}
	vm = vmObservation{
		ID:            id,
		Instance:      record.Resource.Instance,
		PID:           observation.PID,
		StartTime:     observation.StartTime,
		Verified:      record.Verified,
		AgentReady:    record.AgentReady,
		NetworkActual: observation.Network,
	}
	witness, err := machine.LoadPolicyWitness(ctx, inventory.db, id)
	if err == nil && witness.PolicyHash.Valid {
		retirement, err := machine.LoadDirectRetirement(ctx, inventory.db, witness.OperationID)
		if err != nil {
			return vm, err
		}
		policy := retirement.Input.Policy
		if machine.InstalledPolicyHash(policy) != witness.PolicyHash {
			return vm, fmt.Errorf("installed network policy differs from retirement witness")
		}
		vm.NetworkPolicyInstance = witness.Instance
		if vm.NetworkPolicyInstance != vm.Instance {
			return vm, fmt.Errorf("direct policy belongs to another instance")
		}
		if err = inventory.backend.VerifyNetworkPolicy(ctx, record.Resource, policy); err != nil {
			return vm, err
		}
		vm.NetworkEnabled = true
	} else if err != nil && err != sql.ErrNoRows {
		return vm, err
	}
	return vm, nil
}

func (inventory *vmInventory) listVMs(ctx context.Context) ([]string, error) {
	rows, err := inventory.db.QueryContext(ctx, "SELECT id FROM vms ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

type runningHaven struct {
	process *daemonProcess
	client  *sdk.Client
	token   string
	args    []string
}

type havenLaunch struct {
	Artifacts string
	Materials string
	Cgroup    string
	IOMax     string
	Layout    string
	Image     string
}

func startHaven(t *testing.T, ctx context.Context, launch havenLaunch) (*runningHaven, error) {
	artifacts, materials, cgroup, ioMax, layout, image := launch.Artifacts, launch.Materials, launch.Cgroup, launch.IOMax, launch.Layout, launch.Image
	t.Helper()
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, err
	}
	token := hex.EncodeToString(secret[:])
	if err := os.WriteFile("/run/p/root-token", []byte(token), 0o600); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		return nil, err
	}
	args := []string{
		"--state-dir=/run/p/e",
		"--socket=/run/p/manage/control.sock",
		"--listen=" + address,
		"--root-token-file=/run/p/root-token",
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
	retainConsoles(t, artifacts, cgroup)
	process := startProcess(t, artifacts, processCommand{
		Name:   "haven-1",
		Binary: "/run/p/bin/haven",
		Args:   append(append([]string{}, args...), "--init"),
	})
	t.Cleanup(func() {
		process.stop(t)
	})
	client := sdk.New("http://"+address, token)
	awaitProcess(t, ctx, process, func(attempt context.Context) error {
		_, err := client.List(attempt)
		return err
	})
	management := sdk.NewManagement("/run/p/manage/control.sock")
	response, err := management.Manage(ctx, types.ManagementRequest{
		Action: "import",
		Key:    "runtime-import",
		Import: &types.ImageImport{
			Layout:   layout,
			Digest:   image,
			MaxBytes: 512 << 20,
			MaxFiles: 100000,
		},
	})
	if err != nil || response.Operation == nil || response.Operation.Status != "succeeded" || response.Operation.Result.ImageDigest != image {
		return nil, fmt.Errorf("OCI import through Haven: %+v: %w", response, err)
	}
	businessStatus, err := client.Status(ctx)
	if err != nil {
		return nil, err
	}
	managementStatus, err := management.Status(ctx)
	if err != nil || businessStatus.StoreID != managementStatus.StoreID {
		return nil, fmt.Errorf("business and management store differ: %w", err)
	}
	return &runningHaven{
		process: process,
		client:  client,
		token:   token,
		args:    args,
	}, nil
}
