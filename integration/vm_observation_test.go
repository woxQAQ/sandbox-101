//go:build linux && privileged

package integration_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/sdk"
)

// vmInventory observes only the isolated run's explicitly supplied
// store and verifies its exact resource through the real host inspector. It has
// no SQL writes, admission, lifecycle operations or daemon control protocol.
type vmInventory struct {
	db           *sql.DB
	backend      *host.CloudHypervisor
	ownedBackend bool
}

type vmObservation struct {
	ID, Instance                  string
	PID                           int
	StartTime                     uint64
	Verified, AgentReady          bool
	NetworkEnabled, NetworkActual bool
	NetworkPolicyInstance         string
}

func newVMInventory(t *testing.T, state string, backends ...*host.CloudHypervisor) *vmInventory {
	t.Helper()
	inventory := &vmInventory{}
	if len(backends) == 1 {
		inventory.backend = backends[0]
	} else {
		inventory.ownedBackend = true
		inventory.backend = host.NewCloudHypervisor(host.CloudOptions{
			Binary:       requireEnv(t, "SANDBOX_E2E_HYPERVISOR"),
			BuildReceipt: requireEnv(t, "SANDBOX_E2E_BUILD_RECEIPT"),
			CgroupRoot:   requireEnv(t, "SANDBOX_E2E_CGROUP_ROOT"),
			IOMax:        requireEnv(t, "SANDBOX_E2E_IO_MAX"),
			UID:          vmmIdentity(t, "SANDBOX_E2E_VMM_UID"), GID: vmmIdentity(t, "SANDBOX_E2E_VMM_GID"),
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

func (inventory *vmInventory) Close() {
	_ = inventory.db.Close()
	if inventory.ownedBackend {
		_ = inventory.backend.Close()
	}
}

func (inventory *vmInventory) GetVM(ctx context.Context, id string) (vmObservation, error) {
	var vm vmObservation
	var raw []byte
	if err := inventory.db.QueryRowContext(ctx, "SELECT body FROM vms WHERE id=?", id).Scan(&raw); err != nil {
		return vm, err
	}
	var record struct {
		Resource   loom.Resource `json:"resource"`
		Verified   bool          `json:"verified"`
		AgentReady bool          `json:"agent_ready"`
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		return vm, err
	}
	if record.Resource.VMID != id || record.Resource.Instance == "" {
		return vm, fmt.Errorf("VM observation lacks exact resource identity")
	}
	observation, err := inventory.backend.Inspect(ctx, record.Resource)
	if err != nil || !observation.Alive {
		return vm, fmt.Errorf("actual VM inspection: alive=%v: %w", observation.Alive, err)
	}
	vm = vmObservation{ID: id, Instance: record.Resource.Instance, PID: observation.PID, StartTime: observation.StartTime, Verified: record.Verified, AgentReady: record.AgentReady, NetworkActual: observation.Network}
	var policyRaw []byte
	err = inventory.db.QueryRowContext(ctx, "SELECT instance,body FROM network_policies WHERE vm_id=?", id).Scan(&vm.NetworkPolicyInstance, &policyRaw)
	if err == nil {
		var policy loom.NetworkPolicy
		if err = json.Unmarshal(policyRaw, &policy); err != nil {
			return vm, err
		}
		if vm.NetworkPolicyInstance != vm.Instance {
			return vm, fmt.Errorf("direct policy belongs to another instance")
		}
		if err = inventory.backend.VerifyNetworkPolicy(ctx, record.Resource, &policy); err != nil {
			return vm, err
		}
		vm.NetworkEnabled = true
	} else if err != sql.ErrNoRows {
		return vm, err
	}
	return vm, nil
}

func (inventory *vmInventory) ListVMs(ctx context.Context) ([]string, error) {
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

func startHaven(t *testing.T, ctx context.Context, artifacts, materials, cgroup, ioMax, layout, image string) (*runningHaven, error) {
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
		"--state-dir=/run/p/e", "--socket=/run/p/manage/control.sock", "--listen=" + address,
		"--root-token-file=/run/p/root-token", "--cloud-hypervisor=" + filepath.Join(materials, "cloud-hypervisor"),
		"--build-receipt=" + filepath.Join(materials, "cloud-hypervisor.json"), "--cgroup-root=" + cgroup,
		"--host-policy=" + filepath.Join(artifacts, "host-policy.json"), "--io-max=" + ioMax,
		"--vmm-uid=65534", "--vmm-gid=65534", "--max-memory-mib=4096", "--max-vcpus=8",
	}
	retainConsoles(t, artifacts, cgroup)
	process := startProcess(t, artifacts, "haven-1", "/run/p/bin/haven", append(append([]string{}, args...), "--init")...)
	t.Cleanup(func() { process.stop(t) })
	client := sdk.New("http://"+address, token)
	awaitProcess(t, ctx, process, func(attempt context.Context) error { _, err := client.List(attempt); return err })
	management := sdk.NewManagement("/run/p/manage/control.sock")
	response, err := management.Manage(ctx, api.ManagementRequest{Action: "import", Key: "runtime-import", Import: &api.ImageImport{Layout: layout, Digest: image, MaxBytes: 512 << 20, MaxFiles: 100000}})
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
	return &runningHaven{process: process, client: client, token: token, args: args}, nil
}
