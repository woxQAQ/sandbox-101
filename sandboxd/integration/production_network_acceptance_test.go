//go:build linux && privileged

package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	engineclient "github.com/woxQAQ/sandbox-engine/client"
	"github.com/woxQAQ/sandboxd"
	"github.com/woxQAQ/sandboxd/egress"
	"github.com/woxQAQ/sandboxd/sdk"
)

// These public addresses exist only on the fresh outer namespace's loopback.
// Every probe crosses the real guest virtio/TAP, namespace policy and SNAT;
// the namespace has no external interface or route to the actual Internet.
func productionGuestTCP(t *testing.T, ctx context.Context, client *sdk.Client, ec *engineclient.Client, artifacts, image string) {
	t.Helper()
	const allowed, denied = "93.184.216.34", "93.184.216.35"
	for _, address := range []string{allowed, denied} {
		if out, err := exec.CommandContext(ctx, "ip", "addr", "add", address+"/32", "dev", "lo").CombinedOutput(); err != nil {
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
				observed <- observation{Stage: stage, RemoteIP: conn.RemoteAddr().(*net.TCPAddr).IP.String()}
			}
			conn.Close()
		}
	}()
	defer func() { listener.Close(); <-serverDone }()
	_, err = client.PutTemplate(ctx, "tcp-template", sandboxd.Template{ID: "production-tcp", Image: image, Shape: api.Shape{VCPUs: 1, MemoryMiB: microVMMemoryMiB(t), DiskMiB: 512}}, false)
	if err != nil {
		t.Fatal(err)
	}
	var owned []string
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for _, id := range owned {
			if _, err := client.Change(cleanup, id, "tcp-release-"+id, sandboxd.Command{Action: "release"}); err != nil {
				t.Errorf("release TCP sandbox: %v", err)
			}
		}
	}()
	claim := func(key, mode string) sandboxd.Sandbox {
		t.Helper()
		policy := egress.Policy{Mode: mode}
		if mode == "direct" {
			policy.AllowedCIDRs = []string{allowed + "/32"}
			policy.MaxConnections = 16
		}
		result, err := client.Claim(ctx, "tcp-claim-"+key, sandboxd.Command{Action: "claim", TemplateID: "production-tcp", TTLSeconds: 600, Policy: policy})
		if err != nil {
			t.Fatal(err)
		}
		owned = append(owned, result.Sandbox.ID)
		return *result.Sandbox
	}
	var proof []observation
	probe := func(sandbox sandboxd.Sandbox, stage, address string, want bool) {
		t.Helper()
		// The supplied guest's /bin/sh is real Bash. Locate its existing coreutils
		// timeout through /bin/cat; no guest binary or network operation is mocked.
		script := fmt.Sprintf("exec 9<>/dev/tcp/%s/%d; printf '%s\\n' >&9; IFS= read -r reply <&9; test \"$reply\" = 'reply-%s'", address, port, stage, stage)
		command := fmt.Sprintf("tool=$(/bin/readlink /bin/cat); if \"${tool%%/*}/timeout\" 4 /bin/sh -c '%s'; then printf allowed; else printf blocked; fi", strings.ReplaceAll(script, "'", "'\\''"))
		got := productionExec(t, ctx, client, sandbox.ID, command)
		if got != map[bool]string{true: "allowed", false: "blocked"}[want] {
			t.Fatalf("actual guest TCP %s: got %q allowed=%v", stage, got, want)
		}
		vm, err := ec.GetVM(ctx, sandbox.VMID)
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
	if _, err := client.Change(ctx, none.ID, "tcp-release-none", sandboxd.Command{Action: "release"}); err != nil {
		t.Fatal(err)
	}
	owned = owned[:1]
	forked, err := client.Change(ctx, direct.ID, "tcp-fork", sandboxd.Command{Action: "fork", Count: 1})
	if err != nil || len(forked.Sandboxes) != 1 {
		t.Fatalf("capture/fork for real TCP rebind: %+v: %v", forked.Sandboxes, err)
	}
	branch := forked.Sandboxes[0]
	owned = append(owned, branch.ID)
	probe(branch, "fork", allowed, true)
	probe(branch, "fork-denied", denied, false)
	if _, err := client.Change(ctx, direct.ID, "tcp-pause", sandboxd.Command{Action: "pause"}); err != nil {
		t.Fatal(err)
	}
	// Connecting performs the actual product auto-resume and instance rebind.
	productionExec(t, ctx, client, direct.ID, "printf resumed")
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
	productionArtifact(t, artifacts, "guest-tcp.json", data)
	t.Log("real guest virtio/TAP + policy + SNAT TCP passed for source, fork and resume; denied CIDR and none policy blocked")
}
