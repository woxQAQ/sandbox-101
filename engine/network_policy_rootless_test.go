//go:build linux && kernel_isolation

package engine

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

// The veth guest substitutes for TAP frame injection, but packets traverse the
// real forwarding/conntrack/nft hooks with the production interface names and
// addresses. Install/Verify/Network/Remove are the real CloudHypervisor methods.
// This does not exercise the privileged VMM launcher, CH/TAP fd handoff,
// production host quotas, or daemon adoption. The host-policy receipt and its
// admission sets are fixtures inside the new network namespace.
func TestKernelIsolationRootlessNetwork(t *testing.T) {
	if !rootlessRun(t, "network") {
		return
	}
	rootlessPrivateRun(t)
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	dir := t.TempDir()
	r := Resource{
		VMID:         "kernel-acceptance",
		Instance:     "current-instance",
		Dir:          dir,
		NetworkToken: "kernel-acceptance",
		Slot:         2,
	}
	b := &CloudHypervisor{
		opts: CloudOptions{
			UID:        0,
			HostPolicy: filepath.Join(dir, "host-policy.json"),
		},
	}
	if err := writeJSON(
		b.opts.HostPolicy,
		HostPolicy{Version: 1, StorageQuota: true, NetworkQuotas: true},
	); err != nil {
		t.Fatal(err)
	}
	rootlessWrite(t, "/proc/sys/net/ipv4/ip_forward", "1")
	rootlessNFT(t, "", `table inet sandbox_engine {
 set active_ifaces { type ifname; }
 set active_ips { type ipv4_addr; }
}
`)
	if err := b.networkPrepare(ctx, r); err != nil {
		t.Fatal(err)
	}
	ns, host, _, mapped, hostIP, peerIP := netNames(r)
	guest := "kernel-acceptance-guest"
	rootlessCommand(t, "ip", "netns", "add", guest)
	// Keep the production-created namespace, uplink, NAT and ownership receipt;
	// replace its TAP endpoint with a local kernel peer carrying guest packets.
	rootlessCommand(t, "ip", "-n", ns, "link", "del", "tap0")
	rootlessCommand(
		t,
		"ip",
		"-n",
		ns,
		"link",
		"add",
		"tap0",
		"type",
		"veth",
		"peer",
		"name",
		"eth0",
		"netns",
		guest,
	)
	for _, args := range [][]string{
		{"-n", ns, "link", "set", "tap0", "alias", netAlias(r)},
		{"-n", ns, "link", "set", "tap0", "address", "02:00:00:00:00:01"},
		{"-n", ns, "addr", "add", "192.168.127.1/30", "dev", "tap0"},
		{"-n", guest, "addr", "add", "192.168.127.2/30", "dev", "eth0"},
		{"-n", guest, "link", "set", "lo", "up"},
		{"-n", guest, "link", "set", "eth0", "up"},
		{"-n", guest, "route", "add", "default", "via", "192.168.127.1"},
		{"link", "set", "lo", "up"},
	} {
		rootlessCommand(t, "ip", args...)
	}
	targets := []string{"1.1.1.1", "8.8.8.8", "10.42.0.9", "169.254.169.254"}
	for _, ip := range targets {
		rootlessCommand(t, "ip", "addr", "add", ip+"/32", "dev", "lo")
	}
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var received atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				if _, err := io.CopyN(conn, conn, 4); err == nil {
					received.Add(1)
				}
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	port := listener.Addr().(*net.TCPAddr).Port
	probe := func(ip string, allow bool) {
		t.Helper()
		rootlessNetworkProbe(
			t,
			guest,
			net.JoinHostPort(ip, fmt.Sprint(port)),
			allow,
		)
	}
	links := func(up bool) {
		t.Helper()
		state := "down"
		if up {
			state = "up"
		}
		rootlessCommand(t, "ip", "-n", ns, "link", "set", "tap0", state)
		rootlessCommand(t, "ip", "-n", ns, "link", "set", "uplink", state)
		rootlessCommand(t, "ip", "link", "set", host, state)
		if up {
			// This helper is used only for the unfiltered baseline and the
			// accidental-link-up revocation probe. Recreate routes removed by
			// link-down so missing topology cannot masquerade as an nft drop.
			rootlessCommand(
				t,
				"ip",
				"-n",
				ns,
				"route",
				"replace",
				"default",
				"via",
				hostIP,
				"dev",
				"uplink",
				"onlink",
			)
			rootlessCommand(
				t,
				"ip",
				"route",
				"replace",
				mapped+"/32",
				"via",
				peerIP,
				"dev",
				host,
				"onlink",
			)
		}
	}
	checkDown := func() {
		t.Helper()
		up, err := b.networkInspect(ctx, r)
		if err != nil || up {
			t.Fatalf(
				"owned interfaces did not fail closed: active=%v error=%v",
				up,
				err,
			)
		}
	}
	// First prove each local route, service and return path is usable. Otherwise
	// an unreachable endpoint could be mistaken for firewall enforcement.
	rootlessNFT(t, ns, "delete table inet engine_policy\n")
	links(true)
	for _, ip := range targets {
		probe(ip, true)
	}
	if err := b.Network(ctx, r, false); err != nil {
		t.Logf(
			"actual host links: %s",
			rootlessCommand(t, "ip", "-j", "link", "show"),
		)
		t.Logf(
			"actual namespace links: %s",
			rootlessCommand(t, "ip", "-n", ns, "-j", "link", "show"),
		)
		t.Fatal(err)
	}
	checkDown()
	policy := &api.NetworkPolicy{
		Version:        1,
		AllowedCIDRs:   []string{"1.1.1.1/32"},
		MaxConnections: 64,
	}
	rootlessArtifact(
		t,
		"network-policy.nft",
		[]byte(renderNetworkPolicy(policy)),
	)
	if err := b.InstallNetworkPolicy(ctx, r, policy); err != nil {
		t.Fatal(err)
	}
	if err := b.VerifyNetworkPolicy(ctx, r, policy); err != nil {
		t.Fatal(err)
	}
	checkDown()
	if err := b.Network(ctx, r, true); err != nil {
		t.Fatal(err)
	}
	probe(targets[0], true)
	for _, ip := range targets[1:] {
		probe(ip, false)
		probe(targets[0], true)
	}
	rootlessWantError(
		t,
		b.InstallNetworkPolicy(ctx, r, nil),
		"policy revocation while interfaces are active",
		api.Conflict,
	)
	if err := b.VerifyNetworkPolicy(ctx, r, policy); err != nil {
		t.Fatalf(
			"traffic changed the stateless enforcement fingerprint: %v",
			err,
		)
	}
	rootlessArtifact(
		t,
		"network-ruleset.json",
		rootlessCommand(
			t,
			"ip",
			"netns",
			"exec",
			ns,
			"nft",
			"-j",
			"-s",
			"list",
			"ruleset",
		),
	)
	if err := b.Network(ctx, r, false); err != nil {
		t.Fatal(err)
	}
	checkDown()
	probe(targets[0], false)
	broad := &api.NetworkPolicy{
		Version:        1,
		AllowedCIDRs:   []string{"0.0.0.0/0"},
		MaxConnections: 64,
	}
	if err := b.InstallNetworkPolicy(ctx, r, broad); err != nil {
		t.Fatal(err)
	}
	if err := b.Network(ctx, r, true); err != nil {
		t.Fatal(err)
	}
	probe(targets[0], true)
	probe(targets[1], true)
	probe(targets[2], false)
	probe(targets[3], false)
	if err := b.Network(ctx, r, false); err != nil {
		t.Fatal(err)
	}
	limited := &api.NetworkPolicy{
		Version:        1,
		AllowedCIDRs:   []string{"0.0.0.0/0"},
		MaxConnections: 2,
	}
	if err := b.InstallNetworkPolicy(ctx, r, limited); err != nil {
		t.Fatal(err)
	}
	if err := b.Network(ctx, r, true); err != nil {
		t.Fatal(err)
	}
	// Hold two real connections: the kernel can retire completed TCP flows from
	// ct count before their conntrack entries expire.
	holder := rootlessNetworkFlows(
		t,
		guest,
		[]string{
			net.JoinHostPort(targets[0], fmt.Sprint(port)),
			net.JoinHostPort(targets[1], fmt.Sprint(port)),
		},
	)
	probe(targets[0], false)
	if err := b.VerifyNetworkPolicy(ctx, r, limited); err != nil {
		t.Fatalf(
			"flow accounting changed the stateless enforcement fingerprint: %v",
			err,
		)
	}
	rootlessArtifact(
		t,
		"network-flow-budget.txt",
		[]byte(
			"max_connections=2; two concurrent TCP echoes across distinct public destinations succeeded; third TCP handshake denied while both connections remained open\n",
		),
	)
	holder.release(t)
	if out, err := holder.wait(); err != nil {
		t.Fatalf("bounded TCP flow holder: %v: %s", err, out)
	}
	if err := b.Network(ctx, r, false); err != nil {
		t.Fatal(err)
	}
	if err := b.InstallNetworkPolicy(ctx, r, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.VerifyNetworkPolicy(ctx, r, nil); err != nil {
		t.Fatal(err)
	}
	rootlessWantError(
		t,
		b.Network(ctx, r, true),
		"activation with revoked policy",
		api.Conflict,
	)
	checkDown()
	// Revocation must also drop real packets if interfaces are accidentally up.
	links(true)
	probe(targets[0], false)
	if err := b.Network(ctx, r, false); err != nil {
		t.Fatal(err)
	}
	if err := b.InstallNetworkPolicy(ctx, r, policy); err != nil {
		t.Fatal(err)
	}
	stale := r
	stale.Instance = "later-instance"
	rootlessWantError(
		t,
		b.VerifyNetworkPolicy(ctx, stale, policy),
		"stale instance receipt verification",
		api.Conflict,
	)
	rootlessWantError(
		t,
		b.Network(ctx, stale, true),
		"stale instance activation",
		api.Conflict,
	)
	checkDown()
	// Also exercise the engine-level committed-instance helper without a fake
	// backend. It must reject a stale commit before consulting kernel permission.
	e := &Engine{opts: Options{Backend: b}}
	v := &vmRecord{
		Resource: stale,
		VM: api.VM{
			Instance:              stale.Instance,
			NetworkPolicy:         policy,
			NetworkPolicyInstance: r.Instance,
		},
	}
	rootlessWantError(
		t,
		e.verifyNetworkPolicy(ctx, v),
		"stale engine policy commit",
		api.Conflict,
	)
	if err := b.Network(ctx, r, true); err != nil {
		t.Fatal(err)
	}
	probe(targets[0], true)
	rootlessNFT(t, ns, "insert rule inet engine_policy forward accept\n")
	rootlessWantError(
		t,
		b.VerifyNetworkPolicy(ctx, r, policy),
		"kernel ruleset tampering",
		api.Unknown,
	)
	rootlessWantError(
		t,
		b.Network(ctx, r, true),
		"tampered rules activation",
		api.Unknown,
	)
	checkDown()
	probe(targets[0], false)
	if err := b.networkRemove(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat("/run/netns/" + ns); !os.IsNotExist(err) {
		t.Fatalf("owned namespace anchor remains: %v", err)
	}
	rootlessCommand(t, "ip", "netns", "del", guest)
	t.Logf(
		"real TCP echoes=%d; verified public allowlist, private/metadata deny under /0, shared two-flow saturation, link disable, nil-policy revoke, instance binding, ruleset tamper fail-closed and owned cleanup",
		received.Load(),
	)
}

func rootlessWantError(t *testing.T, err error, label string, code api.Code) {
	t.Helper()
	var failure *api.Error
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("%s: wanted %s, got %v", label, code, err)
	}
	t.Logf("%s rejected: %v", label, err)
}

func rootlessNFT(t *testing.T, ns, rules string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if ns != "" {
		if err := namespaceNFT(ctx, ns, rules); err != nil {
			t.Fatal(err)
		}
		return
	}
	cmd := exec.CommandContext(ctx, "nft", "-f", "-")
	cmd.Stdin = strings.NewReader(rules)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("private nft fixture: %v: %s", err, out)
	}
}

func rootlessNetworkProbe(t *testing.T, ns, target string, allow bool) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(
		ctx,
		"ip",
		"netns",
		"exec",
		ns,
		exe,
		"-test.run=^TestKernelIsolationRootlessProbe$",
		"-test.timeout=3s",
	)
	cmd.Env = append(
		os.Environ(),
		rootlessChildEnv+"=probe",
		"SANDBOX_KERNEL_PROBE_TARGET="+target,
		fmt.Sprintf("SANDBOX_KERNEL_PROBE_ALLOW=%t", allow),
		"GOMAXPROCS=1",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Logf(
			"guest addresses: %s",
			rootlessCommand(t, "ip", "-n", ns, "-j", "addr", "show"),
		)
		t.Logf(
			"guest routes: %s",
			rootlessCommand(
				t,
				"ip",
				"-n",
				ns,
				"-j",
				"route",
				"show",
				"table",
				"all",
			),
		)
		t.Logf(
			"router routes: %s",
			rootlessCommand(
				t,
				"ip",
				"-n",
				"sbe-kernel-acceptance",
				"-j",
				"route",
				"show",
				"table",
				"all",
			),
		)
		t.Logf(
			"outer routes: %s",
			rootlessCommand(t, "ip", "-j", "route", "show", "table", "all"),
		)
		t.Logf(
			"router nft rules: %s",
			rootlessCommand(
				t,
				"ip",
				"netns",
				"exec",
				"sbe-kernel-acceptance",
				"nft",
				"list",
				"ruleset",
			),
		)
		t.Fatalf("TCP probe %s allow=%v: %v: %s", target, allow, err, out)
	}
	t.Logf("real TCP %s allowed=%v", target, allow)
}

func rootlessNetworkFlows(
	t *testing.T,
	ns string,
	targets []string,
) *rootlessOwnedProcess {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cmd := exec.CommandContext(
		ctx,
		"ip",
		"netns",
		"exec",
		ns,
		exe,
		"-test.run=^TestKernelIsolationRootlessFlowHolder$",
		"-test.timeout=9s",
	)
	cmd.Env = append(
		os.Environ(),
		rootlessChildEnv+"=flow-holder",
		"SANDBOX_KERNEL_FLOW_TARGETS="+strings.Join(targets, ","),
		"GOMAXPROCS=1",
	)
	input, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	owned := &rootlessOwnedProcess{
		cmd:    cmd,
		input:  input,
		output: bufio.NewReader(output),
		cancel: cancel,
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_, _ = owned.wait()
		}
		cancel()
	})
	if ready, err := owned.output.ReadString(
		'\n',
	); err != nil ||
		ready != "ready\n" {
		t.Fatalf("TCP connections did not become ready: %q: %v", ready, err)
	}
	return owned
}

func TestKernelIsolationRootlessFlowHolder(t *testing.T) {
	if os.Getenv(rootlessChildEnv) != "flow-holder" {
		t.Skip("bounded TCP subprocess only")
	}
	rootlessRequireNamespaces(t)
	for _, target := range strings.Split(os.Getenv("SANDBOX_KERNEL_FLOW_TARGETS"), ",") {
		conn, err := net.DialTimeout("tcp4", target, 1500*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(450 * time.Millisecond))
		if _, err := conn.Write([]byte("nft!")); err != nil {
			t.Fatal(err)
		}
		var echo [4]byte
		if _, err := io.ReadFull(
			conn,
			echo[:],
		); err != nil ||
			string(echo[:]) != "nft!" {
			t.Fatalf("held flow echo=%q error=%v", echo, err)
		}
		_ = conn.SetDeadline(time.Time{})
	}
	fmt.Println("ready")
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
}

func TestKernelIsolationRootlessProbe(t *testing.T) {
	if os.Getenv(rootlessChildEnv) != "probe" {
		t.Skip("private network subprocess only")
	}
	rootlessRequireNamespaces(t)
	allow := os.Getenv("SANDBOX_KERNEL_PROBE_ALLOW") == "true"
	dialTimeout := 450 * time.Millisecond
	if allow {
		// A new route after link activation may require ARP and a TCP SYN
		// retransmission. Allow one retransmit while keeping every probe bounded.
		dialTimeout = 1500 * time.Millisecond
	}
	conn, err := net.DialTimeout(
		"tcp4",
		os.Getenv("SANDBOX_KERNEL_PROBE_TARGET"),
		dialTimeout,
	)
	if !allow {
		if err == nil {
			_ = conn.Close()
			t.Fatal("forbidden TCP connection succeeded")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(450 * time.Millisecond))
	if _, err := conn.Write([]byte("nft!")); err != nil {
		t.Fatal(err)
	}
	var echo [4]byte
	if _, err := io.ReadFull(
		conn,
		echo[:],
	); err != nil ||
		string(echo[:]) != "nft!" {
		t.Fatalf("echo=%q error=%v", echo, err)
	}
}
