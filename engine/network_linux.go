package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/woxQAQ/sandbox-engine/api"
)

type networkReceipt struct {
	Token string `json:"token"`
	Inode uint64 `json:"inode"`
}
type linkInfo struct {
	Name  string   `json:"ifname"`
	Alias string   `json:"ifalias"`
	Flags []string `json:"flags"`
	Index int      `json:"ifindex"`
	Peer  int      `json:"link_index"`
}

func hostToolPath() string {
	parts := []string{}
	for _, p := range filepath.SplitList(os.Getenv("PATH")) {
		if filepath.IsAbs(p) {
			parts = append(parts, p)
		}
	}
	parts = append(parts, "/usr/sbin", "/usr/bin", "/sbin", "/bin")
	return strings.Join(parts, ":")
}

func command(ctx context.Context, name string, args ...string) ([]byte, error) {
	c := exec.CommandContext(ctx, name, args...)
	c.Env = []string{"PATH=" + hostToolPath(), "LC_ALL=C"}
	b, e := c.CombinedOutput()
	if e != nil {
		return nil, fmt.Errorf("%s %v: %w: %.4096s", name, args, e, b)
	}
	return b, nil
}

func netNames(r Resource) (ns, host, peer, mapped, hostIP, peerIP string) {
	n := r.Slot * 2
	return "sbe-" + r.NetworkToken, fmt.Sprintf(
			"sb%05dh",
			r.Slot,
		), "uplink", fmt.Sprintf(
			"10.200.%d.%d",
			r.Slot/256,
			r.Slot%256,
		), fmt.Sprintf(
			"10.201.%d.%d",
			n/256,
			n%256+1,
		), fmt.Sprintf(
			"10.201.%d.%d",
			n/256,
			n%256,
		)
}

func netAlias(r Resource) string {
	return "sandbox-engine:" + r.VMID + ":" + r.NetworkToken
}

func netIP(ctx context.Context, ns string, args ...string) ([]byte, error) {
	return command(ctx, "ip", append([]string{"-n", ns}, args...)...)
}

func (b *CloudHypervisor) networkPrepare(
	ctx context.Context,
	r Resource,
) error {
	ns, host, peer, mapped, hostIP, peerIP := netNames(r)
	receiptPath := filepath.Join(r.Dir, "network.json")
	var receipt networkReceipt
	if e := readJSON(receiptPath, &receipt); e == nil {
		_, e = b.networkInspect(ctx, r)
		return e
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if _, e := os.Stat("/run/netns/" + ns); e == nil {
		return api.Fail(
			api.ManualRecovery,
			"network namespace exists without completed ownership receipt; preserve and diagnose %s",
			ns,
		)
	}
	if _, e := command(ctx, "ip", "netns", "add", ns); e != nil {
		return e
	}
	// Never delete a partially prepared namespace on error. The durable VM intent
	// and named namespace remain a recovery anchor for explicit diagnosis.
	if _, e := command(
		ctx,
		"ip",
		"link",
		"add",
		host,
		"type",
		"veth",
		"peer",
		"name",
		peer,
		"netns",
		ns,
	); e != nil {
		return e
	}
	if _, e := command(
		ctx,
		"ip",
		"link",
		"set",
		host,
		"alias",
		netAlias(r),
	); e != nil {
		return e
	}
	for _, args := range [][]string{{"link", "set", peer, "alias", netAlias(r)}, {"tuntap", "add", "dev", "tap0", "mode", "tap", "user", strconv.Itoa(int(b.opts.UID))}, {"link", "set", "tap0", "alias", netAlias(r)}, {"link", "set", "tap0", "address", "02:00:00:00:00:01", "mtu", "1500"}, {"link", "set", peer, "mtu", "1500"}, {"addr", "add", "192.168.127.1/30", "dev", "tap0"}, {"addr", "add", peerIP + "/31", "dev", peer}, {"link", "set", "lo", "up"}} {
		if _, e := netIP(ctx, ns, args...); e != nil {
			return e
		}
	}
	for _, args := range [][]string{{"link", "set", host, "mtu", "1500"}, {"addr", "add", hostIP + "/31", "dev", host}} {
		if _, e := command(ctx, "ip", args...); e != nil {
			return e
		}
	}
	if _, e := command(
		ctx,
		"ip",
		"netns",
		"exec",
		ns,
		"sysctl",
		"-qw",
		"net.ipv4.ip_forward=1",
		"net.ipv6.conf.all.disable_ipv6=1",
		"net.ipv6.conf.default.disable_ipv6=1",
	); e != nil {
		return e
	}
	rules := fmt.Sprintf(`table ip engine_vm {
 chain pre { type nat hook prerouting priority dstnat; policy accept; iifname "uplink" ip daddr %s dnat to 192.168.127.2; }
 chain post { type nat hook postrouting priority srcnat; policy accept; oifname "uplink" ip saddr 192.168.127.2 snat to %s; }
}
`, mapped, mapped)
	// NAT alone never grants access. The initial inet policy blocks both IPv4
	// and IPv6 forwarding and all VMM sockets except namespace loopback.
	rules += renderNetworkPolicy(nil)
	if e := namespaceNFT(ctx, ns, rules); e != nil {
		return e
	}
	// Gateway routes require UP next-hop devices and disappear when those
	// devices go down. Network(enable) installs them before opening the TAP.
	st, e := os.Stat("/run/netns/" + ns)
	if e != nil {
		return e
	}
	receipt = networkReceipt{
		Token: r.NetworkToken,
		Inode: st.Sys().(*syscall.Stat_t).Ino,
	}
	return writeJSON(receiptPath, receipt)
}

func (b *CloudHypervisor) networkInspect(
	ctx context.Context,
	r Resource,
) (bool, error) {
	ns, host, peer, _, _, _ := netNames(r)
	var receipt networkReceipt
	err := readJSON(filepath.Join(r.Dir, "network.json"), &receipt)
	if errors.Is(err, os.ErrNotExist) {
		if _, x := os.Stat("/run/netns/" + ns); os.IsNotExist(x) {
			return false, nil
		}
		return false, api.Fail(
			api.ManualRecovery,
			"network preparation incomplete; ownership receipt absent",
		)
	}
	if err != nil {
		return false, err
	}
	var removing networkReceipt
	if x := readJSON(
		filepath.Join(r.Dir, "network-removing.json"),
		&removing,
	); x == nil {
		if removing != receipt {
			return false, api.Fail(
				api.ManualRecovery,
				"network cleanup receipt mismatch",
			)
		}
		st, x := os.Stat("/run/netns/" + ns)
		if os.IsNotExist(x) {
			return false, nil
		}
		if x != nil {
			return false, x
		}
		if st.Sys().(*syscall.Stat_t).Ino != receipt.Inode {
			return false, api.Fail(
				api.ManualRecovery,
				"namespace replaced during cleanup",
			)
		}
		return false, nil
	} else if !os.IsNotExist(
		x,
	) {
		return false, x
	}
	st, err := os.Stat("/run/netns/" + ns)
	if err != nil {
		return false, api.Fail(
			api.ManualRecovery,
			"owned namespace missing; cannot prove host resources were cleaned",
		)
	}
	if receipt.Token != r.NetworkToken ||
		receipt.Inode != st.Sys().(*syscall.Stat_t).Ino {
		return false, api.Fail(
			api.ManualRecovery,
			"network namespace identity mismatch",
		)
	}
	var links []linkInfo
	out, err := command(ctx, "ip", "-j", "link", "show", "dev", host)
	if err != nil {
		return false, err
	}
	if err = json.Unmarshal(out, &links); err != nil {
		return false, err
	}
	if len(links) != 1 || links[0].Alias != netAlias(r) {
		return false, api.Fail(
			api.ManualRecovery,
			"host veth ownership mismatch",
		)
	}
	hostLink := links[0]
	out, err = netIP(ctx, ns, "-j", "link", "show")
	if err != nil {
		return false, err
	}
	// Unmarshal may reuse each nested Flags slice; keep the host copy separate.
	links = nil
	if err = json.Unmarshal(out, &links); err != nil {
		return false, err
	}
	var tap, up *linkInfo
	for i := range links {
		if links[i].Name == "tap0" {
			tap = &links[i]
		}
		if links[i].Name == peer {
			up = &links[i]
		}
	}
	if tap == nil || up == nil || tap.Alias != netAlias(r) ||
		up.Alias != netAlias(r) ||
		hostLink.Peer != up.Index ||
		up.Peer != hostLink.Index {
		return false, api.Fail(
			api.ManualRecovery,
			"TAP/veth identity or pairing mismatch",
		)
	}
	isUp := func(l linkInfo) bool {
		return slices.Contains(l.Flags, "UP")
	}
	// Conservatively report any active interface. Policy replacement requires
	// every interface down, not merely absence of a complete forwarding path.
	return isUp(hostLink) || isUp(*tap) || isUp(*up), nil
}

// ensureNetworkRoute accepts only an absent route or the route for the verified
// namespace/link identity. Never replace an unrelated host route.
func ensureNetworkRoute(
	ctx context.Context,
	ns, destination, gateway, device string,
) error {
	run := func(args ...string) ([]byte, error) {
		if ns != "" {
			return netIP(ctx, ns, args...)
		}
		return command(ctx, "ip", args...)
	}
	out, err := run("-j", "route", "show", "exact", destination)
	if err != nil {
		return err
	}
	var routes []struct {
		Destination string `json:"dst"`
		Gateway     string `json:"gateway"`
		Device      string `json:"dev"`
		Type        string `json:"type"`
	}
	if err := json.Unmarshal(out, &routes); err != nil {
		return err
	}
	if len(routes) != 0 {
		if len(routes) != 1 ||
			strings.TrimSuffix(
				routes[0].Destination,
				"/32",
			) != strings.TrimSuffix(
				destination,
				"/32",
			) ||
			routes[0].Gateway != gateway ||
			routes[0].Device != device ||
			(routes[0].Type != "" && routes[0].Type != "unicast") {
			return api.Fail(
				api.ManualRecovery,
				"network route ownership mismatch for %s",
				destination,
			)
		}
		return nil
	}
	_, err = run(
		"route",
		"add",
		destination,
		"via",
		gateway,
		"dev",
		device,
		"onlink",
	)
	return err
}

func (b *CloudHypervisor) Network(
	ctx context.Context,
	r Resource,
	enable bool,
) (result error) {
	ns, host, peer, mapped, hostIP, peerIP := netNames(r)
	if _, x := os.Stat(
		filepath.Join(r.Dir, "network-removing.json"),
	); x == nil {
		if enable {
			return api.Fail(api.Conflict, "network cleanup in progress")
		}
		_, x = b.networkInspect(ctx, r)
		return x
	}
	_, err := b.networkInspect(ctx, r)
	if err != nil {
		return err
	}
	if _, err = os.Stat(
		filepath.Join(r.Dir, "network.json"),
	); os.IsNotExist(
		err,
	) {
		if enable {
			return api.Fail(api.Conflict, "network not prepared")
		}
		return nil
	}
	if enable {
		// Set admission precedes link activation. Host or policy verification
		// failure also closes links that were already active before this call.
		defer func() {
			if result != nil {
				result = errors.Join(
					result,
					b.Network(context.WithoutCancel(ctx), r, false),
				)
			}
		}()
		if err = b.checkHostPolicy(ctx); err != nil {
			return err
		}
		var installed networkPolicyReceipt
		if err = readJSON(
			filepath.Join(r.Dir, "network-policy.json"),
			&installed,
		); err != nil {
			return api.Fail(
				api.Conflict,
				"direct policy receipt required: %v",
				err,
			)
		}
		if installed.Policy == nil {
			return api.Fail(api.Conflict, "direct policy revoked")
		}
		if err = b.VerifyNetworkPolicy(ctx, r, installed.Policy); err != nil {
			return err
		}
		for _, elem := range []struct{ set, value string }{{"active_ifaces", `"` + host + `"`}, {"active_ips", mapped}} {
			// add element is idempotent on supported nft versions; an existing element
			// is explicitly checked before insertion.
			if _, x := command(
				ctx,
				"nft",
				"get",
				"element",
				"inet",
				"sandbox_engine",
				elem.set,
				"{",
				elem.value,
				"}",
			); x != nil {
				if _, err = command(
					ctx,
					"nft",
					"add",
					"element",
					"inet",
					"sandbox_engine",
					elem.set,
					"{",
					elem.value,
					"}",
				); err != nil {
					return err
				}
			}
		}
		if _, err = netIP(ctx, ns, "link", "set", peer, "up"); err != nil {
			return err
		}
		if _, err = command(ctx, "ip", "link", "set", host, "up"); err != nil {
			return err
		}
		if err = ensureNetworkRoute(
			ctx,
			ns,
			"default",
			hostIP,
			peer,
		); err != nil {
			return err
		}
		if err = ensureNetworkRoute(
			ctx,
			"",
			mapped+"/32",
			peerIP,
			host,
		); err != nil {
			return err
		}
		_, err = netIP(ctx, ns, "link", "set", "tap0", "up")
		return err
	}
	var errs []error
	for _, a := range [][]string{{"link", "set", "tap0", "down"}, {"link", "set", peer, "down"}} {
		if _, x := netIP(ctx, ns, a...); x != nil {
			errs = append(errs, x)
		}
	}
	if _, x := command(ctx, "ip", "link", "set", host, "down"); x != nil {
		errs = append(errs, x)
	}
	if len(errs) == 0 {
		if active, x := b.networkInspect(ctx, r); x != nil {
			errs = append(errs, x)
		} else if active {
			errs = append(
				errs,
				api.Fail(api.Unknown, "interface shutdown unconfirmed"),
			)
		}
	}
	return errors.Join(errs...)
}

func (b *CloudHypervisor) networkRemove(ctx context.Context, r Resource) error {
	var err error
	ns, host, _, mapped, _, _ := netNames(r)
	var receipt networkReceipt
	if err := readJSON(
		filepath.Join(r.Dir, "network.json"),
		&receipt,
	); errors.Is(
		err,
		os.ErrNotExist,
	) {
		_, err = b.networkInspect(ctx, r)
		return err
	} else if err != nil {
		return err
	}
	// A cleanup marker is written only after full ownership verification. It
	// permits retry after the owned veth has already been deleted.
	marker := filepath.Join(r.Dir, "network-removing.json")
	if _, err := os.Stat(marker); os.IsNotExist(err) {
		if _, err = b.networkInspect(ctx, r); err != nil {
			return err
		}
		if err = b.Network(ctx, r, false); err != nil {
			return err
		}
		if err = writeJSON(marker, receipt); err != nil {
			return err
		}
	}
	var saved networkReceipt
	if err := readJSON(marker, &saved); err != nil {
		return err
	}
	if saved != receipt {
		return api.Fail(api.ManualRecovery, "cleanup ownership marker mismatch")
	}
	if st, err := os.Stat(
		"/run/netns/" + ns,
	); err == nil &&
		st.Sys().(*syscall.Stat_t).Ino != receipt.Inode {
		return api.Fail(api.ManualRecovery, "namespace replaced during cleanup")
	} else if err != nil &&
		!os.IsNotExist(err) {
		return err
	}
	out, err := command(ctx, "ip", "-j", "link", "show")
	if err != nil {
		return err
	}
	var existing []linkInfo
	if err = json.Unmarshal(out, &existing); err != nil {
		return err
	}
	for _, link := range existing {
		if link.Name == host && link.Alias != netAlias(r) {
			return api.Fail(
				api.ManualRecovery,
				"reused veth name during cleanup",
			)
		}
	}
	for _, elem := range []struct{ set, value string }{{"active_ifaces", `"` + host + `"`}, {"active_ips", mapped}} {
		if _, x := command(
			ctx,
			"nft",
			"get",
			"element",
			"inet",
			"sandbox_engine",
			elem.set,
			"{",
			elem.value,
			"}",
		); x == nil {
			if _, err := command(
				ctx,
				"nft",
				"delete",
				"element",
				"inet",
				"sandbox_engine",
				elem.set,
				"{",
				elem.value,
				"}",
			); err != nil {
				return err
			}
		} else {
			if err = b.checkHostPolicy(ctx); err != nil {
				return err
			}
		}
	}
	if out, x := command(
		ctx,
		"ip",
		"-j",
		"route",
		"show",
		mapped+"/32",
	); x != nil {
		return x
	} else if string(
		out,
	) != "[]\n" {
		if _, err := command(
			ctx,
			"ip",
			"route",
			"del",
			mapped+"/32",
			"dev",
			host,
		); err != nil {
			return err
		}
	}
	if out, x := command(ctx, "ip", "-j", "link", "show"); x != nil {
		return x
	} else {
		var links []linkInfo
		if err = json.Unmarshal(out, &links); err != nil {
			return err
		}
		for _, l := range links {
			if l.Name == host {
				if l.Alias != netAlias(r) {
					return api.Fail(
						api.ManualRecovery,
						"reused veth name during cleanup",
					)
				}
				if _, err = command(
					ctx,
					"ip",
					"link",
					"del",
					host,
				); err != nil {
					return err
				}
			}
		}
	}
	// The named namespace is deliberately the final kernel-resource anchor.
	if _, x := os.Stat("/run/netns/" + ns); x == nil {
		if _, err := command(ctx, "ip", "netns", "del", ns); err != nil {
			return err
		}
	}
	if err = os.Remove(
		filepath.Join(r.Dir, "network.json"),
	); err != nil &&
		!os.IsNotExist(err) {
		return err
	}
	return syncDir(r.Dir)
}
