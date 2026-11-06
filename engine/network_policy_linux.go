package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/woxQAQ/sandbox-engine/api"
)

type networkPolicyReceipt struct {
	Version   int                `json:"version"`
	Instance  string             `json:"instance"`
	Namespace networkReceipt     `json:"namespace"`
	Policy    *api.NetworkPolicy `json:"policy"`
	RulesHash string             `json:"rules_hash"`
}

var _ NetworkPolicyBackend = (*CloudHypervisor)(nil)

func namespaceNFT(ctx context.Context, ns, rules string) error {
	cmd := exec.CommandContext(ctx, "ip", "netns", "exec", ns, "nft", "-f", "-")
	cmd.Env = []string{"PATH=" + hostToolPath(), "LC_ALL=C"}
	cmd.Stdin = strings.NewReader(rules)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("namespace nft transaction: %w: %.4096s", err, out)
	}
	return nil
}

// All interpolated policy values have been validated as canonical IPv4 prefixes
// or bounded integers. No hostname, instance ID or caller text enters nft source.
func renderNetworkPolicy(p *api.NetworkPolicy) string {
	var rules strings.Builder
	rules.WriteString("table inet engine_policy {\n")
	if p != nil {
		fmt.Fprintf(
			&rules,
			" set allowed { type ipv4_addr; flags interval; auto-merge; elements = { %s }; }\n",
			strings.Join(p.AllowedCIDRs, ", "),
		)
		fmt.Fprintf(
			&rules,
			" set denied { type ipv4_addr; flags interval; elements = { %s }; }\n",
			strings.Join(directDeniedCIDRs, ", "),
		)
		// The count expression is shared by all guest destinations/protocols in
		// this namespace. It sees established packets too, so replacing a policy
		// cannot bypass its budget using already-existing conntrack entries.
		fmt.Fprintf(&rules, ` chain egress {
  ip daddr @denied drop
  ip daddr != @allowed drop
  meta l4proto != { tcp, udp, icmp } drop
  jump budget
 }
 chain budget {
  ct count over %d drop
  ct state { new, established, related } accept
 }
`, p.MaxConnections)
	}
	rules.WriteString(
		" chain forward { type filter hook forward priority filter; policy drop;\n",
	)
	if p != nil {
		rules.WriteString(`  meta nfproto != ipv4 drop
  ct state invalid drop
  iifname "tap0" oifname "uplink" ip saddr 192.168.127.2 jump egress
  iifname "uplink" oifname "tap0" ip daddr 192.168.127.2 ct direction reply ct state { established, related } ct original ip saddr 192.168.127.2 ct original ip daddr @denied drop
  iifname "uplink" oifname "tap0" ip daddr 192.168.127.2 ct direction reply ct state { established, related } ct original ip saddr 192.168.127.2 ct original ip daddr @allowed meta l4proto { tcp, udp, icmp } jump budget
`)
	}
	rules.WriteString(` }
 chain output { type filter hook output priority filter; policy drop; oifname "lo" accept; }
 chain input { type filter hook input priority filter; policy drop; iifname "lo" accept; }
}
`)
	return rules.String()
}

// Hash the complete stateless namespace ruleset, not just an identifying comment
// or the allowlist. Changes to NAT, verdicts, chain hooks, other tables, limits,
// etc. invalidate verification. Only nft allocation handles and tool metainfo
// are excluded. No packet counters or dynamic sets are installed by this policy.
func networkRulesHash(data []byte) (string, error) {
	var ruleset struct {
		NFTables []map[string]json.RawMessage `json:"nftables"`
	}
	if err := json.Unmarshal(data, &ruleset); err != nil {
		return "", err
	}
	var entries []map[string]json.RawMessage
	for _, entry := range ruleset.NFTables {
		if _, meta := entry["metainfo"]; meta {
			if len(entry) != 1 {
				return "", api.Fail(api.Unknown, "invalid nft metainfo entry")
			}
			continue
		}
		if len(entry) != 1 {
			return "", api.Fail(api.Unknown, "invalid nft ruleset entry")
		}
		for kind, raw := range entry {
			var object map[string]json.RawMessage
			if err := json.Unmarshal(
				raw,
				&object,
			); err != nil ||
				object == nil {
				return "", api.Fail(api.Unknown, "invalid nft %s object", kind)
			}
			delete(object, "handle")
			entry[kind] = encode(object)
		}
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return "", api.Fail(api.Unknown, "namespace nft ruleset is empty")
	}
	// Decode once more so nested JSON object key order is canonical as well.
	var canonical any
	if err := json.Unmarshal(encode(entries), &canonical); err != nil {
		return "", err
	}
	sum := sha256.Sum256(encode(canonical))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (b *CloudHypervisor) networkPolicyHash(
	ctx context.Context,
	r Resource,
) (string, error) {
	ns, _, _, _, _, _ := netNames(r)
	out, err := command(
		ctx,
		"ip",
		"netns",
		"exec",
		ns,
		"nft",
		"-j",
		"-s",
		"list",
		"ruleset",
	)
	if err != nil {
		return "", err
	}
	return networkRulesHash(out)
}

func (b *CloudHypervisor) InstallNetworkPolicy(
	ctx context.Context,
	r Resource,
	p *api.NetworkPolicy,
) error {
	if r.Instance == "" {
		return api.Fail(
			api.Invalid,
			"direct policy requires a running instance identity",
		)
	}
	if err := validateNetworkPolicy(p); err != nil {
		return err
	}
	active, err := b.networkInspect(ctx, r)
	if err != nil {
		return err
	}
	if active {
		return api.Fail(
			api.Conflict,
			"all owned interfaces must be down before policy replacement",
		)
	}
	if _, err = os.Stat(
		filepath.Join(r.Dir, "network-removing.json"),
	); err == nil {
		return api.Fail(api.Conflict, "network cleanup in progress")
	} else if !os.IsNotExist(
		err,
	) {
		return err
	}
	var nsReceipt networkReceipt
	if err = readJSON(
		filepath.Join(r.Dir, "network.json"),
		&nsReceipt,
	); err != nil {
		return err
	}
	// A new key with an identical policy is a verified no-op. Stale receipts or
	// kernel rules are repaired only here, under the off-only precondition.
	if b.VerifyNetworkPolicy(ctx, r, p) == nil {
		return nil
	}
	ns, _, _, _, _, _ := netNames(r)
	// add is deliberately non-exclusive. Add/delete/recreate is one atomic nft
	// transaction, works with an absent table, and never flushes foreign tables.
	rules := "add table inet engine_policy\ndelete table inet engine_policy\n" + renderNetworkPolicy(
		p,
	)
	if err = namespaceNFT(ctx, ns, rules); err != nil {
		return err
	}
	hash, err := b.networkPolicyHash(ctx, r)
	if err != nil {
		return err
	}
	return writeJSON(
		filepath.Join(r.Dir, "network-policy.json"),
		networkPolicyReceipt{
			Version:   1,
			Instance:  r.Instance,
			Namespace: nsReceipt,
			Policy:    p,
			RulesHash: hash,
		},
	)
}

func (b *CloudHypervisor) VerifyNetworkPolicy(
	ctx context.Context,
	r Resource,
	p *api.NetworkPolicy,
) error {
	if err := validateNetworkPolicy(p); err != nil {
		return err
	}
	if _, err := b.networkInspect(ctx, r); err != nil {
		return err
	}
	var receipt networkPolicyReceipt
	if err := readJSON(
		filepath.Join(r.Dir, "network-policy.json"),
		&receipt,
	); err != nil {
		return api.Fail(
			api.Unknown,
			"network policy receipt unavailable: %v",
			err,
		)
	}
	var nsReceipt networkReceipt
	if err := readJSON(
		filepath.Join(r.Dir, "network.json"),
		&nsReceipt,
	); err != nil {
		return err
	}
	if receipt.Version != 1 || r.Instance == "" ||
		receipt.Instance != r.Instance ||
		receipt.Namespace != nsReceipt ||
		!reflect.DeepEqual(receipt.Policy, p) {
		return api.Fail(
			api.Conflict,
			"network policy receipt does not match current namespace, instance or persisted policy",
		)
	}
	hash, err := b.networkPolicyHash(ctx, r)
	if err != nil {
		return api.Fail(
			api.Unknown,
			"network policy kernel verification failed: %v",
			err,
		)
	}
	if receipt.RulesHash != hash {
		return api.Fail(
			api.Unknown,
			"namespace rules differ from the installed network policy",
		)
	}
	return nil
}
