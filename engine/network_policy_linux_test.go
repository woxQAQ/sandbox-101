package engine

import (
	"strings"
	"testing"
)

func TestNetworkPolicyRulesDefaultDenyAndSharedBudget(t *testing.T) {
	baseline := renderNetworkPolicy(nil)
	if strings.Contains(baseline, "jump egress") || strings.Contains(baseline, "ct state") || !strings.Contains(baseline, "hook forward priority filter; policy drop") {
		t.Fatalf("baseline permits forwarding: %s", baseline)
	}
	p := directPolicy()
	p.AllowedCIDRs = []string{"0.0.0.0/0"}
	rules := renderNetworkPolicy(p)
	for _, required := range []string{
		"type ipv4_addr", "meta nfproto != ipv4 drop", "ct state invalid drop",
		"ip daddr @denied drop", "ip daddr != @allowed drop",
		"meta l4proto != { tcp, udp, icmp } drop", "ct count over 256 drop",
		"ct direction reply", "ct original ip daddr @denied drop",
		"ct original ip daddr @allowed meta l4proto { tcp, udp, icmp } jump budget",
		"hook input priority filter; policy drop", "hook output priority filter; policy drop",
	} {
		if !strings.Contains(rules, required) {
			t.Errorf("missing %q in rules:\n%s", required, rules)
		}
	}
	for _, denied := range directDeniedCIDRs {
		if !strings.Contains(rules, denied) {
			t.Errorf("missing mandatory denied prefix %s", denied)
		}
	}
	if strings.Count(rules, "ct count over") != 1 || strings.Count(rules, "jump budget") != 2 {
		t.Fatal("outbound and reply packets do not share one connection budget")
	}
	if strings.Index(rules, "ip daddr @denied drop") > strings.Index(rules, "jump budget") {
		t.Fatal("destination deny checks follow forwarding admission")
	}
}

func TestNetworkPolicyRulesHashDetectsEnforcementChanges(t *testing.T) {
	original := `{"nftables":[
	 {"metainfo":{"version":"1","json_schema_version":1}},
	 {"table":{"family":"inet","name":"engine_policy","handle":4}},
	 {"chain":{"family":"inet","table":"engine_policy","name":"forward","handle":5,"hook":"forward","prio":0,"policy":"drop"}},
	 {"rule":{"family":"inet","table":"engine_policy","chain":"forward","handle":7,"expr":[{"ct count":{"val":256,"inv":true}},{"drop":null}]}}
	]}`
	hash, err := networkRulesHash([]byte(original))
	if err != nil {
		t.Fatal(err)
	}
	for _, equivalent := range []string{
		strings.ReplaceAll(original, `"handle":7`, `"handle":90`),
		strings.ReplaceAll(original, `"version":"1"`, `"version":"2"`),
		strings.ReplaceAll(original, `"val":256,"inv":true`, `"inv":true,"val":256`),
	} {
		x, err := networkRulesHash([]byte(equivalent))
		if err != nil || x != hash {
			t.Fatalf("unstable kernel policy fingerprint: %s %v", x, err)
		}
	}
	for _, changed := range []string{
		strings.ReplaceAll(original, `"val":256`, `"val":257`),
		strings.ReplaceAll(original, `"policy":"drop"`, `"policy":"accept"`),
		strings.ReplaceAll(original, `"drop":null`, `"accept":null`),
		strings.ReplaceAll(original, `"hook":"forward"`, `"hook":"input"`),
		strings.ReplaceAll(original, `"prio":0`, `"prio":10`),
		strings.ReplaceAll(original, `"name":"engine_policy"`, `"name":"another"`),
	} {
		x, err := networkRulesHash([]byte(changed))
		if err != nil || x == hash {
			t.Fatalf("enforcement change not detected: %s %v", changed, err)
		}
	}
	for _, invalid := range []string{"{", `{}`, `{"nftables":[]}`, `{"nftables":[{"metainfo":{}}]}`, `{"nftables":[{"rule":null}]}`} {
		if _, err := networkRulesHash([]byte(invalid)); err == nil {
			t.Fatalf("accepted missing or unknown rules: %s", invalid)
		}
	}
}
