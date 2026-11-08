package egress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGuardRejectsSpecialAndMixedAnswers(t *testing.T) {
	blocked := []string{
		"0.0.0.0",
		"10.1.2.3",
		"100.64.1.1",
		"100.100.100.200",
		"127.0.0.1",
		"169.254.169.254",
		"172.16.0.1",
		"192.168.1.1",
		"192.0.0.8",
		"192.0.2.1",
		"198.18.0.1",
		"198.51.100.1",
		"203.0.113.1",
		"224.0.0.1",
		"240.0.0.1",
		"255.255.255.255",
		"168.63.129.16",
		"::",
		"::1",
		"::ffff:127.0.0.1",
		"::ffff:10.0.0.1",
		"64:ff9b::808:808",
		"64:ff9b:1::a00:1",
		"2001::1",
		"2001:db8::1",
		"2002:808:808::1",
		"3fff::1",
		"fc00::1",
		"fe80::1",
		"ff02::1",
	}
	p, err := compilePolicy(Policy{Mode: "relay"}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range blocked {
		if p.checkIP(netip.MustParseAddr(raw)) == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, raw := range []string{"8.8.8.8", "::ffff:8.8.8.8", "2606:4700:4700::1111"} {
		if p.checkIP(netip.MustParseAddr(raw)) != nil {
			t.Errorf("blocked public address %s", raw)
		}
	}
	var dials atomic.Int32
	m, b := newTestManager(
		t,
		Options{
			Resolver: resolverFunc(
				func(context.Context, string, string) ([]netip.Addr, error) {
					return []netip.Addr{
						netip.MustParseAddr("8.8.8.8"),
						netip.MustParseAddr("127.0.0.1"),
					}, nil
				},
			),
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, errors.New("unexpected")
			},
		},
		Policy{Mode: "relay"},
		nil,
	)
	resp, _ := get(t, proxyClient(t, m, b), "http://example.com/")
	if resp.StatusCode != 403 || dials.Load() != 0 {
		t.Fatal("mixed DNS answer allowed a dial")
	}
}

func TestDNSRebindingAndUpstreamTLSVerification(t *testing.T) {
	var lookups, dials atomic.Int32
	m, b := newTestManager(
		t,
		Options{
			Resolver: resolverFunc(
				func(context.Context, string, string) ([]netip.Addr, error) {
					if lookups.Add(1) == 1 {
						return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
					}
					return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
				},
			),
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, errors.New("unexpected")
			},
		},
		Policy{Mode: "relay"},
		nil,
	)
	r, _ := get(t, proxyClient(t, m, b), "http://example.com/")
	if r.StatusCode != 502 || dials.Load() != 0 {
		t.Fatalf(
			"rebinding was not denied: %d dials=%d",
			r.StatusCode,
			dials.Load(),
		)
	}
	var reached atomic.Int32
	up := httptest.NewTLSServer(
		http.HandlerFunc(
			func(http.ResponseWriter, *http.Request) { reached.Add(1) },
		),
	)
	defer up.Close()
	m2, b2 := newTestManager(
		t,
		Options{DialContext: targetDialer(up.Listener.Addr().String(), nil)},
		Policy{Mode: "relay"},
		map[string]Secret{
			"key": {
				Value:  "host-secret",
				Hosts:  []string{"example.com"},
				Header: "X-Key",
			},
		},
	)
	r, _ = get(t, proxyClient(t, m2, b2), "https://example.com/")
	if r.StatusCode != 502 || reached.Load() != 0 {
		t.Fatal("untrusted upstream received credential")
	}
}

func TestPrivateCIDRExemptionsAndSecretFormatting(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		p, err := compilePolicy(
			Policy{
				Mode: "relay",
				AllowedCIDRs: []string{
					"10.2.0.0/16",
					"169.254.0.0/16",
					"64:ff9b::/96",
				},
			},
			enabled,
		)
		if err != nil {
			t.Fatal(err)
		}
		if allowed := p.checkIP(
			netip.MustParseAddr("10.2.3.4"),
		) == nil; allowed != enabled {
			t.Fatal("private CIDR opt-in ignored")
		}
		for _, raw := range []string{"10.3.3.4", "169.254.169.254", "64:ff9b::808:808"} {
			if p.checkIP(netip.MustParseAddr(raw)) == nil {
				t.Fatalf("exempted %s", raw)
			}
		}
	}
	s := Secret{
		Value:  "do-not-persist",
		Hosts:  []string{"example.com"},
		Header: "Authorization",
	}
	data, _ := json.Marshal(s)
	for _, out := range []string{string(data), fmt.Sprintf("%v", s), fmt.Sprintf("%+v", s), fmt.Sprintf("%#v", s)} {
		if strings.Contains(out, s.Value) {
			t.Fatalf("credential serialization leaked: %s", out)
		}
	}
}

func TestRevokeCancelsInFlightNumericDial(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	m, b := newTestManager(
		t,
		Options{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				close(started)
				<-ctx.Done()
				close(stopped)
				return nil, ctx.Err()
			},
		},
		Policy{Mode: "relay"},
		nil,
	)
	client := proxyClient(t, m, b)
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := client.Get("http://example.com/")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("numeric dial did not start")
	}
	if err := m.Revoke("sandbox-1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("revoke did not cancel dial")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("request survived revoke")
	}
}
