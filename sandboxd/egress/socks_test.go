package egress

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSOCKS5TCPAndGuards(t *testing.T) {
	up := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "socks upstream") },
		),
	)
	defer up.Close()
	var dials atomic.Int32
	_, b := newTestManager(
		t,
		Options{DialContext: targetDialer(up.Listener.Addr().String(), &dials)},
		Policy{Mode: "relay", AllowedHosts: []string{"example.com"}},
		nil,
	)
	conn, status := socksConnect(t, b.SOCKSPath, "example.com", 1)
	if status != 0 {
		t.Fatalf("SOCKS status: %d", status)
	}
	_, _ = io.WriteString(
		conn,
		"GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n",
	)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if body := readResponse(t, resp, err); body != "socks upstream" {
		t.Fatal(body)
	}
	_ = conn.Close()
	for _, tc := range []struct {
		host    string
		command byte
	}{{"127.0.0.1", 1}, {"attacker.example.com", 1}, {"example.com", 3}, {"example.com", 2}} {
		conn, status := socksConnect(t, b.SOCKSPath, tc.host, tc.command)
		_ = conn.Close()
		if status == 0 {
			t.Errorf("SOCKS accepted %+v", tc)
		}
	}
	if dials.Load() != 1 {
		t.Fatal("guarded SOCKS made an unexpected dial")
	}
	_, b2 := newTestManager(
		t,
		Options{},
		Policy{Mode: "relay"},
		map[string]Secret{
			"key": {
				Value:  "host-secret",
				Hosts:  []string{"example.com"},
				Header: "Authorization",
			},
		},
	)
	conn, status = socksConnect(t, b2.SOCKSPath, "example.com", 1)
	_ = conn.Close()
	if status == 0 {
		t.Fatal("raw SOCKS bypassed credential checks")
	}
}

func TestSOCKS5HalfClose(t *testing.T) {
	up, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := up.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		data, err := io.ReadAll(conn)
		if err == nil {
			_, _ = conn.Write(append([]byte("echo:"), data...))
		}
	}()
	_, b := newTestManager(
		t,
		Options{DialContext: targetDialer(up.Addr().String(), nil)},
		Policy{Mode: "relay"},
		nil,
	)
	conn, status := socksConnect(t, b.SOCKSPath, "example.com", 1)
	defer conn.Close()
	if status != 0 {
		t.Fatal(status)
	}
	_, _ = io.WriteString(conn, "payload")
	if err := conn.(*net.UnixConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(conn)
	if err != nil || string(data) != "echo:payload" {
		t.Fatalf("half-close lost reply: %q %v", data, err)
	}
	<-done
}
