package client_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandbox-engine/client"
	"github.com/woxQAQ/sandbox-engine/server"
)

type fixture struct {
	api.Client
	request api.Request
}

func (f *fixture) Submit(_ context.Context, r api.Request) (api.Operation, error) {
	f.request = r
	return api.Operation{ID: "op-1", Key: r.Key, Request: r, Status: "running"}, nil
}
func (f *fixture) GetVM(context.Context, string) (api.VM, error) {
	return api.VM{}, api.Fail(api.NotFound, "absent")
}
func (f *fixture) ListVMs(context.Context) ([]api.VM, error) {
	return []api.VM{{ID: "vm-1", Phase: "stopped"}, {ID: "vm-2", Phase: "running"}}, nil
}
func TestUDSRoundTripAndTypedErrors(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "engine.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err = os.Chmod(socket, 0600); err != nil {
		t.Fatal(err)
	}
	f := &fixture{}
	s := &http.Server{Handler: server.Handler(f)}
	go s.Serve(l)
	defer s.Close()
	c := client.New(socket)
	defer c.Close()
	op, err := c.Submit(context.Background(), api.Request{Key: "key with /? unicode 世界", Action: "start", VMID: "vm-1"})
	if err != nil || op.ID != "op-1" || f.request.Key != op.Key {
		t.Fatalf("submit %+v %v", op, err)
	}
	all, err := c.ListVMs(context.Background())
	if err != nil || len(all) != 2 {
		t.Fatal("full list failed", err)
	}
	_, err = c.GetVM(context.Background(), "absent")
	var a *api.Error
	if !errors.As(err, &a) || a.Code != api.NotFound {
		t.Fatalf("typed error lost %v", err)
	}
}
