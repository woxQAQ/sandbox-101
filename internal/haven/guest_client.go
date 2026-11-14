package haven

import (
	"context"
	"errors"
	"net"
	"path/filepath"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/woxQAQ/haven/loom"
	protocol "github.com/woxQAQ/sandbox-protocol/gen"
)

// LiveGuest uses the unchanged Admin/Workload portals on the exact candidate's
// vsock socket. It does not infer readiness or publish sandbox state.
type LiveGuest struct{}

func guestSocket(resource loom.Resource) string {
	return filepath.Join(resource.RunDir, "io", "vsock")
}

func guestControl(ctx context.Context, resource loom.Resource, port int, method string, params map[string]any) (map[string]any, error) {
	return loom.GuestControl(ctx, guestSocket(resource), port, newID("rpc-"), method, params)
}

func (LiveGuest) Info(ctx context.Context, resource loom.Resource) error {
	result, err := guestControl(ctx, resource, 2048, "info", map[string]any{})
	if err != nil {
		return err
	}
	if result["portal"] != "workload" || result["protocol"] != float64(1) {
		return loom.Fail(loom.Incompatible, "workload portal mismatch")
	}
	return nil
}

func (LiveGuest) Activate(ctx context.Context, resource loom.Resource, activation Activation) error {
	env := map[string]any{}
	for key, value := range activation.Env {
		env[key] = value
	}
	_, err := guestControl(ctx, resource, 2046, "activate", map[string]any{
		"sandbox_id":     activation.SandboxID,
		"hostname":       activation.Hostname,
		"reset_identity": activation.ResetIdentity,
		"env":            env,
		"network":        activation.Network,
		"ca_pem":         activation.CAPEM,
	})
	return err
}

func (LiveGuest) PrepareTemplate(ctx context.Context, resource loom.Resource) error {
	result, err := guestControl(ctx, resource, 2046, "prepare_template", map[string]any{})
	if err != nil {
		return err
	}
	if result["prepared"] != true || result["quiescent"] != true {
		return loom.Fail(loom.Conflict, "guest template preparation unconfirmed")
	}
	return nil
}

func (LiveGuest) Exec(ctx context.Context, resource loom.Resource, key, command string) error {
	if key == "" {
		return loom.Fail(loom.Invalid, "warmup requires explicit exec identity")
	}
	result, err := guestControl(ctx, resource, 2048, "exec", map[string]any{
		"exec_key": key,
		"command":  command,
	})
	if err != nil {
		var classified *loom.Error
		if errors.As(err, &classified) && classified.Code != loom.Internal {
			return err
		}
		return loom.Fail(loom.Unknown, "guest keyed execution completion unconfirmed")
	}
	if result["result_state"] != "completed" {
		return loom.Fail(loom.Unknown, "guest keyed execution omitted durable completion")
	}
	code, ok := result["exit_code"].(float64)
	if !ok {
		return loom.Fail(loom.Unknown, "guest keyed execution omitted exit status")
	}
	if code != 0 {
		return loom.Fail(loom.Internal, "guest warmup failed")
	}
	return nil
}

func (LiveGuest) Connect(ctx context.Context, resource loom.Resource) (net.Conn, error) {
	return loom.DialGuest(ctx, guestSocket(resource), 2048)
}

func (client LiveGuest) Forward(ctx context.Context, resource loom.Resource, port int) (net.Conn, error) {
	if port < 1 || port > 65535 {
		return nil, loom.Fail(loom.Invalid, "invalid forward port")
	}
	conn, err := client.Connect(ctx, resource)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() {
		_ = conn.Close()
	})
	defer stop()
	params, _ := structpb.NewStruct(map[string]any{
		"port": port,
	})
	id := newID("forward-")
	err = protocol.WriteFrame(conn, &protocol.Frame{
		Id:     id,
		Method: "port_forward",
		Params: params,
		Eof:    true,
	})
	if err == nil {
		var reply *protocol.Frame
		reply, err = protocol.ReadFrame(conn)
		if err == nil && (reply.Id != id || reply.Error != "" || reply.Params.AsMap()["ready"] != true) {
			err = loom.Fail(loom.Unknown, "guest forward setup unconfirmed")
		}
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}
