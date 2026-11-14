package haven

import (
	"context"
	"errors"
	"fmt"
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
	result, err := loom.GuestControl(ctx, guestSocket(resource), port, newID("rpc-"), method, params)
	if err == nil {
		return result, nil
	}
	// Guest codes remain protocol data. Only the caller assigns their meaning
	// for admission; no Loom error hierarchy crosses this boundary.
	var reason error
	switch result["error_code"] {
	case "result_unknown":
		reason = ErrUnknown
	case "exec_in_progress", "exec_key_conflict", "conflict", "quiesced":
		reason = ErrConflict
	case "resource_exhausted", "exec_ledger_capacity_exhausted":
		reason = ErrExhausted
	case "not_found":
		reason = ErrManualRecovery
	case "invalid":
		if method == "exec" {
			reason = ErrInvalid
		}
	}
	if reason != nil {
		return result, fmt.Errorf("%w: %w", reason, err)
	}
	return result, err
}

func (LiveGuest) Info(ctx context.Context, resource loom.Resource) error {
	result, err := guestControl(ctx, resource, 2048, "info", map[string]any{})
	if err != nil {
		return err
	}
	if result["portal"] != "workload" || result["protocol"] != float64(1) {
		return fmt.Errorf("%w: workload portal mismatch", ErrIncompatible)
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
		return fmt.Errorf("%w: guest template preparation unconfirmed", ErrConflict)
	}
	return nil
}

func (LiveGuest) Exec(ctx context.Context, resource loom.Resource, key, command string) error {
	if key == "" {
		return fmt.Errorf("%w: warmup requires explicit exec identity", ErrInvalid)
	}
	result, err := guestControl(ctx, resource, 2048, "exec", map[string]any{
		"exec_key": key,
		"command":  command,
	})
	if err != nil {
		if errors.Is(err, ErrUnknown) || errors.Is(err, ErrConflict) || errors.Is(err, ErrExhausted) || errors.Is(err, ErrManualRecovery) || errors.Is(err, ErrInvalid) {
			return err
		}
		return fmt.Errorf("%w: guest keyed execution completion unconfirmed", ErrUnknown)
	}
	if result["result_state"] != "completed" {
		return fmt.Errorf("%w: guest keyed execution omitted durable completion", ErrUnknown)
	}
	code, ok := result["exit_code"].(float64)
	if !ok {
		return fmt.Errorf("%w: guest keyed execution omitted exit status", ErrUnknown)
	}
	if code != 0 {
		return errors.New("guest warmup failed")
	}
	return nil
}

func (LiveGuest) Connect(ctx context.Context, resource loom.Resource) (net.Conn, error) {
	return loom.DialGuest(ctx, guestSocket(resource), 2048)
}

func (client LiveGuest) Forward(ctx context.Context, resource loom.Resource, port int) (net.Conn, error) {
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("%w: invalid forward port", ErrInvalid)
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
			err = fmt.Errorf("%w: guest forward setup unconfirmed", ErrUnknown)
		}
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}
