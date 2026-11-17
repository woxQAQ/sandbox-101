package guest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"path/filepath"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	protocol "github.com/woxQAQ/sandbox-protocol/gen"
)

// Client uses the unchanged Admin/Workload portals on the exact candidate's
// vsock socket. It does not infer readiness or publish sandbox state.
type Client struct{}

func requestID(prefix string) string {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(nonce[:])
}

// Socket returns the endpoint for an already verified resource; it proves no ownership.
func Socket(resource loom.Resource) string {
	return filepath.Join(resource.RunDir, "io", "vsock")
}

func guestControl(ctx context.Context, resource loom.Resource, port int, method string, params map[string]any) (map[string]any, error) {
	result, err := loom.GuestControl(ctx, Socket(resource), port, requestID("rpc-"), method, params)
	if err == nil {
		return result, nil
	}
	// Guest codes remain protocol data. Only the caller assigns their meaning
	// for admission; no Loom error hierarchy crosses this boundary.
	var reason error
	switch result["error_code"] {
	case "result_unknown":
		reason = errs.ErrUnknown
	case "exec_in_progress", "exec_key_conflict", "conflict", "quiesced":
		reason = errs.ErrConflict
	case "resource_exhausted", "exec_ledger_capacity_exhausted":
		reason = errs.ErrExhausted
	case "not_found":
		reason = errs.ErrManualRecovery
	case "invalid":
		if method == "exec" {
			reason = errs.ErrInvalid
		}
	}
	if reason != nil {
		return result, fmt.Errorf("%w: %w", reason, err)
	}
	return result, err
}

func (Client) Info(ctx context.Context, resource loom.Resource) error {
	result, err := guestControl(ctx, resource, 2048, "info", map[string]any{})
	if err != nil {
		return err
	}
	if result["portal"] != "workload" || result["protocol"] != float64(1) {
		return fmt.Errorf("%w: workload portal mismatch", errs.ErrIncompatible)
	}
	return nil
}

func (Client) Activate(ctx context.Context, resource loom.Resource, activation Activation) error {
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

func (Client) PrepareTemplate(ctx context.Context, resource loom.Resource) error {
	result, err := guestControl(ctx, resource, 2046, "prepare_template", map[string]any{})
	if err != nil {
		return err
	}
	if result["prepared"] != true || result["quiescent"] != true {
		return fmt.Errorf("%w: guest template preparation unconfirmed", errs.ErrConflict)
	}
	return nil
}

func (Client) Exec(ctx context.Context, resource loom.Resource, key, command string) error {
	if key == "" {
		return fmt.Errorf("%w: warmup requires explicit exec identity", errs.ErrInvalid)
	}
	result, err := guestControl(ctx, resource, 2048, "exec", map[string]any{
		"exec_key": key,
		"command":  command,
	})
	if err != nil {
		if errors.Is(err, errs.ErrUnknown) || errors.Is(err, errs.ErrConflict) || errors.Is(err, errs.ErrExhausted) || errors.Is(err, errs.ErrManualRecovery) || errors.Is(err, errs.ErrInvalid) {
			return err
		}
		return fmt.Errorf("%w: guest keyed execution completion unconfirmed", errs.ErrUnknown)
	}
	if result["result_state"] != "completed" {
		return fmt.Errorf("%w: guest keyed execution omitted durable completion", errs.ErrUnknown)
	}
	code, ok := result["exit_code"].(float64)
	if !ok {
		return fmt.Errorf("%w: guest keyed execution omitted exit status", errs.ErrUnknown)
	}
	if code != 0 {
		return errors.New("guest warmup failed")
	}
	return nil
}

func (Client) Connect(ctx context.Context, resource loom.Resource) (net.Conn, error) {
	return loom.DialGuest(ctx, Socket(resource), 2048)
}

func (client Client) Forward(ctx context.Context, resource loom.Resource, port int) (net.Conn, error) {
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("%w: invalid forward port", errs.ErrInvalid)
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
	id := requestID("forward-")
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
			err = fmt.Errorf("%w: guest forward setup unconfirmed", errs.ErrUnknown)
		}
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}
