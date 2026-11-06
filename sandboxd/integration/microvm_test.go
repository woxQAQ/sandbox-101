//go:build privileged

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	engineclient "github.com/woxQAQ/sandbox-engine/client"
	pb "github.com/woxQAQ/sandbox-protocol/gen"
	"github.com/woxQAQ/sandboxd"
	"github.com/woxQAQ/sandboxd/egress"
	"github.com/woxQAQ/sandboxd/sdk"
)

func microVMMemoryMiB(t *testing.T) int64 {
	t.Helper()
	memory := int64(512)
	if value := os.Getenv("SANDBOX_E2E_MEMORY_MIB"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 128 || parsed > 1<<24 {
			t.Fatalf(
				"SANDBOX_E2E_MEMORY_MIB must be a valid production shape (128..16777216): %q",
				value,
			)
		}
		memory = parsed
	}
	return memory
}

// Run only against an isolated, provisioned host with the exact CH/kernel/image
// contract. Missing setup fails explicitly; a skipped test is not VM validation.
func TestMicroVMLifecycle(t *testing.T) {
	endpoint, tokenPath, image, socket := os.Getenv(
		"SANDBOX_E2E_ENDPOINT",
	), os.Getenv(
		"SANDBOX_E2E_TOKEN_FILE",
	), os.Getenv(
		"SANDBOX_E2E_IMAGE_DIGEST",
	), os.Getenv(
		"SANDBOX_E2E_ENGINE_SOCKET",
	)
	if endpoint == "" || tokenPath == "" || image == "" || socket == "" {
		t.Fatal(
			"set SANDBOX_E2E_ENDPOINT, SANDBOX_E2E_TOKEN_FILE, SANDBOX_E2E_IMAGE_DIGEST and SANDBOX_E2E_ENGINE_SOCKET for an isolated privileged host",
		)
	}
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	client := sdk.New(endpoint, strings.TrimSpace(string(token)))
	engine := engineclient.New(socket)
	defer engine.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	prefix := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	shape := api.Shape{VCPUs: 1, MemoryMiB: microVMMemoryMiB(t), DiskMiB: 512}
	_, err = client.PutTemplate(
		ctx,
		prefix+"-template",
		sandboxd.Template{ID: prefix, Image: image, Shape: shape},
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		for _, id := range ids {
			if _, err := client.Change(
				cleanup,
				id,
				prefix+"-cleanup-"+id,
				sandboxd.Command{Action: "release"},
			); err != nil {
				t.Errorf("cleanup %s: %v", id, err)
			}
		}
	}()
	claimed, err := client.Claim(
		ctx,
		prefix+"-claim",
		sandboxd.Command{
			Action:     "claim",
			TemplateID: prefix,
			TTLSeconds: 600,
			Policy:     egress.Policy{Mode: "none"},
			Env: map[string]sandboxd.EnvVar{
				"CLAIM_ENV": {Value: "source"},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	source := claimed.Sandbox
	ids = append(ids, source.ID)
	run := func(id, command string) string {
		t.Helper()
		conn, err := client.Connect(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		var stdout, stderr bytes.Buffer
		code, err := conn.Exec(ctx, command, &stdout, &stderr)
		if err != nil || code != 0 {
			t.Fatalf("exec: %v code=%d stderr=%s", err, code, stderr.String())
		}
		return stdout.String()
	}
	originalIdentity := run(source.ID, "cat /etc/machine-id")
	if len(strings.TrimSpace(originalIdentity)) != 32 {
		t.Fatal("invalid guest machine-id")
	}
	if got := run(source.ID, "printf '%s' \"$CLAIM_ENV\""); got != "source" {
		t.Fatal("claim environment was not activated")
	}
	conn, err := client.Connect(ctx, source.ID)
	if err != nil {
		t.Fatal(err)
	}
	session, err := conn.Call(ctx, "session_create", map[string]any{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := session["session_id"].(string)
	if _, err = conn.Call(
		ctx,
		"exec",
		map[string]any{"session_id": sessionID, "command": "STATE=checkpoint"},
		nil,
	); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	checkpoint, err := client.Change(
		ctx,
		source.ID,
		prefix+"-checkpoint",
		sandboxd.Command{Action: "checkpoint"},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if _, err := client.DeleteCheckpoint(
			cleanup,
			checkpoint.Checkpoint.ID,
			prefix+"-delete-checkpoint",
		); err != nil {
			t.Errorf("checkpoint cleanup: %v", err)
		}
	}()
	forked, err := client.Change(
		ctx,
		source.ID,
		prefix+"-fork",
		sandboxd.Command{
			Action: "fork",
			Count:  2,
			Env:    map[string]sandboxd.EnvVar{"CLAIM_ENV": {Value: "branch"}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{originalIdentity: true}
	for _, branch := range forked.Sandboxes {
		ids = append(ids, branch.ID)
		identity := run(branch.ID, "cat /etc/machine-id")
		if seen[identity] {
			t.Fatal("fork duplicated guest identity")
		}
		seen[identity] = true
		if run(branch.ID, "printf '%s' \"$CLAIM_ENV\"") != "branch" {
			t.Fatal("branch environment was not replaced")
		}
	}
	paused, err := client.Change(
		ctx,
		source.ID,
		prefix+"-pause",
		sandboxd.Command{Action: "pause"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if paused.Sandbox.State != "paused" || paused.Sandbox.VMID != "" {
		t.Fatal("pause retained a VM")
	}
	if _, err = engine.GetVM(ctx, source.VMID); err == nil {
		t.Fatal("pause retained the source VMM")
	}
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := client.Connect(ctx, source.ID)
			if err == nil {
				_, err = stream.Call(ctx, "info", map[string]any{}, nil)
				stream.Close()
			}
			failures <- err
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	if identity := run(
		source.ID,
		"cat /etc/machine-id",
	); identity != originalIdentity {
		t.Fatal("auto-resume changed guest identity")
	}
	conn, err = client.Connect(ctx, source.ID)
	if err != nil {
		t.Fatal(err)
	}
	var restored bytes.Buffer
	if _, err = conn.Call(
		ctx,
		"exec",
		map[string]any{
			"session_id": sessionID,
			"command":    "printf '%s' \"$STATE\"",
		},
		func(f *pb.Frame) error { _, err := restored.Write(f.Data); return err },
	); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if restored.String() != "checkpoint" {
		t.Fatal("resume lost the live shell's memory state")
	}
	if _, err = client.Change(
		ctx,
		source.ID,
		prefix+"-rollback",
		sandboxd.Command{
			Action:       "rollback",
			CheckpointID: checkpoint.Checkpoint.ID,
		},
	); err != nil {
		t.Fatal(err)
	}
	if run(source.ID, "cat /etc/machine-id") != originalIdentity {
		t.Fatal("rollback changed guest identity")
	}
	if _, err = client.Change(
		ctx,
		source.ID,
		prefix+"-pause-again",
		sandboxd.Command{Action: "pause"},
	); err != nil {
		t.Fatal(err)
	}
	before, err := engine.ListVMs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Change(
		ctx,
		source.ID,
		prefix+"-zero-wake-release",
		sandboxd.Command{Action: "release"},
	); err != nil {
		t.Fatal(err)
	}
	after, err := engine.ListVMs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatal("paused release changed VM inventory")
	}
	for _, branch := range forked.Sandboxes {
		if run(branch.ID, "printf branch-survives") != "branch-survives" {
			t.Fatal("source deletion broke a branch")
		}
	}
}
