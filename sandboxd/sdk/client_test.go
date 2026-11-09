package sdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	pb "github.com/woxQAQ/sandbox-protocol/gen"
	"github.com/woxQAQ/sandboxd/agent"
)

// pipeConnection returns a Connection whose peer is a scriptable net.Pipe end.
func pipeConnection(t *testing.T) (*Connection, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return &Connection{Conn: client}, server
}

// respond writes a terminal success frame echoing params under the given id.
func respond(t *testing.T, c net.Conn, id string, params map[string]any) {
	t.Helper()
	f, err := agent.Request(id, "", params)
	if err != nil {
		t.Errorf("build response: %v", err)
		return
	}
	if err := agent.Write(c, f); err != nil {
		t.Errorf("write response: %v", err)
	}
}

// expectNoFrame asserts the peer stays silent for a bounded window. The window
// bounds a negative assertion; synchronization never relies on it.
func expectNoFrame(t *testing.T, c net.Conn, window time.Duration) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(window)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, err := agent.Read(c)
	if err := c.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear read deadline: %v", err)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("peer sent an unexpected frame (err=%v)", err)
	}
}

func TestConnectionWaitingCallHonorsContext(t *testing.T) {
	conn, server := pipeConnection(t)

	// The peer accepts the first request, then withholds its response so the
	// first RPC occupies the connection.
	firstRequest := make(chan *pb.Frame, 1)
	releaseFirst := make(chan struct{})
	firstPeerDone := make(chan struct{})
	go func() {
		defer close(firstPeerDone)
		f, err := agent.Read(server)
		if err != nil {
			t.Errorf("read first request: %v", err)
			return
		}
		firstRequest <- f
		<-releaseFirst
		respond(t, server, f.Id, map[string]any{"first": true})
	}()

	firstResult := make(chan error, 1)
	go func() {
		_, err := conn.Call(context.Background(), "info", nil, nil)
		firstResult <- err
	}()

	select {
	case f := <-firstRequest:
		if f.Id != "1" || f.Method != "info" {
			t.Fatalf("unexpected first request: %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first request never reached the peer")
	}

	// A waiting Call with an already-canceled context must return
	// context.Canceled while the first RPC is still in flight.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	canceledResult := make(chan error, 1)
	go func() {
		_, err := conn.Call(canceled, "info", nil, nil)
		canceledResult <- err
	}()
	select {
	case err := <-canceledResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled waiting call did not return")
	}
	expectNoFrame(t, server, 200*time.Millisecond)

	// A waiting Call with a short deadline must return DeadlineExceeded.
	deadlineResult := make(chan error, 1)
	deadlineCtx, deadlineCancel := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	defer deadlineCancel()
	go func() {
		_, err := conn.Call(deadlineCtx, "info", nil, nil)
		deadlineResult <- err
	}()
	select {
	case err := <-deadlineResult:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deadline-bound waiting call did not return")
	}
	expectNoFrame(t, server, 200*time.Millisecond)

	// The first RPC still completes normally once the peer responds.
	close(releaseFirst)
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("first call failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first call did not complete")
	}
	<-firstPeerDone

	// The connection stays reusable, and neither canceled waiter consumed an
	// RPC id: the next admitted call gets id 2.
	thirdResult := make(chan error, 1)
	go func() {
		params, err := conn.Call(context.Background(), "info", nil, nil)
		if err == nil && params["third"] != true {
			err = errors.New("third response params missing")
		}
		thirdResult <- err
	}()
	readThird := make(chan *pb.Frame, 1)
	go func() {
		f, err := agent.Read(server)
		if err != nil {
			t.Errorf("read third request: %v", err)
			return
		}
		readThird <- f
		respond(t, server, f.Id, map[string]any{"third": true})
	}()
	select {
	case f := <-readThird:
		if f.Id != "2" {
			t.Fatalf(
				"canceled waiters consumed RPC ids; third request id=%q",
				f.Id,
			)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("third request never reached the peer")
	}
	select {
	case err := <-thirdResult:
		if err != nil {
			t.Fatalf("third call failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("third call did not complete")
	}
}

// TestConnectionCanceledBeforeSend covers a context canceled before the
// admission token is even contended: the call must fail without sending,
// without consuming an id, and without poisoning the connection.
func TestConnectionCanceledBeforeSend(t *testing.T) {
	conn, server := pipeConnection(t)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := conn.Call(canceled, "info", nil, nil); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	expectNoFrame(t, server, 200*time.Millisecond)

	// A healthy call still works and gets id 1.
	result := make(chan error, 1)
	go func() {
		_, err := conn.Call(context.Background(), "info", nil, nil)
		result <- err
	}()
	readDone := make(chan *pb.Frame, 1)
	go func() {
		f, err := agent.Read(server)
		if err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		readDone <- f
		respond(t, server, f.Id, nil)
	}()
	select {
	case f := <-readDone:
		if f.Id != "1" {
			t.Fatalf(
				"pre-canceled call consumed an id; next request id=%q",
				f.Id,
			)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request never reached the peer")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("call after pre-canceled call failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call after pre-canceled call did not complete")
	}
}

// TestConnectionActiveCallCancellationClosesTransport keeps the established
// semantic: canceling an RPC whose request was already sent closes the
// transport to interrupt it, and the connection is not reusable afterwards.
func TestConnectionActiveCallCancellationClosesTransport(t *testing.T) {
	conn, server := pipeConnection(t)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := conn.Call(ctx, "watch", nil, nil)
		result <- err
	}()

	readDone := make(chan *pb.Frame, 1)
	go func() {
		f, err := agent.Read(server)
		if err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		readDone <- f
	}()
	select {
	case f := <-readDone:
		if f.Id != "1" || f.Method != "watch" {
			t.Fatalf("unexpected request: %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request never reached the peer")
	}

	// Cancel mid-flight: the call must be interrupted, and the peer must
	// observe the transport close.
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("canceled in-flight call succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight call ignored cancellation")
	}
	peerClosed := make(chan error, 1)
	go func() {
		_, err := agent.Read(server)
		peerClosed <- err
	}()
	select {
	case err := <-peerClosed:
		if err == nil {
			t.Fatal("peer did not observe transport close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("peer did not observe transport close")
	}

	// The closed connection must not be reused: a fresh call fails before
	// sending anything.
	if _, err := conn.Call(
		context.Background(),
		"info",
		nil,
		nil,
	); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("expected reuse refusal, got %v", err)
	}
}

// TestConnectionConcurrentCallsStaySerialized issues a bounded set of
// concurrent calls and verifies each receives its own response with a
// distinct sequential id.
func TestConnectionConcurrentCallsStaySerialized(t *testing.T) {
	conn, server := pipeConnection(t)
	const callers = 8

	peerDone := make(chan []string, 1)
	go func() {
		var ids []string
		for range callers {
			f, err := agent.Read(server)
			if err != nil {
				t.Errorf("read request: %v", err)
				break
			}
			ids = append(ids, f.Id)
			respond(t, server, f.Id, map[string]any{
				"echo": f.Params.AsMap()["marker"],
			})
		}
		peerDone <- ids
	}()

	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			marker := float64(i)
			params, err := conn.Call(
				context.Background(),
				"info",
				map[string]any{"marker": marker},
				nil,
			)
			if err != nil {
				errs <- fmt.Errorf("caller %d: %w", i, err)
				return
			}
			if params["echo"] != marker {
				errs <- fmt.Errorf(
					"caller %d: mismatched response %v",
					i,
					params,
				)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	ids := <-peerDone
	if len(ids) != callers {
		t.Fatalf("peer saw %d requests, want %d", len(ids), callers)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate RPC id %q", id)
		}
		seen[id] = true
		n, err := strconv.Atoi(id)
		if err != nil || n < 1 || n > callers {
			t.Fatalf("unexpected RPC id %q", id)
		}
	}
}

// gatedCloseConn blocks Close until released, exposing the window in which a
// cancellation callback has started closing the transport but not finished.
type gatedCloseConn struct {
	net.Conn

	started   chan struct{}
	release   chan struct{}
	closeOnce sync.Once
}

func (c *gatedCloseConn) Close() error {
	c.closeOnce.Do(func() { close(c.started) })
	<-c.release
	return c.Conn.Close()
}

// TestConnectionCancellationCallbackCannotOutliveAdmission races a terminal
// response against context cancellation: while the old close callback is
// still running, the next call must not be admitted; once the callback
// finished, the closed transport must not be reused.
func TestConnectionCancellationCallbackCannotOutliveAdmission(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	gated := &gatedCloseConn{
		Conn:    client,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	conn := &Connection{Conn: gated}

	ctx, cancel := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		params, err := conn.Call(ctx, "info", nil, nil)
		if err == nil && params["first"] != true {
			err = errors.New("first response params missing")
		}
		firstResult <- err
	}()

	readDone := make(chan *pb.Frame, 1)
	go func() {
		f, err := agent.Read(server)
		if err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		readDone <- f
	}()
	var id string
	select {
	case f := <-readDone:
		id = f.Id
	case <-time.After(5 * time.Second):
		t.Fatal("request never reached the peer")
	}

	// Cancel, then wait until the close callback is inside the gated Close.
	cancel()
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation callback did not start closing the transport")
	}

	// The terminal response still lands for the in-flight call, but its
	// exit path now waits for the callback behind the gated Close.
	respond(t, server, id, map[string]any{"first": true})

	secondResult := make(chan error, 1)
	go func() {
		_, err := conn.Call(context.Background(), "info", nil, nil)
		secondResult <- err
	}()

	// While the old callback is unfinished, the next call must neither
	// return nor send.
	select {
	case err := <-secondResult:
		t.Fatalf("next call returned before callback finished: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	expectNoFrame(t, server, 200*time.Millisecond)

	close(gated.release)
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("first call lost its terminal response: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first call did not complete")
	}
	select {
	case err := <-secondResult:
		if err == nil {
			t.Fatal("call on callback-closed connection succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call on callback-closed connection did not return")
	}

	// The transport really is closed underneath.
	peerClosed := make(chan error, 1)
	go func() {
		_, err := agent.Read(server)
		peerClosed <- err
	}()
	select {
	case err := <-peerClosed:
		if err == nil {
			t.Fatal("peer did not observe transport close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("peer did not observe transport close")
	}
}
