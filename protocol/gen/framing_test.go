package protocol

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type shortWriter struct{ bytes.Buffer }

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return w.Buffer.Write(p)
}

func TestFramingBackToBackAndShortWrites(t *testing.T) {
	params, _ := structpb.NewStruct(
		map[string]any{
			"env":       map[string]any{"X": "y"},
			"exit_code": float64(0),
		},
	)
	frame := &Frame{
		Id:     "one",
		Method: "exec",
		Params: params,
		Data:   []byte{0, 1, 2},
		Eof:    true,
	}
	var wire shortWriter
	for range 2 {
		if err := WriteFrame(&wire, frame); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		got, err := ReadFrame(&wire)
		if err != nil || !proto.Equal(got, frame) {
			t.Fatalf("got %v, %v", got, err)
		}
	}
	if _, err := ReadFrame(&wire); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestGoldenWireMatchesRust(t *testing.T) {
	want, _ := hex.DecodeString("0000000f0a0161120470696e67220200012801")
	frame := &Frame{Id: "a", Method: "ping", Data: []byte{0, 1}, Eof: true}
	var wire bytes.Buffer
	if err := WriteFrame(&wire, frame); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire.Bytes(), want) {
		t.Fatalf("wire: %x", wire.Bytes())
	}
}

func TestInvalidAndTruncatedFrames(t *testing.T) {
	for _, wire := range [][]byte{{0, 0}, {0, 0, 0, 2, 0}, {0, 0, 0, 0}, {0, 128, 0, 1}, {0, 0, 0, 1, 255}} {
		if _, err := ReadFrame(bytes.NewReader(wire)); err == nil {
			t.Fatalf("accepted invalid wire %x", wire)
		}
	}
	if err := WriteFrame(
		io.Discard,
		&Frame{Id: "huge", Data: make([]byte, MaxFrameSize)},
	); err == nil {
		t.Fatal("accepted oversized encoded frame")
	}
}
