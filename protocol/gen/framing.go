package protocol

import (
	"encoding/binary"
	"errors"
	"io"

	"google.golang.org/protobuf/proto"
)

const MaxFrameSize = 8 << 20
const ChunkSize = 64 << 10

// ReadFrame reads exactly one length-delimited protobuf message. Clean EOF before
// a prefix is io.EOF; a partial prefix or payload is io.ErrUnexpectedEOF.
func ReadFrame(r io.Reader) (*Frame, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(prefix[:])
	if n == 0 || n > MaxFrameSize {
		return nil, errors.New("invalid frame size")
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	frame := new(Frame)
	if err := proto.Unmarshal(payload, frame); err != nil {
		return nil, err
	}
	return frame, nil
}

// WriteFrame does not synchronize concurrent writers; callers must serialize
// writes to prevent interleaved prefixes and payloads.
func WriteFrame(w io.Writer, f *Frame) error {
	payload, err := proto.Marshal(f)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > MaxFrameSize {
		return errors.New("invalid frame size")
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(payload)))
	for _, part := range [][]byte{prefix[:], payload} {
		for len(part) > 0 {
			n, err := w.Write(part)
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
			part = part[n:]
		}
	}
	return nil
}
