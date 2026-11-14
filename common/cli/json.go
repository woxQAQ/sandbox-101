// Package cli shares bounded JSON file input and output for protocol clients.
package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"syscall"
)

// ReadJSONFile reads one bounded intent from an explicit file. Stdin is never consumed.
func ReadJSONFile(path string, value any) error {
	if path == "" || path == "-" {
		return errors.New("--request-file requires a JSON file path")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("request file must be a regular file")
	}
	payload, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(payload) > 1<<20 {
		return errors.New("JSON input exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("expected exactly one JSON value")
	}
	return nil
}

func JSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
