// Package logging initializes a standard slog handler for application startup.
package logging

import (
	"fmt"
	"io"
	"log/slog"
)

// New keeps logging policy at startup while callers use slog directly.
func New(output io.Writer, level, format string) (*slog.Logger, error) {
	var severity slog.Level
	switch level {
	case "debug":
		severity = slog.LevelDebug
	case "info":
		severity = slog.LevelInfo
	case "warn":
		severity = slog.LevelWarn
	case "error":
		severity = slog.LevelError
	default:
		return nil, fmt.Errorf("unsupported log level %q", level)
	}
	options := &slog.HandlerOptions{
		Level: severity,
	}
	switch format {
	case "text":
		return slog.New(slog.NewTextHandler(output, options)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(output, options)), nil
	default:
		return nil, fmt.Errorf("unsupported log format %q", format)
	}
}
