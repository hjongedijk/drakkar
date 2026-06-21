package observability

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

type Level string

const (
	LevelTrace Level = "trace"
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

// New creates a zerolog logger that writes to w. If logsDir is non-empty,
// log lines are also tee'd to logsDir/drakkar.log for the /api/logs endpoint.
func New(w io.Writer, level Level) zerolog.Logger {
	return NewWithFile(w, level, "")
}

func NewWithFile(w io.Writer, level Level, logsDir string) zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339

	// DRAKKAR_LOG_FORMAT=console enables colored human-readable output.
	// The log file always receives raw JSON for the UI log viewer.
	useConsole := strings.ToLower(os.Getenv("DRAKKAR_LOG_FORMAT")) == "console"

	// stdoutWriter is either a plain writer or a colorized console writer.
	var stdoutWriter io.Writer = w
	if useConsole {
		stdoutWriter = zerolog.ConsoleWriter{
			Out:           w,
			TimeFormat:    "01-02 15:04:05",
			FieldsExclude: []string{"service"}, // already implied by the app name
		}
	}

	out := stdoutWriter
	if logsDir != "" {
		if err := os.MkdirAll(logsDir, 0o755); err == nil {
			logPath := filepath.Join(logsDir, "drakkar.log")
			if f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
				// stdoutWriter transforms bytes (ConsoleWriter or plain).
				// f receives the original JSON bytes — MultiWriter delivers both.
				out = io.MultiWriter(stdoutWriter, f)
			}
		}
	}

	logger := zerolog.New(out).With().Timestamp().Str("service", "drakkar").Logger()
	return logger.Level(parseLevel(level))
}

func parseLevel(level Level) zerolog.Level {
	switch strings.ToLower(string(level)) {
	case "trace":
		return zerolog.TraceLevel
	case "debug":
		return zerolog.DebugLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	default:
		return zerolog.InfoLevel
	}
}
