package observability

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/rs/zerolog"
)

// ZerologSlogHandler routes slog records through a zerolog.Logger so that
// packages using slog (nntp, library, api) share the same colored output
// format as packages using zerolog directly.
type ZerologSlogHandler struct {
	logger zerolog.Logger
}

// NewSlogHandler returns an slog.Handler backed by logger.  Install it with:
//
//	slog.SetDefault(slog.New(observability.NewSlogHandler(logger)))
func NewSlogHandler(logger zerolog.Logger) slog.Handler {
	return &ZerologSlogHandler{logger: logger}
}

func (h *ZerologSlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	var zl zerolog.Level
	switch {
	case level >= slog.LevelError:
		zl = zerolog.ErrorLevel
	case level >= slog.LevelWarn:
		zl = zerolog.WarnLevel
	case level >= slog.LevelInfo:
		zl = zerolog.InfoLevel
	default:
		zl = zerolog.DebugLevel
	}
	return h.logger.GetLevel() <= zl
}

func (h *ZerologSlogHandler) Handle(_ context.Context, r slog.Record) error {
	var ev *zerolog.Event
	switch {
	case r.Level >= slog.LevelError:
		ev = h.logger.Error()
	case r.Level >= slog.LevelWarn:
		ev = h.logger.Warn()
	case r.Level >= slog.LevelInfo:
		ev = h.logger.Info()
	default:
		ev = h.logger.Debug()
	}
	if ev == nil {
		return nil
	}
	r.Attrs(func(a slog.Attr) bool {
		switch v := a.Value.Any().(type) {
		case error:
			ev = ev.AnErr(a.Key, v)
		case int, int64:
			ev = ev.Int64(a.Key, a.Value.Int64())
		case float64:
			ev = ev.Float64(a.Key, v)
		case bool:
			ev = ev.Bool(a.Key, v)
		default:
			ev = ev.Str(a.Key, fmt.Sprintf("%v", a.Value.Any()))
		}
		return true
	})
	ev.Msg(r.Message)
	return nil
}

func (h *ZerologSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	ctx := h.logger.With()
	for _, a := range attrs {
		ctx = ctx.Str(a.Key, fmt.Sprintf("%v", a.Value.Any()))
	}
	return &ZerologSlogHandler{logger: ctx.Logger()}
}

func (h *ZerologSlogHandler) WithGroup(name string) slog.Handler {
	return &ZerologSlogHandler{logger: h.logger.With().Str("group", name).Logger()}
}
