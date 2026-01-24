package repo

type DittoRepo interface {
	Mirror() error
}

// Logger is a simple logging interface
// It mimics the standard library log/slog methods.
type Logger interface {
	Debug(msg string, args ...any)
	Error(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}
