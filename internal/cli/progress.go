package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rushikeshg25/ripple/internal/transport"
)

type reporter struct {
	file        *os.File
	encoder     *json.Encoder
	terminal    io.Writer
	quiet       bool
	cancel      context.CancelFunc
	err         error
	last        time.Time
	lastBytes   uint64
	lastElapsed float64
}

func newReporter(path string, terminal io.Writer, quiet bool, cancel context.CancelFunc) (*reporter, error) {
	r := &reporter{terminal: terminal, quiet: quiet, cancel: cancel}
	if path != "" {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, fmt.Errorf("create events file: %w", err)
		}
		r.file = f
		r.encoder = json.NewEncoder(f)
	}
	return r, nil
}
func (r *reporter) observe(e transport.Event) {
	if r.err != nil {
		return
	}
	if r.encoder != nil {
		r.err = r.encoder.Encode(e)
	}
	terminalEvent := e.Type == "complete" || e.Type == "published" || e.Type == "error"
	if r.err == nil && !r.quiet && (r.last.IsZero() || e.Time.Sub(r.last) >= 250*time.Millisecond || terminalEvent) {
		rate := float64(0)
		delta := e.Stats.ElapsedSeconds - r.lastElapsed
		if delta > 0 && e.Stats.Bytes >= r.lastBytes {
			rate = float64(e.Stats.Bytes-r.lastBytes) / delta / 1024
		}
		_, r.err = fmt.Fprintf(r.terminal, "%-10s bytes=%d rate=%.1fKiB/s cwnd=%.1f flight=%d rtt=%.1fms rto=%.1fms retries=%d\n", e.Type, e.Stats.Bytes, rate, e.Stats.CongestionWindow, e.Stats.InFlight, e.Stats.RTTMillis, e.Stats.RTOMillis, e.Stats.Retransmissions)
		r.last = e.Time
		r.lastBytes = e.Stats.Bytes
		r.lastElapsed = e.Stats.ElapsedSeconds
	}
	if r.err != nil {
		r.cancel()
	}
}
func (r *reporter) close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
func summary(out io.Writer, mode string, s transport.Stats, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(out).Encode(struct {
			Role string `json:"role"`
			transport.Stats
		}{mode, s})
	}
	_, err := fmt.Fprintf(out, "%s complete: %d bytes in %.3fs, sha256=%s, retries=%d\n", mode, s.Bytes, s.ElapsedSeconds, s.SHA256, s.Retransmissions)
	return err
}
