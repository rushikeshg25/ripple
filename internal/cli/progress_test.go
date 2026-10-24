package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rushikeshg25/ripple/internal/transport"
)

func TestEventRecordingAndNoClobber(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	var terminal bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := newReporter(path, &terminal, false, cancel)
	if err != nil {
		t.Fatal(err)
	}
	e := transport.Event{Time: time.Now(), Type: "ack", Seq: 3, Stats: transport.Stats{Bytes: 123, ElapsedSeconds: 1, CongestionWindow: 4, RTTMillis: 2}}
	r.observe(e)
	if err = r.close(); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != nil || r.err != nil || !strings.Contains(terminal.String(), "cwnd=4.0") {
		t.Fatal(ctx.Err(), r.err, terminal.String())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded transport.Event
	if err = json.Unmarshal(b, &decoded); err != nil || decoded.Seq != 3 || decoded.Stats.Bytes != 123 {
		t.Fatal(err, decoded)
	}
	if _, err = newReporter(path, io.Discard, true, cancel); err == nil {
		t.Fatal("overwrote events")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("output failed") }
func TestProgressFailureCancelsTransfer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := newReporter("", failingWriter{}, false, cancel)
	if err != nil {
		t.Fatal(err)
	}
	r.observe(transport.Event{Time: time.Now()})
	if r.err == nil || ctx.Err() == nil {
		t.Fatal("write error hidden")
	}
}
func TestJSONSummary(t *testing.T) {
	var b bytes.Buffer
	if err := summary(&b, "send", transport.Stats{Bytes: 1}, true); err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(b.Bytes(), &result); err != nil || result["role"] != "send" || result["bytes"] != float64(1) {
		t.Fatal(result, err)
	}
}
