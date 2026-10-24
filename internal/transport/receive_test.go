package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/rushikeshg25/ripple/internal/wire"
)

func TestLoopbackTransfers(t *testing.T) {
	for _, size := range []int{0, 1, wire.MaxPayload, wire.MaxPayload*100 + 13} {
		t.Run(fmtSize(size), func(t *testing.T) {
			server, client := udpSocket(t), udpSocket(t)
			data := bytes.Repeat([]byte{byte(size)}, size)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cfg := testConfig()
			cfg.Window = 7
			published := false
			var out bytes.Buffer
			cfg.Commit = func() error {
				if !bytes.Equal(out.Bytes(), data) {
					return errors.New("wrong bytes at publish")
				}
				published = true
				return nil
			}
			done := make(chan error, 1)
			go func() { _, err := Receive(ctx, server, &out, cfg); done <- err }()
			stats, err := Send(ctx, client, server.LocalAddr().(*net.UDPAddr), bytes.NewReader(data), wire.Metadata{Size: uint64(size), SHA256: sha256.Sum256(data)}, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			if !published || !bytes.Equal(data, out.Bytes()) || stats.Bytes != uint64(size) || stats.PeakInFlight > 7 {
				t.Fatal("transfer contract", stats)
			}
		})
	}
}
func fmtSize(n int) string {
	if n == 0 {
		return "empty"
	}
	if n == 1 {
		return "one-byte"
	}
	if n == wire.MaxPayload {
		return "one-packet"
	}
	return "multi-window"
}
func TestReceiverCancellationAndIdle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Receive(ctx, udpSocket(t), &bytes.Buffer{}, testConfig()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.IdleTimeout = 100 * time.Millisecond
	if _, err := Receive(context.Background(), udpSocket(t), &bytes.Buffer{}, cfg); err == nil {
		t.Fatal("idle receiver never failed")
	}
}
