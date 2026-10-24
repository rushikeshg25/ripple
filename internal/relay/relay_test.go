package relay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"net"
	"testing"
	"time"

	"github.com/rushikeshg25/ripple/internal/transport"
	"github.com/rushikeshg25/ripple/internal/wire"
)

func socket(t *testing.T) *net.UDPConn {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func TestRelayFaultTransfer(t *testing.T) {
	server, client, proxy := socket(t), socket(t), socket(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rc := DefaultConfig()
	rc.Loss = .05
	rc.Duplicate = .05
	rc.Corrupt = .01
	rc.Delay = 2 * time.Millisecond
	rc.Jitter = 2 * time.Millisecond
	rc.Seed = 7
	type relayResult struct {
		s   Stats
		err error
	}
	relayDone := make(chan relayResult, 1)
	go func() {
		s, err := Run(ctx, proxy, server.LocalAddr().(*net.UDPAddr), rc)
		relayDone <- relayResult{s, err}
	}()
	cfg := transport.DefaultConfig()
	cfg.MinRTO = 20 * time.Millisecond
	cfg.InitialRTO = 40 * time.Millisecond
	cfg.MaxRTO = 120 * time.Millisecond
	cfg.Linger = 360 * time.Millisecond
	data := bytes.Repeat([]byte("ripple udp\n"), 12000)
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() { _, err := transport.Receive(ctx, server, &out, cfg); done <- err }()
	sent, err := transport.Send(ctx, client, proxy.LocalAddr().(*net.UDPAddr), bytes.NewReader(data), wire.Metadata{Size: uint64(len(data)), SHA256: sha256.Sum256(data)}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	cancel()
	r := <-relayDone
	if !errors.Is(r.err, context.Canceled) {
		t.Fatal(r.err)
	}
	if !bytes.Equal(out.Bytes(), data) || sent.Retransmissions == 0 || r.s.Dropped == 0 || r.s.Duplicated == 0 {
		t.Fatal("fault transfer failed", sent, r.s)
	}
	if r.s.PeakQueued > rc.QueueLimit {
		t.Fatal("unbounded queue")
	}
}
func TestRejectInvalidRelayConfig(t *testing.T) {
	for _, p := range []float64{-1, 1.1, math.NaN(), math.Inf(1)} {
		c := DefaultConfig()
		c.Loss = p
		if c.Validate() == nil {
			t.Fatal("accepted", p)
		}
	}
}
func TestRelayQueueIsBounded(t *testing.T) {
	upstream, proxy, client := socket(t), socket(t), socket(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := DefaultConfig()
	cfg.Delay = time.Second
	cfg.QueueLimit = 2
	done := make(chan Stats, 1)
	go func() { s, _ := Run(ctx, proxy, upstream.LocalAddr().(*net.UDPAddr), cfg); done <- s }()
	meta, _ := (wire.Metadata{}).Marshal()
	b, _ := (wire.Packet{Kind: wire.Hello, Session: 1, Payload: meta}).Marshal()
	for i := 0; i < 20; i++ {
		if _, err := client.WriteToUDP(b, proxy.LocalAddr().(*net.UDPAddr)); err != nil {
			t.Fatal(err)
		}
	}
	// This check observes a bounded deadline, not exact scheduling or packet counts.
	time.Sleep(100 * time.Millisecond)
	cancel()
	s := <-done
	if s.PeakQueued != 2 || s.QueueDropped == 0 {
		t.Fatal(s)
	}
}
