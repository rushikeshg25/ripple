package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rushikeshg25/ripple/internal/wire"
)

// An explicit fault schedule makes coverage independent of random seeds and timing.
func TestRecoveryAcrossEveryProtocolPhase(t *testing.T) {
	server, client, proxy := udpSocket(t), udpSocket(t), udpSocket(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	proxyDone := make(chan error, 1)
	dropped := make(chan map[wire.Kind]bool, 1)
	go func() {
		e := endpoint{conn: proxy, cfg: testConfig(), started: time.Now()}
		seen := map[wire.Kind]bool{}
		defer func() { dropped <- seen }()
		var sender *net.UDPAddr
		var held *wire.Packet
		target := server.LocalAddr().(*net.UDPAddr)
		for {
			p, from, ok, err := e.read(ctx)
			if err != nil {
				proxyDone <- err
				return
			}
			if !ok {
				continue
			}
			to := target
			if samePeer(from, target) {
				to = sender
			} else {
				sender = from
			}
			if to == nil {
				continue
			}
			if !seen[p.Kind] {
				seen[p.Kind] = true
				continue
			} // lose first Hello, HelloAck, Data, Ack, Finish, FinishAck
			if p.Kind == wire.Data {
				if p.Seq == 1 && held == nil {
					copy := p
					held = &copy
					continue
				}
				bad := p
				bad.Session++
				if err = e.write(ctx, bad, to); err != nil {
					proxyDone <- err
					return
				}
				raw, _ := p.Marshal()
				raw[len(raw)-1] ^= 1
				if _, err = proxy.WriteToUDP(raw, to); err != nil {
					proxyDone <- err
					return
				}
			}
			if p.Kind == wire.Ack {
				bad := p
				bad.Ack = 1000000
				bad.SACK = 0
				if err = e.write(ctx, bad, to); err != nil {
					proxyDone <- err
					return
				}
			}
			if err = e.write(ctx, p, to); err != nil {
				proxyDone <- err
				return
			}
			if p.Kind == wire.Data {
				if err = e.write(ctx, p, to); err != nil {
					proxyDone <- err
					return
				} // duplicate
				if p.Seq == 2 && held != nil {
					if err = e.write(ctx, *held, to); err != nil {
						proxyDone <- err
						return
					}
					held = nil
				}
			}
		}
	}()
	data := bytes.Repeat([]byte("selective recovery\n"), 5000)
	var out bytes.Buffer
	commits := 0
	cfg := testConfig()
	cfg.Commit = func() error { commits++; return nil }
	type outcome struct {
		stats Stats
		err   error
	}
	done := make(chan outcome, 1)
	go func() { s, err := Receive(ctx, server, &out, cfg); done <- outcome{s, err} }()
	sent, err := Send(ctx, client, proxy.LocalAddr().(*net.UDPAddr), bytes.NewReader(data), wire.Metadata{Size: uint64(len(data)), SHA256: sha256.Sum256(data)}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	received := <-done
	if received.err != nil {
		t.Fatal(received.err)
	}
	cancel()
	if err := <-proxyDone; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !bytes.Equal(data, out.Bytes()) || commits != 1 {
		t.Fatal("content or publication count", commits)
	}
	// A duplicate data packet generates another ACK, so dropping an ACK need not
	// cause a separate retransmission. Assert the injected faults directly.
	if len(<-dropped) != 6 {
		t.Fatal("not every packet kind was dropped")
	}
	if sent.Retransmissions < 5 || sent.Invalid == 0 || received.stats.Invalid == 0 {
		t.Fatal("faults not exercised", sent, received.stats)
	}
	if sent.PeakInFlight > cfg.Window {
		t.Fatal("window exceeded")
	}
}

func TestPublicationFailureCannotReportSuccess(t *testing.T) {
	server, client := udpSocket(t), udpSocket(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cfg := testConfig()
	cfg.MaxRetries = 1
	cfg.Commit = func() error { return errors.New("disk publication failed") }
	done := make(chan error, 1)
	go func() { _, err := Receive(ctx, server, &bytes.Buffer{}, cfg); done <- err }()
	_, err := Send(ctx, client, server.LocalAddr().(*net.UDPAddr), bytes.NewReader(nil), wire.Metadata{SHA256: sha256.Sum256(nil)}, cfg)
	if err == nil {
		t.Fatal("sender reported success after failed publication")
	}
	if err := <-done; err == nil || !strings.Contains(err.Error(), "disk publication failed") {
		t.Fatal(err)
	}
}

func TestDuplicateTrafficDoesNotExtendIdleDeadline(t *testing.T) {
	server, client := udpSocket(t), udpSocket(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cfg := testConfig()
	cfg.IdleTimeout = 150 * time.Millisecond
	done := make(chan error, 1)
	go func() { _, err := Receive(ctx, server, &bytes.Buffer{}, cfg); done <- err }()
	e := endpoint{conn: client, cfg: cfg, started: time.Now()}
	meta, _ := (wire.Metadata{Size: 1}).Marshal()
	p := wire.Packet{Kind: wire.Hello, Session: 1, Payload: meta}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "stalled") {
				t.Fatal(err)
			}
			return
		case <-ticker.C:
			if err := e.write(ctx, p, server.LocalAddr().(*net.UDPAddr)); err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("duplicate traffic kept receiver alive")
		}
	}
}
