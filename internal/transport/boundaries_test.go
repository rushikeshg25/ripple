package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rushikeshg25/ripple/internal/wire"
)

func TestOversizeHelloCannotPinSession(t *testing.T) {
	server, client := udpSocket(t), udpSocket(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cfg := testConfig()
	cfg.MaxSize = 8
	type result struct {
		s   Stats
		err error
	}
	done := make(chan result, 1)
	go func() { s, err := Receive(ctx, server, &bytes.Buffer{}, cfg); done <- result{s, err} }()
	e := endpoint{conn: client, cfg: cfg, started: time.Now()}
	bad, _ := (wire.Metadata{Size: 9}).Marshal()
	if err := e.write(ctx, wire.Packet{Kind: wire.Hello, Session: 99, Payload: bad}, server.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	data := []byte("ok")
	if _, err := Send(ctx, client, server.LocalAddr().(*net.UDPAddr), bytes.NewReader(data), wire.Metadata{Size: 2, SHA256: sha256.Sum256(data)}, cfg); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil || r.s.Invalid == 0 {
		t.Fatal(r)
	}
}
func TestReceiverRejectsWrongFinalHash(t *testing.T) {
	server, client := udpSocket(t), udpSocket(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cfg := testConfig()
	published := false
	cfg.Commit = func() error { published = true; return nil }
	done := make(chan error, 1)
	go func() { _, err := Receive(ctx, server, &bytes.Buffer{}, cfg); done <- err }()
	e := endpoint{conn: client, cfg: cfg, started: time.Now()}
	meta, _ := (wire.Metadata{Size: 1}).Marshal()
	peer := server.LocalAddr().(*net.UDPAddr)
	if _, err := e.exchange(ctx, peer, wire.Packet{Kind: wire.Hello, Session: 1, Payload: meta}, func(p wire.Packet) bool { return p.Kind == wire.HelloAck }); err != nil {
		t.Fatal(err)
	}
	if _, err := e.exchange(ctx, peer, wire.Packet{Kind: wire.Data, Session: 1, Payload: []byte{1}}, func(p wire.Packet) bool { return p.Kind == wire.Ack && p.Ack == 1 }); err != nil {
		t.Fatal(err)
	}
	if err := e.write(ctx, wire.Packet{Kind: wire.Finish, Session: 1}, peer); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil || !strings.Contains(err.Error(), "SHA-256 mismatch") {
		t.Fatal(err)
	}
	if published {
		t.Fatal("published corrupt content")
	}
}
func TestIPv6Loopback(t *testing.T) {
	server, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	defer server.Close()
	client, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := Receive(ctx, server, &bytes.Buffer{}, testConfig()); done <- err }()
	if _, err := Send(ctx, client, server.LocalAddr().(*net.UDPAddr), bytes.NewReader(nil), wire.Metadata{SHA256: sha256.Sum256(nil)}, testConfig()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
