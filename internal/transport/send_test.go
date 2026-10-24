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

func udpSocket(t *testing.T) *net.UDPConn {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func testConfig() Config {
	c := DefaultConfig()
	c.InitialRTO = 30 * time.Millisecond
	c.MinRTO = 20 * time.Millisecond
	c.MaxRTO = 80 * time.Millisecond
	c.Linger = 240 * time.Millisecond
	c.IdleTimeout = 3 * time.Second
	return c
}
func TestSenderWithScriptedPeer(t *testing.T) {
	server, client := udpSocket(t), udpSocket(t)
	data := bytes.Repeat([]byte("ripple"), 500)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	received := make(chan []byte, 1)
	go func() {
		e := endpoint{conn: server, cfg: testConfig(), started: time.Now()}
		var got []byte
		for {
			p, peer, ok, err := e.read(ctx)
			if err != nil {
				return
			}
			if !ok {
				continue
			}
			switch p.Kind {
			case wire.Hello:
				b, _ := wire.WindowPayload(4)
				_ = e.write(ctx, wire.Packet{Kind: wire.HelloAck, Session: p.Session, Payload: b}, peer)
			case wire.Data:
				got = append(got, p.Payload...)
				_ = e.write(ctx, wire.Packet{Kind: wire.Ack, Session: p.Session, Ack: p.Seq + 1}, peer)
			case wire.Finish:
				_ = e.write(ctx, wire.Packet{Kind: wire.FinishAck, Session: p.Session, Ack: uint32((len(data) + wire.MaxPayload - 1) / wire.MaxPayload)}, peer)
				received <- got
				return
			}
		}
	}()
	stats, err := Send(ctx, client, server.LocalAddr().(*net.UDPAddr), bytes.NewReader(data), wire.Metadata{Size: uint64(len(data)), SHA256: sha256.Sum256(data)}, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if !bytes.Equal(data, got) {
			t.Fatal("data differs")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if stats.Bytes != uint64(len(data)) || stats.SHA256 == "" {
		t.Fatal(stats)
	}
}
func TestSenderNoPeerIsBounded(t *testing.T) {
	silent, client := udpSocket(t), udpSocket(t)
	c := testConfig()
	c.MaxRetries = 1
	_, err := Send(context.Background(), client, silent.LocalAddr().(*net.UDPAddr), bytes.NewReader(nil), wire.Metadata{SHA256: sha256.Sum256(nil)}, c)
	if err == nil || !strings.Contains(err.Error(), "exhausted retries") {
		t.Fatal(err)
	}
}
