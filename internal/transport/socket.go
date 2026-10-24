package transport

import (
	"context"
	"errors"
	"github.com/rushikeshg25/ripple/internal/wire"
	"net"
	"time"
)

const pollInterval = 10 * time.Millisecond

type endpoint struct {
	conn    *net.UDPConn
	cfg     Config
	stats   Stats
	started time.Time
}

func (e *endpoint) emit(kind string, seq uint32) {
	e.stats.ElapsedSeconds = time.Since(e.started).Seconds()
	if e.cfg.Observe != nil {
		e.cfg.Observe(Event{Time: time.Now().UTC(), Type: kind, Seq: seq, Stats: e.stats})
	}
}
func (e *endpoint) write(ctx context.Context, p wire.Packet, peer *net.UDPAddr) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b, err := p.Marshal()
	if err != nil {
		return err
	}
	if err = e.conn.SetWriteDeadline(time.Now().Add(pollInterval)); err != nil {
		return err
	}
	_, err = e.conn.WriteToUDP(b, peer)
	if err != nil {
		return err
	}
	e.stats.Sent++
	return nil
}
func (e *endpoint) read(ctx context.Context) (wire.Packet, *net.UDPAddr, bool, error) {
	if err := ctx.Err(); err != nil {
		return wire.Packet{}, nil, false, err
	}
	if err := e.conn.SetReadDeadline(time.Now().Add(pollInterval)); err != nil {
		return wire.Packet{}, nil, false, err
	}
	var b [wire.MaxDatagram + 1]byte // one extra byte exposes truncated oversized datagrams
	n, peer, err := e.conn.ReadFromUDP(b[:])
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return wire.Packet{}, nil, false, nil
		}
		return wire.Packet{}, nil, false, err
	}
	e.stats.Received++
	p, err := wire.Parse(b[:n])
	if err != nil {
		e.stats.Invalid++
		return wire.Packet{}, peer, false, nil
	}
	return p, peer, true, nil
}
func samePeer(a, b *net.UDPAddr) bool {
	return a != nil && b != nil && a.Port == b.Port && a.Zone == b.Zone && a.IP.Equal(b.IP)
}
func control(p wire.Packet) bool { return p.Seq == 0 && p.Ack == 0 && p.SACK == 0 }
