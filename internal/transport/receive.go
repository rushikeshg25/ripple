package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/rushikeshg25/ripple/internal/wire"
)

// Receive accepts one peer/session. Commit runs once after size/hash validation,
// before acknowledging completion. A short linger answers duplicate Finish packets.
// The caller owns conn and must clean up unpublished output on error.
func Receive(ctx context.Context, conn *net.UDPConn, out io.Writer, cfg Config) (result Stats, err error) {
	if err = cfg.Validate(); err != nil {
		return result, err
	}
	if conn == nil || out == nil {
		return result, fmt.Errorf("receive requires socket and destination")
	}
	e := &endpoint{conn: conn, cfg: cfg, started: time.Now()}
	defer func() {
		kind := "complete"
		if err != nil {
			kind = "error"
		}
		e.emit(kind, 0)
		result = e.stats
	}()
	e.emit("listening", 0)
	var peer *net.UDPAddr
	var hello wire.Packet
	var meta wire.Metadata
	for peer == nil {
		if time.Since(e.started) > cfg.IdleTimeout {
			return result, fmt.Errorf("timed out waiting for sender")
		}
		p, from, ok, readErr := e.read(ctx)
		if readErr != nil {
			return result, readErr
		}
		if !ok {
			continue
		}
		m, parseErr := wire.ParseMetadata(p.Payload)
		if p.Kind != wire.Hello || !control(p) || parseErr != nil || m.Size > cfg.MaxSize {
			e.stats.Invalid++
			continue
		}
		peer, hello, meta = from, p, m
	}
	window, _ := wire.WindowPayload(cfg.Window)
	helloAck := wire.Packet{Kind: wire.HelloAck, Session: hello.Session, Payload: window}
	if err = e.write(ctx, helloAck, peer); err != nil {
		return result, err
	}
	r := newReassembly(meta, cfg.Window, out)
	lastProgress := time.Now()
	var committedAt time.Time
	e.emit("connected", 0)
	for {
		if !committedAt.IsZero() && time.Since(committedAt) >= cfg.Linger {
			return result, nil
		}
		if committedAt.IsZero() && time.Since(lastProgress) > cfg.IdleTimeout {
			return result, fmt.Errorf("receive stalled after %d bytes", r.bytes)
		}
		p, from, ok, readErr := e.read(ctx)
		if readErr != nil {
			return result, readErr
		}
		if !ok {
			continue
		}
		if !samePeer(from, peer) || p.Session != hello.Session {
			e.stats.Invalid++
			continue
		}
		switch p.Kind {
		case wire.Hello:
			if !control(p) || !bytes.Equal(p.Payload, hello.Payload) {
				e.stats.Invalid++
				continue
			}
			if err = e.write(ctx, helloAck, peer); err != nil {
				return result, err
			}
		case wire.Data:
			if p.Ack != 0 || p.SACK != 0 || len(p.Payload) != meta.PayloadSize(p.Seq) || len(p.Payload) == 0 {
				e.stats.Invalid++
				continue
			}
			if committedAt.IsZero() {
				accepted, acceptErr := r.accept(p.Seq, p.Payload)
				if acceptErr != nil {
					return result, acceptErr
				}
				if accepted {
					lastProgress = time.Now()
					e.stats.Bytes = r.bytes
					e.emit("received", p.Seq)
				}
			}
			ack, sack := r.ack()
			if err = e.write(ctx, wire.Packet{Kind: wire.Ack, Session: hello.Session, Ack: ack, SACK: sack}, peer); err != nil {
				return result, err
			}
		case wire.Finish:
			if !control(p) || len(p.Payload) != 0 {
				e.stats.Invalid++
				continue
			}
			if r.next != meta.Packets() {
				ack, sack := r.ack()
				if err = e.write(ctx, wire.Packet{Kind: wire.Ack, Session: hello.Session, Ack: ack, SACK: sack}, peer); err != nil {
					return result, err
				}
				continue
			}
			if committedAt.IsZero() {
				if err = r.verify(); err != nil {
					return result, err
				}
				if cfg.Commit != nil {
					if err = cfg.Commit(); err != nil {
						return result, fmt.Errorf("publish destination: %w", err)
					}
				}
				committedAt = time.Now()
				e.stats.SHA256 = fmt.Sprintf("%x", meta.SHA256)
				e.emit("published", 0)
			}
			if err = e.write(ctx, wire.Packet{Kind: wire.FinishAck, Session: hello.Session, Ack: meta.Packets()}, peer); err != nil {
				return result, err
			}
		default:
			e.stats.Invalid++
		}
	}
}
