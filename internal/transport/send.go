package transport

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/rushikeshg25/ripple/internal/wire"
)

// Send transfers exactly meta.Size bytes, checking the source digest before close.
// The caller owns conn; no other goroutine may read from it during this call.
func Send(ctx context.Context, conn *net.UDPConn, peer *net.UDPAddr, src io.Reader, meta wire.Metadata, cfg Config) (result Stats, err error) {
	if err = cfg.Validate(); err != nil {
		return result, err
	}
	if conn == nil || peer == nil || src == nil {
		return result, fmt.Errorf("send requires socket, peer, and source")
	}
	if meta.Size > cfg.MaxSize {
		return result, fmt.Errorf("source exceeds max size")
	}
	payload, err := meta.Marshal()
	if err != nil {
		return result, err
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
	var random [8]byte
	if _, err = rand.Read(random[:]); err != nil {
		return result, err
	}
	session := binary.BigEndian.Uint64(random[:])
	if session == 0 {
		session = 1
	}
	hello := wire.Packet{Kind: wire.Hello, Session: session, Payload: payload}
	response, err := e.exchange(ctx, peer, hello, func(p wire.Packet) bool {
		_, err := wire.ParseWindow(p.Payload)
		return p.Kind == wire.HelloAck && control(p) && err == nil
	})
	if err != nil {
		return result, fmt.Errorf("handshake: %w", err)
	}
	remoteWindow, _ := wire.ParseWindow(response.Payload)
	limit := min(cfg.Window, remoteWindow)
	w := newSendWindow(limit)
	cc := newCongestion(limit)
	timer := newRTO(cfg.InitialRTO, cfg.MinRTO, cfg.MaxRTO)
	digest := sha256.New()
	lastProgress := time.Now()
	var recoveryUntil time.Time
	update := func() {
		e.stats.CongestionWindow = cc.window
		e.stats.InFlight = len(w.pending)
		e.stats.PeakInFlight = max(e.stats.PeakInFlight, len(w.pending))
		e.stats.RTTMillis = float64(timer.srtt) / float64(time.Millisecond)
		e.stats.RTOMillis = float64(timer.current) / float64(time.Millisecond)
	}
	update()
	e.emit("connected", 0)
	for w.next < meta.Packets() || len(w.pending) > 0 {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		if time.Since(lastProgress) > cfg.IdleTimeout {
			return result, fmt.Errorf("data transfer stalled")
		}
		for w.next < meta.Packets() && w.available(cc.allowance()) {
			data := make([]byte, meta.PayloadSize(w.next))
			if _, err = io.ReadFull(src, data); err != nil {
				return result, fmt.Errorf("read source: %w", err)
			}
			_, _ = digest.Write(data)
			f := w.add(data, time.Now())
			if err = e.write(ctx, wire.Packet{Kind: wire.Data, Session: session, Seq: f.seq, Payload: data}, peer); err != nil {
				return result, err
			}
			update()
			e.emit("sent", f.seq)
		}
		now := time.Now()
		var expired *flight
		for _, f := range w.pending {
			if now.Sub(f.sent) >= timer.current && (expired == nil || f.sent.Before(expired.sent)) {
				expired = f
			}
		}
		if expired != nil {
			if expired.tries > cfg.MaxRetries {
				return result, fmt.Errorf("packet %d exhausted retries", expired.seq)
			}
			if !now.Before(recoveryUntil) {
				cc.lost()
				timer.backoff()
				recoveryUntil = now.Add(timer.current)
			}
			if err = e.write(ctx, wire.Packet{Kind: wire.Data, Session: session, Seq: expired.seq, Payload: expired.data}, peer); err != nil {
				return result, err
			}
			expired.tries++
			expired.sent = time.Now()
			e.stats.Retransmissions++
			update()
			e.emit("retransmit", expired.seq)
		}
		p, from, ok, readErr := e.read(ctx)
		if readErr != nil {
			return result, readErr
		}
		if !ok {
			continue
		}
		if !samePeer(from, peer) || p.Session != session || p.Kind != wire.Ack || p.Seq != 0 || len(p.Payload) != 0 {
			e.stats.Invalid++
			continue
		}
		oldBase := w.base
		done, valid := w.acknowledge(p.Ack, p.SACK)
		if !valid {
			e.stats.Invalid++
			continue
		}
		if len(done) > 0 || w.base > oldBase {
			lastProgress = time.Now()
		}
		for _, f := range done {
			e.stats.Bytes += uint64(len(f.data))
			timer.observe(time.Since(f.sent), f.tries > 1)
		}
		cc.acknowledged(len(done))
		update()
		e.emit("ack", p.Ack)
	}
	var extra [1]byte
	if _, err = io.ReadFull(src, extra[:]); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("source grew during transfer")
		}
		return result, fmt.Errorf("check source length: %w", err)
	}
	if string(digest.Sum(nil)) != string(meta.SHA256[:]) {
		return result, fmt.Errorf("source changed: SHA-256 mismatch")
	}
	_, err = e.exchange(ctx, peer, wire.Packet{Kind: wire.Finish, Session: session}, func(p wire.Packet) bool {
		return p.Kind == wire.FinishAck && p.Seq == 0 && p.Ack == meta.Packets() && p.SACK == 0 && len(p.Payload) == 0
	})
	if err != nil {
		return result, fmt.Errorf("finish: %w", err)
	}
	e.stats.SHA256 = fmt.Sprintf("%x", meta.SHA256)
	return result, nil
}

func (e *endpoint) exchange(ctx context.Context, peer *net.UDPAddr, request wire.Packet, accept func(wire.Packet) bool) (wire.Packet, error) {
	timeout := e.cfg.InitialRTO
	started := time.Now()
	for attempt := 0; attempt <= e.cfg.MaxRetries; attempt++ {
		if err := e.write(ctx, request, peer); err != nil {
			return wire.Packet{}, err
		}
		if attempt > 0 {
			e.stats.Retransmissions++
			e.emit("control_retry", 0)
		}
		until := time.Now().Add(timeout)
		for time.Now().Before(until) {
			if time.Since(started) > e.cfg.IdleTimeout {
				return wire.Packet{}, fmt.Errorf("control exchange stalled")
			}
			p, from, ok, err := e.read(ctx)
			if err != nil {
				return wire.Packet{}, err
			}
			if ok && samePeer(from, peer) && p.Session == request.Session && accept(p) {
				return p, nil
			}
			if ok {
				e.stats.Invalid++
			}
		}
		timeout = min(2*timeout, e.cfg.MaxRTO)
	}
	return wire.Packet{}, fmt.Errorf("control exchange exhausted retries")
}
