// Package relay provides a bounded, seeded, single-client UDP fault relay.
package relay

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"time"

	"github.com/rushikeshg25/ripple/internal/wire"
)

type Config struct {
	Loss, Duplicate, Corrupt float64
	Delay, Jitter            time.Duration
	Seed                     int64
	QueueLimit               int
}

func DefaultConfig() Config { return Config{Seed: 1, QueueLimit: 4096} }
func (c Config) Validate() error {
	for _, p := range []float64{c.Loss, c.Duplicate, c.Corrupt} {
		if math.IsNaN(p) || math.IsInf(p, 0) || p < 0 || p > 1 {
			return fmt.Errorf("fault probabilities must be finite numbers in [0,1]")
		}
	}
	if c.Delay < 0 || c.Jitter < 0 || c.Delay > time.Minute || c.Jitter > time.Minute {
		return fmt.Errorf("delay and jitter must be between 0 and 1m")
	}
	if c.QueueLimit < 1 || c.QueueLimit > 65536 {
		return fmt.Errorf("queue limit must be 1..65536")
	}
	return nil
}

type Stats struct {
	Received     uint64 `json:"received"`
	Forwarded    uint64 `json:"forwarded"`
	Dropped      uint64 `json:"dropped"`
	Duplicated   uint64 `json:"duplicated"`
	Corrupted    uint64 `json:"corrupted"`
	QueueDropped uint64 `json:"queue_dropped"`
	Foreign      uint64 `json:"foreign"`
	PeakQueued   int    `json:"peak_queued"`
}

type datagram struct {
	at    time.Time
	order uint64
	data  []byte
	to    *net.UDPAddr
}
type schedule []datagram

func (q schedule) Len() int { return len(q) }
func (q schedule) Less(i, j int) bool {
	if q[i].at.Equal(q[j].at) {
		return q[i].order < q[j].order
	}
	return q[i].at.Before(q[j].at)
}
func (q schedule) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *schedule) Push(v any)   { *q = append(*q, v.(datagram)) }
func (q *schedule) Pop() any {
	old := *q
	n := len(old)
	v := old[n-1]
	old[n-1] = datagram{}
	*q = old[:n-1]
	return v
}

// Run pins the first valid Hello sender. It owns socket I/O until cancellation;
// it neither closes the caller's socket nor supports a second client/session.
func Run(ctx context.Context, conn *net.UDPConn, upstream *net.UDPAddr, cfg Config) (stats Stats, err error) {
	if err = cfg.Validate(); err != nil {
		return stats, err
	}
	if conn == nil || upstream == nil {
		return stats, fmt.Errorf("relay requires socket and upstream")
	}
	random := rand.New(rand.NewSource(cfg.Seed))
	var client *net.UDPAddr
	var queue schedule
	var order uint64
	for {
		if err = ctx.Err(); err != nil {
			return stats, err
		}
		now := time.Now()
		for len(queue) > 0 && !queue[0].at.After(now) {
			d := heap.Pop(&queue).(datagram)
			if err = conn.SetWriteDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
				return stats, err
			}
			if _, err = conn.WriteToUDP(d.data, d.to); err != nil {
				return stats, err
			}
			stats.Forwarded++
		}
		deadline := time.Now().Add(5 * time.Millisecond)
		if len(queue) > 0 && queue[0].at.Before(deadline) {
			deadline = queue[0].at
		}
		if err = conn.SetReadDeadline(deadline); err != nil {
			return stats, err
		}
		var buffer [wire.MaxDatagram + 1]byte
		n, from, readErr := conn.ReadFromUDP(buffer[:])
		if readErr != nil {
			var ne net.Error
			if errors.As(readErr, &ne) && ne.Timeout() {
				continue
			}
			return stats, readErr
		}
		stats.Received++
		if n > wire.MaxDatagram {
			stats.Dropped++
			continue
		}
		to := upstream
		if equal(from, upstream) {
			if client == nil {
				stats.Foreign++
				continue
			}
			to = client
		} else {
			if client == nil {
				p, parseErr := wire.Parse(buffer[:n])
				if parseErr != nil || p.Kind != wire.Hello || p.Seq != 0 || p.Ack != 0 || p.SACK != 0 {
					stats.Foreign++
					continue
				}
				if _, parseErr = wire.ParseMetadata(p.Payload); parseErr != nil {
					stats.Foreign++
					continue
				}
				client = from
			}
			if !equal(from, client) {
				stats.Foreign++
				continue
			}
		}
		if random.Float64() < cfg.Loss {
			stats.Dropped++
			continue
		}
		copies := 1
		if random.Float64() < cfg.Duplicate {
			copies = 2
			stats.Duplicated++
		}
		for i := 0; i < copies; i++ {
			if len(queue) >= cfg.QueueLimit {
				stats.QueueDropped++
				continue
			}
			data := append([]byte(nil), buffer[:n]...)
			if len(data) > 0 && random.Float64() < cfg.Corrupt {
				data[random.Intn(len(data))] ^= 1
				stats.Corrupted++
			}
			delay := cfg.Delay
			if cfg.Jitter > 0 {
				delay += time.Duration(random.Int63n(2*int64(cfg.Jitter)+1)) - cfg.Jitter
			}
			if delay < 0 {
				delay = 0
			}
			order++
			heap.Push(&queue, datagram{at: time.Now().Add(delay), order: order, data: data, to: to})
			stats.PeakQueued = max(stats.PeakQueued, len(queue))
		}
	}
}
func equal(a, b *net.UDPAddr) bool {
	return a != nil && b != nil && a.Port == b.Port && a.Zone == b.Zone && a.IP.Equal(b.IP)
}
