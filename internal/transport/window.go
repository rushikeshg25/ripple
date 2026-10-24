package transport

import (
	"time"

	"github.com/rushikeshg25/ripple/internal/wire"
)

type flight struct {
	seq   uint32
	data  []byte
	sent  time.Time
	tries int
}
type sendWindow struct {
	base    uint32
	next    uint32
	limit   uint32
	pending map[uint32]*flight
}

func newSendWindow(limit int) *sendWindow {
	return &sendWindow{limit: uint32(limit), pending: make(map[uint32]*flight)}
}
func (w *sendWindow) available(cwnd int) bool {
	return len(w.pending) < cwnd && uint64(w.next) < uint64(w.base)+uint64(w.limit)
}
func (w *sendWindow) add(data []byte, now time.Time) *flight {
	f := &flight{seq: w.next, data: data, sent: now, tries: 1}
	w.pending[f.seq] = f
	w.next++
	return f
}

// acknowledge rejects claims about unsent sequence numbers before mutating state.
// Stale cumulative ACKs are ignored, including their old selective bitmap.
func (w *sendWindow) acknowledge(ack uint32, sack uint64) ([]*flight, bool) {
	if ack < w.base || ack > w.next {
		return nil, false
	}
	for i := uint64(0); i < wire.MaxWindow; i++ {
		if sack&(uint64(1)<<i) != 0 && uint64(ack)+i >= uint64(w.next) {
			return nil, false
		}
	}
	var done []*flight
	// Sequence order also makes RTT observation deterministic across runs.
	for seq := w.base; seq < w.next; seq++ {
		f, ok := w.pending[seq]
		if !ok {
			continue
		}
		selected := seq >= ack && uint64(seq)-uint64(ack) < 64 && sack&(uint64(1)<<(seq-ack)) != 0
		if seq < ack || selected {
			done = append(done, f)
			delete(w.pending, seq)
		}
	}
	w.base = ack
	return done, true
}
