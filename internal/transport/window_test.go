package transport

import (
	"testing"
	"time"
)

func TestSenderWindowAndSelectiveACK(t *testing.T) {
	w := newSendWindow(4)
	for i := 0; i < 4; i++ {
		w.add([]byte{byte(i)}, time.Now())
	}
	if w.available(64) {
		t.Fatal("exceeded sequence window")
	}
	done, ok := w.acknowledge(0, 0b1110)
	if !ok || len(done) != 3 || len(w.pending) != 1 {
		t.Fatal("selective acknowledgement failed")
	}
	if w.available(64) {
		t.Fatal("SACK allowed receiver window overrun")
	}
	done, ok = w.acknowledge(4, 0)
	if !ok || len(done) != 1 || !w.available(1) {
		t.Fatal("cumulative acknowledgement failed")
	}
	if done, ok := w.acknowledge(4, 0); !ok || len(done) != 0 {
		t.Fatal("duplicate ACK")
	}
}
func TestInvalidACKDoesNotAdvance(t *testing.T) {
	w := newSendWindow(64)
	w.add([]byte{1}, time.Now())
	for _, a := range []struct {
		ack  uint32
		sack uint64
	}{{2, 0}, {0, 2}, {1, 1}, {0, 1 << 63}} {
		if _, ok := w.acknowledge(a.ack, a.sack); ok {
			t.Fatal("accepted unsent ACK", a)
		}
		if w.base != 0 || len(w.pending) != 1 {
			t.Fatal("invalid ACK mutated state")
		}
	}
	w.acknowledge(1, 0)
	if _, ok := w.acknowledge(0, 1); ok {
		t.Fatal("accepted stale ACK")
	}
}
