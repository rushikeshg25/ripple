package transport

import (
	"testing"
	"time"
)

func TestRTOSmoothingAndKarn(t *testing.T) {
	r := newRTO(200*time.Millisecond, 10*time.Millisecond, time.Second)
	r.observe(100*time.Millisecond, false)
	if r.srtt != 100*time.Millisecond || r.current != 300*time.Millisecond {
		t.Fatal(r)
	}
	r.observe(200*time.Millisecond, false)
	if r.srtt != 112500*time.Microsecond || r.variation != 62500*time.Microsecond || r.current != 362500*time.Microsecond {
		t.Fatal(r)
	}
	before := *r
	r.observe(time.Second, true)
	if *r != before {
		t.Fatal("ambiguous retransmit sample changed RTT")
	}
	for i := 0; i < 10; i++ {
		r.backoff()
	}
	if r.current != time.Second {
		t.Fatal("backoff not capped")
	}
	r.observe(time.Microsecond, false)
	if r.current > time.Second {
		t.Fatal("sample not capped")
	}
}
func TestRTOMinimum(t *testing.T) {
	r := newRTO(time.Second, 100*time.Millisecond, time.Second)
	r.observe(time.Microsecond, false)
	if r.current != 100*time.Millisecond {
		t.Fatal(r.current)
	}
}
