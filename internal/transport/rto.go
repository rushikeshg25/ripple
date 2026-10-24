package transport

import "time"

// rtoEstimator uses RFC 6298's smoothing weights, with lab-oriented timer bounds.
// It is not a claim of TCP compliance. Retransmitted packets never supply samples.
type rtoEstimator struct {
	srtt      time.Duration
	variation time.Duration
	current   time.Duration
	min       time.Duration
	max       time.Duration
}

func newRTO(initial, min, max time.Duration) *rtoEstimator {
	return &rtoEstimator{current: initial, min: min, max: max}
}
func (r *rtoEstimator) observe(sample time.Duration, retransmitted bool) {
	if retransmitted || sample <= 0 {
		return
	}
	if r.srtt == 0 {
		r.srtt = sample
		r.variation = sample / 2
	} else {
		delta := r.srtt - sample
		if delta < 0 {
			delta = -delta
		}
		r.variation = (3*r.variation + delta) / 4
		r.srtt = (7*r.srtt + sample) / 8
	}
	variance := 4 * r.variation
	if variance < time.Millisecond {
		variance = time.Millisecond
	}
	r.current = r.clamp(r.srtt + variance)
}
func (r *rtoEstimator) backoff() {
	if r.current >= r.max/2 {
		r.current = r.max
	} else {
		r.current = r.clamp(2 * r.current)
	}
}
func (r *rtoEstimator) clamp(d time.Duration) time.Duration {
	if d < r.min {
		return r.min
	}
	if d > r.max {
		return r.max
	}
	return d
}
