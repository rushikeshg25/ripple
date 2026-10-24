package transport

// congestion is packet-count slow start followed by additive increase. A timeout
// halves the threshold and returns to one packet. Fast recovery is out of scope.
type congestion struct {
	window, threshold float64
	limit             int
}

func newCongestion(limit int) *congestion {
	return &congestion{window: 1, threshold: float64(limit), limit: limit}
}
func (c *congestion) allowance() int {
	n := int(c.window)
	if n < 1 {
		return 1
	}
	if n > c.limit {
		return c.limit
	}
	return n
}
func (c *congestion) acknowledged(n int) {
	for i := 0; i < n; i++ {
		if c.window < c.threshold {
			c.window++
		} else {
			c.window += 1 / c.window
		}
		if c.window > float64(c.limit) {
			c.window = float64(c.limit)
		}
	}
}
func (c *congestion) lost() {
	c.threshold = c.window / 2
	if c.threshold < 2 {
		c.threshold = 2
	}
	c.window = 1
}
