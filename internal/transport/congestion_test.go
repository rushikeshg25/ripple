package transport

import "testing"

func TestCongestionGrowthAndLoss(t *testing.T) {
	c := newCongestion(64)
	if c.allowance() != 1 {
		t.Fatal("initial window")
	}
	c.acknowledged(7)
	if c.allowance() != 8 {
		t.Fatal("slow start", c.window)
	}
	c.lost()
	if c.window != 1 || c.threshold != 4 {
		t.Fatal("timeout", c)
	}
	c.acknowledged(3)
	if c.window != 4 {
		t.Fatal("recovery slow start", c)
	}
	c.acknowledged(4)
	if c.window <= 4 || c.window >= 6 {
		t.Fatal("additive increase", c)
	}
	c.acknowledged(100000)
	if c.allowance() != 64 {
		t.Fatal("window limit")
	}
	before := c.window
	c.acknowledged(0)
	if c.window != before {
		t.Fatal("duplicate ACK inflated window")
	}
}
func TestOnePacketWindow(t *testing.T) {
	c := newCongestion(1)
	c.acknowledged(100)
	c.lost()
	c.acknowledged(100)
	if c.allowance() != 1 {
		t.Fatal(c)
	}
}
