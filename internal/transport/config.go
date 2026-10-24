// Package transport implements one reliable, bounded file transfer per UDP socket.
package transport

import (
	"fmt"
	"time"

	"github.com/rushikeshg25/ripple/internal/wire"
)

type Config struct {
	Window                     int
	InitialRTO, MinRTO, MaxRTO time.Duration
	IdleTimeout                time.Duration
	Linger                     time.Duration
	MaxRetries                 int // retransmissions after the first transmission
	MaxSize                    uint64
	Observe                    func(Event)  // synchronous; must not block or retain mutable state
	Commit                     func() error // receiver only: publish verified output before FinishAck
}

func DefaultConfig() Config {
	return Config{Window: 32, InitialRTO: 200 * time.Millisecond, MinRTO: 100 * time.Millisecond, MaxRTO: 2 * time.Second, IdleTimeout: 15 * time.Second, Linger: 6 * time.Second, MaxRetries: 12, MaxSize: 1 << 30}
}
func (c Config) Validate() error {
	if c.Window < 1 || c.Window > wire.MaxWindow {
		return fmt.Errorf("window must be 1..64")
	}
	if c.MinRTO < time.Millisecond || c.InitialRTO < c.MinRTO || c.MaxRTO < c.InitialRTO || c.MaxRTO > 30*time.Second {
		return fmt.Errorf("RTO bounds must satisfy 1ms <= min <= initial <= max <= 30s")
	}
	if c.IdleTimeout <= c.MaxRTO || c.IdleTimeout > 24*time.Hour {
		return fmt.Errorf("idle timeout must exceed max RTO and be at most 24h")
	}
	if c.Linger < 2*c.MaxRTO || c.Linger > 5*time.Minute {
		return fmt.Errorf("linger must be at least twice max RTO and at most 5m")
	}
	if c.MaxRetries < 1 || c.MaxRetries > 100 {
		return fmt.Errorf("max retries must be 1..100")
	}
	if c.MaxSize == 0 || c.MaxSize > wire.MaxSize {
		return fmt.Errorf("max size must be 1..%d bytes", wire.MaxSize)
	}
	return nil
}
