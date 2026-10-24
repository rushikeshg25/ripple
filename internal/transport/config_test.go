package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestConfigValidation(t *testing.T) {
	c := DefaultConfig()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Config){func(c *Config) { c.Window = 65 }, func(c *Config) { c.MinRTO = 0 }, func(c *Config) { c.IdleTimeout = c.MaxRTO }, func(c *Config) { c.Linger = 0 }, func(c *Config) { c.MaxRetries = 0 }, func(c *Config) { c.MaxSize = 0 }} {
		bad := c
		change(&bad)
		if bad.Validate() == nil {
			t.Fatal("accepted invalid config")
		}
	}
}
func TestSocketCancellation(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	e := endpoint{conn: conn, started: time.Now()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := e.read(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
