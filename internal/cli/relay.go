package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"

	"github.com/rushikeshg25/ripple/internal/relay"
)

func runRelay(ctx context.Context, args []string, out, errout io.Writer) (err error) {
	cfg := relay.DefaultConfig()
	fs := flag.NewFlagSet("relay", flag.ContinueOnError)
	fs.SetOutput(errout)
	listen := fs.String("listen", "127.0.0.1:9001", "UDP listen address")
	target := fs.String("to", "", "receiver HOST:PORT")
	fs.Float64Var(&cfg.Loss, "loss", 0, "drop probability per datagram, each direction (0..1)")
	fs.Float64Var(&cfg.Duplicate, "duplicate", 0, "duplicate probability (0..1)")
	fs.Float64Var(&cfg.Corrupt, "corrupt", 0, "single-bit corruption probability per copy (0..1)")
	fs.DurationVar(&cfg.Delay, "delay", 0, "base one-way delay")
	fs.DurationVar(&cfg.Jitter, "jitter", 0, "uniform plus/minus delay variation; negative totals clamp to zero")
	fs.Int64Var(&cfg.Seed, "seed", cfg.Seed, "pseudo-random fault seed")
	fs.IntVar(&cfg.QueueLimit, "queue", cfg.QueueLimit, "maximum queued datagram copies")
	if err = fs.Parse(args); errors.Is(err, flag.ErrHelp) {
		return nil
	} else if err != nil {
		return err
	}
	if fs.NArg() != 0 || *target == "" {
		return fmt.Errorf("relay requires -to HOST:PORT and accepts no positional arguments")
	}
	if err = cfg.Validate(); err != nil {
		return err
	}
	upstream, err := resolve(*target, true)
	if err != nil {
		return err
	}
	addr, err := resolve(*listen, false)
	if err != nil {
		return err
	}
	network := "udp6"
	if upstream.IP.To4() != nil {
		network = "udp4"
	}
	if addr.IP != nil && ((addr.IP.To4() != nil) != (upstream.IP.To4() != nil)) {
		return fmt.Errorf("relay listen and receiver must use the same IP family")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	conn, err := net.ListenUDP(network, addr)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	if _, err = fmt.Fprintf(errout, "relaying %s -> %s\n", conn.LocalAddr(), upstream); err != nil {
		return err
	}
	stats, runErr := relay.Run(ctx, conn, upstream, cfg)
	if err = json.NewEncoder(out).Encode(stats); err != nil {
		return errors.Join(runErr, err)
	}
	if errors.Is(runErr, context.Canceled) {
		return nil
	}
	return runErr
}
