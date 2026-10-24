// Package cli wires the transport to files, UDP sockets, and command-line flags.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"

	"github.com/rushikeshg25/ripple/internal/fileio"
	"github.com/rushikeshg25/ripple/internal/transport"
)

const usage = `Ripple — reliable file transfer over UDP

Usage:
  ripple send -to HOST:PORT -file PATH
  ripple receive -listen 127.0.0.1:9000 -out PATH
  ripple relay -listen 127.0.0.1:9001 -to 127.0.0.1:9000 -loss 0.05

Run a command with -h for its options. Receivers accept one transfer and exit.
`

func Run(ctx context.Context, args []string, out, errout io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		_, err := io.WriteString(out, usage)
		return err
	}
	switch args[0] {
	case "relay":
		return runRelay(ctx, args[1:], out, errout)
	case "send", "receive":
		return transfer(ctx, args[0], args[1:], out, errout)
	default:
		return fmt.Errorf("unknown command %q; use ripple -h", args[0])
	}
}
func transfer(ctx context.Context, mode string, args []string, out, errout io.Writer) (err error) {
	cfg := transport.DefaultConfig()
	fs := flag.NewFlagSet(mode, flag.ContinueOnError)
	fs.SetOutput(errout)
	var address, path string
	if mode == "send" {
		fs.StringVar(&address, "to", "", "receiver or relay HOST:PORT")
		fs.StringVar(&path, "file", "", "regular source file")
	} else {
		fs.StringVar(&address, "listen", "127.0.0.1:9000", "UDP listen address")
		fs.StringVar(&path, "out", "", "new destination path (must not exist)")
	}
	fs.IntVar(&cfg.Window, "window", cfg.Window, "maximum in-flight sequence window (1..64)")
	fs.DurationVar(&cfg.IdleTimeout, "idle-timeout", cfg.IdleTimeout, "maximum time without transfer progress (greater than 2s)")
	fs.Uint64Var(&cfg.MaxSize, "max-size", cfg.MaxSize, "maximum file size in bytes")
	if err = fs.Parse(args); errors.Is(err, flag.ErrHelp) {
		return nil
	} else if err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v; put flags before positional values", fs.Args())
	}
	if path == "" || address == "" {
		return fmt.Errorf("%s requires an address and file path", mode)
	}
	if err = cfg.Validate(); err != nil {
		return err
	}
	addr, err := resolve(address, mode == "send")
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	network := "udp6"
	if addr.IP.To4() != nil || addr.IP == nil {
		network = "udp4"
	}
	var stats transport.Stats
	if mode == "send" {
		source, openErr := fileio.OpenSource(ctx, path, cfg.MaxSize)
		if openErr != nil {
			return openErr
		}
		defer func() { err = errors.Join(err, source.File.Close()) }()
		conn, listenErr := net.ListenUDP(network, nil)
		if listenErr != nil {
			return listenErr
		}
		defer func() { err = errors.Join(err, conn.Close()) }()
		stats, err = transport.Send(ctx, conn, addr, source.File, source.Metadata, cfg)
	} else {
		destination, openErr := fileio.NewDestination(path)
		if openErr != nil {
			return openErr
		}
		defer func() { err = errors.Join(err, destination.Close()) }()
		conn, listenErr := net.ListenUDP(network, addr)
		if listenErr != nil {
			return listenErr
		}
		defer func() { err = errors.Join(err, conn.Close()) }()
		if _, err = fmt.Fprintf(errout, "listening %s\n", conn.LocalAddr()); err != nil {
			return err
		}
		cfg.Commit = destination.Publish
		stats, err = transport.Receive(ctx, conn, destination, cfg)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "%s complete: %d bytes, sha256=%s, retries=%d\n", mode, stats.Bytes, stats.SHA256, stats.Retransmissions)
	return err
}
func resolve(address string, remote bool) (*net.UDPAddr, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, fmt.Errorf("UDP address: %w", err)
	}
	if remote && (addr.Port == 0 || addr.IP == nil || addr.IP.IsUnspecified() || addr.IP.IsMulticast()) {
		return nil, fmt.Errorf("remote address must identify one host and a nonzero port")
	}
	return addr, nil
}
