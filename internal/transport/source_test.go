package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rushikeshg25/ripple/internal/wire"
)

func TestSourceMutationNeverPublishes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		data    []byte
		size    uint64
		hash    [32]byte
		message string
	}{
		{"truncated", []byte{1}, 2, sha256.Sum256([]byte{1, 2}), "read source"},
		{"grown", []byte{1, 2}, 1, sha256.Sum256([]byte{1}), "source grew"},
		{"modified", []byte{2}, 1, sha256.Sum256([]byte{1}), "source changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, client := udpSocket(t), udpSocket(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			cfg := testConfig()
			published := false
			cfg.Commit = func() error { published = true; return nil }
			done := make(chan error, 1)
			go func() { _, err := Receive(ctx, server, &bytes.Buffer{}, cfg); done <- err }()
			_, err := Send(ctx, client, server.LocalAddr().(*net.UDPAddr), bytes.NewReader(tc.data), wire.Metadata{Size: tc.size, SHA256: tc.hash}, cfg)
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatal(err)
			}
			cancel()
			<-done
			if published {
				t.Fatal("published changed source")
			}
		})
	}
}
