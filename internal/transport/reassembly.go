package transport

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/rushikeshg25/ripple/internal/wire"
)

type reassembly struct {
	meta    wire.Metadata
	window  uint32
	next    uint32
	pending map[uint32][]byte
	out     io.Writer
	hash    hash.Hash
	bytes   uint64
}

func newReassembly(m wire.Metadata, window int, out io.Writer) *reassembly {
	return &reassembly{meta: m, window: uint32(window), pending: make(map[uint32][]byte), out: out, hash: sha256.New()}
}

// accept buffers only within the advertised sequence range and writes each byte once.
func (r *reassembly) accept(seq uint32, payload []byte) (bool, error) {
	if len(payload) == 0 || len(payload) != r.meta.PayloadSize(seq) {
		return false, nil
	}
	if seq < r.next {
		return false, nil
	}
	if uint64(seq) >= uint64(r.next)+uint64(r.window) {
		return false, nil
	}
	if _, exists := r.pending[seq]; exists {
		return false, nil
	}
	r.pending[seq] = append([]byte(nil), payload...)
	for {
		data, ok := r.pending[r.next]
		if !ok {
			break
		}
		n, err := r.out.Write(data)
		if err != nil {
			return false, fmt.Errorf("write received bytes: %w", err)
		}
		if n != len(data) {
			return false, io.ErrShortWrite
		}
		_, _ = r.hash.Write(data)
		r.bytes += uint64(n)
		delete(r.pending, r.next)
		r.next++
	}
	return true, nil
}
func (r *reassembly) ack() (uint32, uint64) {
	var sack uint64
	for seq := range r.pending {
		sack |= uint64(1) << (seq - r.next)
	}
	return r.next, sack
}
func (r *reassembly) verify() error {
	if r.bytes != r.meta.Size || r.next != r.meta.Packets() {
		return fmt.Errorf("transfer incomplete: got %d of %d bytes", r.bytes, r.meta.Size)
	}
	if string(r.hash.Sum(nil)) != string(r.meta.SHA256[:]) {
		return fmt.Errorf("SHA-256 mismatch")
	}
	return nil
}
