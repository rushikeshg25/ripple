package transport

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/rushikeshg25/ripple/internal/wire"
)

func TestReassemblyReordersAndDeduplicates(t *testing.T) {
	data := bytes.Repeat([]byte("x"), wire.MaxPayload*3+7)
	var out bytes.Buffer
	r := newReassembly(wire.Metadata{Size: uint64(len(data)), SHA256: sha256.Sum256(data)}, 3, &out)
	put := func(seq uint32) bool {
		t.Helper()
		start := int(seq) * wire.MaxPayload
		ok, err := r.accept(seq, data[start:start+r.meta.PayloadSize(seq)])
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if put(3) {
		t.Fatal("accepted outside window")
	}
	put(2)
	put(1)
	if ack, sack := r.ack(); ack != 0 || sack != 6 {
		t.Fatal(ack, sack)
	}
	if put(2) {
		t.Fatal("accepted duplicate")
	}
	if out.Len() != 0 {
		t.Fatal("wrote before gap filled")
	}
	put(0)
	if ack, sack := r.ack(); ack != 3 || sack != 0 {
		t.Fatal(ack, sack)
	}
	put(3)
	put(0)
	if err := r.verify(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatal("content mismatch")
	}
}
func TestReassemblyIntegrityAndEmpty(t *testing.T) {
	r := newReassembly(wire.Metadata{SHA256: sha256.Sum256(nil)}, 1, io.Discard)
	if err := r.verify(); err != nil {
		t.Fatal(err)
	}
	r = newReassembly(wire.Metadata{Size: 1}, 1, io.Discard)
	if ok, _ := r.accept(0, []byte{1, 2}); ok {
		t.Fatal("wrong payload length")
	}
	if err := r.verify(); err == nil {
		t.Fatal("accepted incomplete")
	}
	_, _ = r.accept(0, []byte{1})
	if err := r.verify(); err == nil {
		t.Fatal("accepted wrong digest")
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func TestShortDestinationWrite(t *testing.T) {
	r := newReassembly(wire.Metadata{Size: 1}, 1, shortWriter{})
	if _, err := r.accept(0, []byte{1}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
}
