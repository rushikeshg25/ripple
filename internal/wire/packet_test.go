package wire

import (
	"bytes"
	"testing"
)

func TestPacketRoundTrip(t *testing.T) {
	want := Packet{Kind: Data, Session: 9, Seq: 42, Ack: 10, SACK: 0x8000000000000001, Payload: bytes.Repeat([]byte{7}, MaxPayload)}
	b, err := want.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != want.Kind || got.Session != want.Session || got.Seq != want.Seq || got.Ack != want.Ack || got.SACK != want.SACK || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("roundtrip: %+v", got)
	}
	b[HeaderSize] = 99
	if got.Payload[0] != 7 {
		t.Fatal("payload aliases input")
	}
}
func TestRejectCorruptionAndLengths(t *testing.T) {
	b, _ := (Packet{Kind: Ack, Session: 1}).Marshal()
	for i := range b {
		damaged := append([]byte(nil), b...)
		damaged[i] ^= 1
		if _, err := Parse(damaged); err == nil {
			t.Fatalf("accepted changed byte %d", i)
		}
	}
	for n := 0; n < HeaderSize; n++ {
		if _, err := Parse(b[:n]); err == nil {
			t.Fatalf("accepted length %d", n)
		}
	}
	if _, err := (Packet{Kind: Data, Session: 1, Payload: make([]byte, MaxPayload+1)}).Marshal(); err == nil {
		t.Fatal("oversize accepted")
	}
	if _, err := Parse(append(b, 0)); err == nil {
		t.Fatal("trailing byte accepted")
	}
}
func FuzzParse(f *testing.F) {
	b, _ := (Packet{Kind: Data, Session: 1, Payload: []byte("hello")}).Marshal()
	f.Add(b)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		p, err := Parse(b)
		if err != nil {
			return
		}
		encoded, err := p.Marshal()
		if err != nil || !bytes.Equal(encoded, b) {
			t.Fatal("noncanonical parse")
		}
	})
}
