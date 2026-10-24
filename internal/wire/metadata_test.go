package wire

import "testing"

func TestMetadataBounds(t *testing.T) {
	for _, size := range []uint64{0, 1, MaxPayload, MaxPayload + 1, MaxSize} {
		m := Metadata{Size: size, SHA256: [32]byte{1, 2, 3}}
		b, err := m.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		got, err := ParseMetadata(b)
		if err != nil || got != m {
			t.Fatalf("metadata %v %v", got, err)
		}
		if size > 0 {
			n := m.PayloadSize(m.Packets() - 1)
			if n < 1 || n > MaxPayload {
				t.Fatalf("last size %d", n)
			}
		}
		if m.PayloadSize(m.Packets()) != 0 {
			t.Fatal("out-of-range packet")
		}
	}
	if _, err := (Metadata{Size: MaxSize + 1}).Marshal(); err == nil {
		t.Fatal("oversize")
	}
	if _, err := ParseMetadata(make([]byte, 39)); err == nil {
		t.Fatal("short metadata")
	}
	for _, n := range []int{1, 32, 64} {
		b, _ := WindowPayload(n)
		got, err := ParseWindow(b)
		if err != nil || got != n {
			t.Fatal(n, got, err)
		}
	}
	for _, b := range [][]byte{nil, {0}, {65}, {1, 2}} {
		if _, err := ParseWindow(b); err == nil {
			t.Fatal("invalid window", b)
		}
	}
}
