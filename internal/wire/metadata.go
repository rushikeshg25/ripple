package wire

import (
	"encoding/binary"
	"fmt"
	"math"
)

const MaxWindow = 64

// MaxSize leaves sequence space for the cumulative acknowledgement of all data.
const MaxSize = uint64(math.MaxUint32) * MaxPayload

type Metadata struct {
	Size   uint64
	SHA256 [32]byte
}

func (m Metadata) Marshal() ([]byte, error) {
	if m.Size > MaxSize {
		return nil, fmt.Errorf("file exceeds protocol size limit")
	}
	b := make([]byte, 40)
	binary.BigEndian.PutUint64(b, m.Size)
	copy(b[8:], m.SHA256[:])
	return b, nil
}
func ParseMetadata(b []byte) (Metadata, error) {
	if len(b) != 40 {
		return Metadata{}, ErrPacket
	}
	m := Metadata{Size: binary.BigEndian.Uint64(b)}
	copy(m.SHA256[:], b[8:])
	if m.Size > MaxSize {
		return Metadata{}, ErrPacket
	}
	return m, nil
}
func (m Metadata) Packets() uint32 { return uint32((m.Size + MaxPayload - 1) / MaxPayload) }
func (m Metadata) PayloadSize(seq uint32) int {
	if seq >= m.Packets() {
		return 0
	}
	left := m.Size - uint64(seq)*MaxPayload
	if left > MaxPayload {
		return MaxPayload
	}
	return int(left)
}
func WindowPayload(window int) ([]byte, error) {
	if window < 1 || window > MaxWindow {
		return nil, fmt.Errorf("window must be 1..64")
	}
	return []byte{byte(window)}, nil
}
func ParseWindow(b []byte) (int, error) {
	if len(b) != 1 || b[0] < 1 || b[0] > MaxWindow {
		return 0, ErrPacket
	}
	return int(b[0]), nil
}
