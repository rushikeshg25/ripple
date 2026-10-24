// Package wire defines Ripple's versioned, bounded UDP datagrams.
package wire

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	HeaderSize  = 40
	MaxDatagram = 1200
	MaxPayload  = MaxDatagram - HeaderSize
	Version     = 1
)

type Kind uint8

const (
	Hello Kind = iota + 1
	HelloAck
	Data
	Ack
	Finish
	FinishAck
)

type Packet struct {
	Kind    Kind
	Session uint64
	Seq     uint32
	Ack     uint32 // next sequence needed; all smaller sequences are acknowledged
	SACK    uint64 // bit i acknowledges Ack+i
	Payload []byte
}

var ErrPacket = errors.New("invalid ripple datagram")

func (p Packet) Marshal() ([]byte, error) {
	if p.Kind < Hello || p.Kind > FinishAck || p.Session == 0 || len(p.Payload) > MaxPayload {
		return nil, ErrPacket
	}
	b := make([]byte, HeaderSize+len(p.Payload))
	copy(b, "RPL1")
	b[4], b[5] = Version, byte(p.Kind)
	binary.BigEndian.PutUint64(b[8:16], p.Session)
	binary.BigEndian.PutUint32(b[16:20], p.Seq)
	binary.BigEndian.PutUint32(b[20:24], p.Ack)
	binary.BigEndian.PutUint64(b[24:32], p.SACK)
	binary.BigEndian.PutUint16(b[32:34], uint16(len(p.Payload)))
	copy(b[HeaderSize:], p.Payload)
	binary.BigEndian.PutUint32(b[36:40], checksum(b))
	return b, nil
}

// Parse owns its payload: callers may immediately reuse their read buffer.
func Parse(b []byte) (Packet, error) {
	if len(b) < HeaderSize || len(b) > MaxDatagram || string(b[:4]) != "RPL1" || b[4] != Version || b[6] != 0 || b[7] != 0 || b[34] != 0 || b[35] != 0 {
		return Packet{}, ErrPacket
	}
	if int(binary.BigEndian.Uint16(b[32:34])) != len(b)-HeaderSize || binary.BigEndian.Uint32(b[36:40]) != checksum(b) {
		return Packet{}, ErrPacket
	}
	p := Packet{Kind: Kind(b[5]), Session: binary.BigEndian.Uint64(b[8:16]), Seq: binary.BigEndian.Uint32(b[16:20]), Ack: binary.BigEndian.Uint32(b[20:24]), SACK: binary.BigEndian.Uint64(b[24:32]), Payload: append([]byte(nil), b[HeaderSize:]...)}
	if p.Kind < Hello || p.Kind > FinishAck || p.Session == 0 {
		return Packet{}, ErrPacket
	}
	return p, nil
}

func checksum(b []byte) uint32 {
	h := crc32.NewIEEE()
	_, _ = h.Write(b[:36])
	_, _ = h.Write(b[HeaderSize:])
	return h.Sum32()
}
