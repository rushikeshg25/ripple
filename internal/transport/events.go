package transport

import "time"

type Stats struct {
	Bytes            uint64  `json:"bytes"`
	Sent             uint64  `json:"packets_sent"`
	Received         uint64  `json:"packets_received"`
	Retransmissions  uint64  `json:"retransmissions"`
	Invalid          uint64  `json:"invalid_packets"`
	ElapsedSeconds   float64 `json:"elapsed_seconds"`
	RTTMillis        float64 `json:"rtt_ms"`
	RTOMillis        float64 `json:"rto_ms"`
	CongestionWindow float64 `json:"cwnd_packets"`
	InFlight         int     `json:"in_flight"`
	PeakInFlight     int     `json:"peak_in_flight"`
	SHA256           string  `json:"sha256,omitempty"`
}
type Event struct {
	Time  time.Time `json:"time"`
	Type  string    `json:"event"`
	Seq   uint32    `json:"seq"`
	Stats Stats     `json:"stats"`
}
