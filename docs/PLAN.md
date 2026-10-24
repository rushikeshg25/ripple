# Ripple delivery plan

## Outcome and boundaries

Transfer a file byte-for-byte through a local relay that drops 5% of UDP datagrams.
Verify SHA-256 and expose retransmissions, RTT, congestion window and throughput.
The implementation is a learning transport, not a production replacement for TCP
or QUIC. Use Go's standard library, two endpoints, and an optional local relay.
No cloud infrastructure, monthly hosting bill, service discovery, or database.

One sender, one receiver, one file per session. Start with IPv4/IPv6 UDP addresses
accepted by Go. Bound datagrams to 1200 bytes, in-flight packets to 64, and receiver
memory to that window. Accept cancellation and bounded stalls at every network
phase. A receiver must not overwrite an existing file or publish partial content.

## Intended commit sequence

Each numbered item is a separately reviewable implementation increment.

1. Scaffold the Go module, license, and delivery plan.
2. Add the bounded binary wire codec and corruption checks.
3. Encode transfer metadata and handshake negotiation.
4. Implement bounded receiver reassembly and selective ACKs.
5. Implement sender window tracking and ACK validation.
6. Add adaptive retransmission timers and exponential backoff.
7. Add slow start and additive-increase congestion control.
8. Define transport configuration, events, and cancellable datagram I/O.
9. Implement sender handshake, streaming, recovery, and finish exchange.
10. Implement receiver session lifecycle and verified completion.
11. Exercise loss, duplication, reordering, and lost close acknowledgements.
12. Build a seeded bidirectional fault relay with bounded delay queues.
13. Add source hashing and atomic, no-clobber destination publication.
14. Add send/receive commands with strict argument validation.
15. Expose relay configuration through the CLI.
16. Add live progress and JSONL event recording.
17. Add a reproducible end-to-end fault demonstration.
18. Document architecture, protocol invariants, and failure behavior.
19. Complete README diagrams, examples, and the implementation plan.
20. Add local verification commands and CI checks.
21. Record release verification, measured behavior, and remaining limits.

## Verification strategy

- Unit tests: packet parsing, metadata bounds, reorder windows, ACK validity,
  Karn's rule, RTO clamps/backoff, and congestion-window transitions.
- Loopback integration: empty and multi-window files, malformed/foreign traffic,
  dropped handshake/data/ACK/finish packets, duplicates, and reordered delivery.
- Filesystem contracts: no-clobber publication, cleanup on failure, and source
  mutation detection. Successful receiver acknowledgement follows publication.
- CLI: argument failures, real separate-process transfer through the fault relay,
  hashes, telemetry, and interruption. Race detector, go vet, parser fuzz smoke.
- CI: Go 1.24 on Ubuntu and macOS; builds and tests require no external services.

## Learning scope and exit criteria

Success is a repeatable local transfer under 5% loss, with a matching hash and
visible recovery, delivered in this implementation session. Keep the design
local and dependency-free. Stop expanding scope once this outcome and the listed
checks pass. If the transport cannot converge under the bounded test model,
retain the failing reproduction and fix the protocol before adding features.

The simpler production alternative is TCP or QUIC. Implementing reliability is
the learning goal, so replacing it with those transports would defeat that goal.
Deferred: encryption/authentication, NAT traversal, multiplexed streams, resumable
transfers, PMTU discovery, fast retransmit, and production congestion fairness.
