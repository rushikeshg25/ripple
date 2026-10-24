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

## Milestone A — Wire contract and bounded state (commits 1–5)

**Delivered:** versioned packet codec, transfer metadata, receiver reassembly, and
sender sequence-window bookkeeping.

The protocol is fixed before socket orchestration so pure state tests can isolate
packet framing from timing. Headers use network byte order and CRC32; metadata
contains size and SHA-256. File names never cross the network. Both windows are
bounded by the 64-bit selective acknowledgement representation.

Acceptance criteria:

- Maximum-size datagrams round-trip without aliasing the caller's read buffer.
- Every single-byte corruption of a valid control packet is rejected.
- Truncated, oversized, unknown-version, and reserved-field inputs fail parsing.
- Empty files and the final short chunk have unambiguous sequence counts.
- Reordered chunks are written exactly once and only after gaps are filled.
- Selective ACKs cannot admit packets beyond the receiver's sequence interval.
- An ACK of unsent data cannot partially mutate sender state.

Evidence: `internal/wire/*_test.go`, `reassembly_test.go`, and `window_test.go`.

## Milestone B — Timers, congestion, and lifecycle (commits 6–10)

**Delivered:** adaptive timers, packet-count congestion control, validated runtime
configuration, cancellable UDP I/O, and complete sender/receiver state machines.

Keep one owner for each endpoint's mutable state. Start with one packet in flight,
grow under new ACKs, and reduce the allowance on timeout. Retransmitted packets do
not supply ambiguous RTT samples. Handshake and close exchanges retry separately.

Receiver completion is a transaction boundary: verify bytes, invoke the publication
callback, then send FinishAck. Duplicate close packets must not rerun publication.
A finite linger responds to final-ACK loss without keeping a receiver alive forever.

Acceptance criteria:

- RTT weights, variance, timer clamps, and exponential backoff have numeric tests.
- Karn's rule excludes retransmitted samples.
- Duplicate ACKs do not increase the congestion window.
- Both packet-count and no-progress bounds terminate silent-peer exchanges.
- Empty, one-byte, exact-chunk, and multi-window files pass over real UDP sockets.
- Receiver idle wait and transfer operations respond to context cancellation.
- Receiver publication occurs only after complete ordered bytes are available.

Evidence: `rto_test.go`, `congestion_test.go`, `config_test.go`, `send_test.go`,
and `receive_test.go`.

## Milestone C — Faults and filesystem contracts (commits 11–13)

**Delivered:** deterministic fault coverage, a configurable relay, source hashing,
and no-clobber destination publication.

Two different test mechanisms serve different purposes. An explicit packet schedule
drops the first packet of every protocol kind and injects reordering, duplicates,
wrong sessions, bad checksums, and invalid ACKs. The usable relay instead offers
probabilistic faults with a seed and a bounded delivery queue.

Filesystem publication uses a temporary file in the destination directory followed
by sync, close, and an exclusive hard link. Source hashing is a separate pass before
transfer; a second streaming hash detects changes between those passes.

Acceptance criteria:

- All six packet kinds can be lost without corrupting the completed transfer.
- Invalid input does not switch sessions, write duplicate bytes, or extend idle
  lifetime indefinitely.
- Publication failure cannot produce sender success.
- The real fault relay transfers correct content with observed loss and retries.
- The relay queue cannot grow beyond its configured limit.
- Pre-existing paths, dangling symlinks, and concurrent destination creators survive.
- Aborting a destination removes temporary output and never removes a final file.

Evidence: `reliability_test.go`, `internal/relay/relay_test.go`, and
`internal/fileio/files_test.go`.

## Milestone D — Usable commands and process proof (commits 14–17)

**Delivered:** send/receive/relay commands, live progress, JSON summaries, JSONL
packet events, and a standalone demonstration script.

CLI validation happens before network work. Defaults bind listeners to loopback.
SIGINT and SIGTERM cancel owned operations. Events are explicit optional files,
created exclusively; output failures surface as errors rather than disappearing.
The demo allocates ephemeral ports and manages only processes it started.

Acceptance criteria:

- Invalid windows, addresses, probabilities, and timing values return clear errors.
- Each command provides `-h`; unknown arguments do not silently change behavior.
- Final summary JSON and event JSONL can be parsed independently of stderr progress.
- Source truncation, growth, or changed content cannot publish a destination.
- An oversized Hello cannot claim the receiver before a valid smaller session.
- Incorrect final hashes are rejected even if all data packet CRCs are valid.
- IPv6 loopback works when available; unavailability is reported as a test skip.
- Separate sender, relay, and receiver processes complete the loss demonstration.
- The process demo verifies hash equality, no-overwrite behavior, and SIGTERM cleanup.

Evidence: `internal/cli/*_test.go`, `source_test.go`, `boundaries_test.go`, and
`scripts/demo.py`. The first 256 KiB demo completed with 29 relay drops and 18
sender retransmissions. These counts describe one run, not a performance target.

## Milestone E — Explain, automate, and publish (commits 18–21)

The final delivery includes the detailed [architecture](ARCHITECTURE.md), README
architecture diagram, local Make targets, an Ubuntu/macOS CI workflow, and a
[verification record](VERIFICATION.md). Publish the reviewed source to the public
`rushikeshg25/ripple` repository with the complete incremental history.

Acceptance criteria:

- Docs explain wire fields, ACK examples, state ownership, timer formulas, memory
  bounds, completion ambiguity, configuration defaults, and unsupported features.
- README commands match the actual CLI and do not require undocumented services.
- `make check`, `make build`, `make fuzz`, and `make demo` have recorded results.
- CI performs formatting checks, vet, race tests, and the demo on both platforms.
- The repository contains source and docs, with no binaries, demo files, or local
  session logs committed.
- GitHub visibility is public, the remote main commit matches local HEAD, and the
  delivered history contains 21 commits. Remote CI status is reported separately
  from local verification.

## Future work: separate proposals, not unfinished v1 requirements

1. **Pacing and fast retransmit:** first add tests that distinguish reordering from
   genuine loss. Measure burst behavior before changing congestion algorithms.
2. **Authenticated transport:** use a reviewed protocol/library; avoid inventing
   cryptographic primitives. Threat-model the unauthenticated initial handshake.
3. **Resume:** design persistent transfer identity and verified chunk manifests.
   A partial file alone is insufficient proof that bytes belong to this session.
4. **NAT traversal:** introduce it as a separate experiment with explicit network
   topologies and failure reporting; do not assume every NAT permits hole punching.
5. **Multiplexing:** define per-stream flow control and session-wide congestion
   accounting before adding multiple simultaneous files.

None of these is required for the bounded learning outcome. There are no recurring
hosting costs to shut down: stop the local processes and keep source, tests, and
optional demo evidence. Generated data belongs to the user and is not auto-deleted.
