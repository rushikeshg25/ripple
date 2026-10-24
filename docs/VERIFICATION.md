# Verification record

Recorded on **2026-10-24**, using Go **1.24.6**, macOS (`darwin/arm64`), and
Python **3.14**. The implementation tested here is the source through commit
`f6cd185`; the final documentation commit does not change executable code.

## Local checks

| Check | Result | What it establishes |
| --- | --- | --- |
| `make check` | Passed | Formatting, `go vet ./...`, and `go test -race -count=1 ./...` |
| `make build` | Passed | Standalone CLI builds with `-trimpath` |
| `make fuzz` | Passed | 526,519 packet-parser executions in the recorded five-second fuzz run, two workers |
| `make demo` | Passed | Real CLI processes, fault relay, matching hashes, telemetry, no-overwrite behavior, and interruption cleanup |
| `python3 scripts/demo.py --bytes 1048576 --loss 0.15 --seed 13` | Passed | Larger file and higher-loss recovery through the real CLI |
| `go test ./internal/transport -run TestIPv6Loopback -v` | Passed, not skipped | IPv6 UDP loopback transfer on the local host |
| Python AST syntax check | Passed | Demo script parses successfully |
| `git diff --check` | Passed | No whitespace errors in the reviewed changes |

The command entry point has no separate unit tests; CLI behavior is covered in
`internal/cli` and by the separate-process demo. Race tests also cover the actual
transport and relay over loopback sockets. A short fuzz run is a bounded parser
check, not exhaustive protocol verification.

## Recorded process demonstrations

Both scenarios used a base one-way delay of 10 ms, jitter of ±5 ms, and 2% duplicate
probability. Loss applies independently to packets in both directions. Corruption
was disabled in these process demonstrations; it is exercised in the automated
transport and relay tests.

| Measurement | Default demo | Higher-loss demo |
| --- | --- | --- |
| Input bytes | 262,144 | 1,048,576 |
| Configured loss probability | 5% | 15% |
| Seed | 7 | 13 |
| Relay datagrams received | 479 | 1,995 |
| Relay datagrams dropped | 29 | 291 |
| Additional duplicate copies | 10 | 49 |
| Relay queue overflow drops | 0 | 0 |
| Sender retransmissions | 18 | 152 |
| Peak sender packets in flight | 13 | 17 |
| Sender elapsed time | 1.307 s | 7.497 s |
| Receiver elapsed time | 7.337 s | 13.525 s |
| File comparison | Matching SHA-256 | Matching SHA-256 |

Default input SHA-256:

```text
2312394bd99545d9de131c24efb781e765ac1aec243f2ed9347597a793a415e9
```

Higher-loss input SHA-256:

```text
fbbab289f7f94b25736c58be46a994c441fd02552cc6022352e3d86d2fab7c83
```

These are observed local runs, not throughput promises or a benchmark against TCP.
The receiver's elapsed time includes its six-second completion linger. A seeded
relay is reproducible at the random-choice level for a fixed arrival sequence;
actual socket scheduling changes packet arrival order and can change retry counts.
Generated files and raw JSONL records remain under local `demo-output/` directories
and are excluded from the repository. CI uploads its own demo diagnostics.

## Important failure contracts exercised

- The deterministic fault test drops the first Hello, HelloAck, Data, Ack, Finish,
  and FinishAck, while also duplicating and reordering data.
- Corrupted packets, wrong-session packets, and ACKs of unsent data do not corrupt
  the final content.
- Duplicate Hello traffic cannot keep an idle receiver alive indefinitely.
- A receiver publication error cannot result in sender success.
- Source truncation, growth, or changed content prevents publication.
- The receiver rejects an incorrect final hash even when packet framing is valid.
- An oversized Hello does not pin a receiver and block a subsequent valid session.
- Existing files, concurrent destination creations, and symlinks are not overwritten.
- SIGTERM of the demo receiver cleans up its unpublished temporary file.
- Telemetry write failures cancel the operation and surface an error.

The first combined-fault test initially expected one retransmission for every
dropped packet kind. Runtime evidence showed that a duplicate data packet can
produce a replacement ACK without another retry. The test was corrected to assert
all six injected drops directly and retain a minimum retry count for the remaining
losses. The protocol did not need a behavioral change.

## Remote CI and scope limits

The [Verify workflow](../.github/workflows/ci.yml) runs formatting, vet, race tests,
and the process demonstration on Ubuntu and macOS. It also fuzzes the parser on
Ubuntu. This record captures local checks before publication; inspect the
[GitHub Actions runs](https://github.com/rushikeshg25/ripple/actions) for the status
of a particular remote commit.

No physical WAN, NAT traversal, Windows runtime, power-loss recovery, or production
congestion fairness was tested. Encryption, peer authentication, resumption,
multiplexing, and fast retransmit are outside v1. Internal timing configurations
are not negotiated; the shipped CLI uses matching defaults. The receiver can
publish successfully while the sender times out if all completion replies are lost.
These boundaries are detailed in [the architecture](ARCHITECTURE.md).
