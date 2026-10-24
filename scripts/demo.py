#!/usr/bin/env python3
"""Run real CLI processes on ephemeral loopback ports and verify their output."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import time


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def ready(process, path, prefix):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        text = path.read_text()
        for line in text.splitlines():
            if line.startswith(prefix):
                return line[len(prefix):].split()[0]
        if process.poll() is not None:
            raise RuntimeError(f"process exited before readiness: {text}")
        time.sleep(0.02)
    raise TimeoutError(f"no readiness message in {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="bin/ripple")
    parser.add_argument("--bytes", type=int, default=262144)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--loss", type=float, default=0.05)
    parser.add_argument("--delay", default="10ms")
    parser.add_argument("--jitter", default="5ms")
    parser.add_argument("--output", default="demo-output")
    args = parser.parse_args()
    if args.bytes < 65536 or args.bytes > 64 * 1024 * 1024:
        parser.error("--bytes must be between 65536 and 67108864")
    if not 0 <= args.loss <= 1:
        parser.error("--loss must be between 0 and 1")
    binary = str(Path(args.binary).resolve())
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix="run-", dir=root))
    source, target = run / "source.bin", run / "received.bin"
    source.write_bytes((bytes(range(256)) * ((args.bytes + 255) // 256))[:args.bytes])
    processes, handles = [], []

    def launch(name, command):
        stdout = (run / f"{name}.json").open("w")
        stderr = (run / f"{name}.stderr").open("w")
        handles.extend([stdout, stderr])
        process = subprocess.Popen([binary] + command, stdout=stdout, stderr=stderr)
        processes.append(process)
        return process

    try:
        receiver = launch("receive", ["receive", "-listen", "127.0.0.1:0",
            "-out", str(target), "-events", str(run / "receive.jsonl"), "-json"])
        receiver_addr = ready(receiver, run / "receive.stderr", "listening ")
        relay = launch("relay", ["relay", "-listen", "127.0.0.1:0", "-to", receiver_addr,
            "-loss", str(args.loss), "-delay", args.delay, "-jitter", args.jitter,
            "-duplicate", "0.02", "-seed", str(args.seed)])
        relay_addr = ready(relay, run / "relay.stderr", "relaying ")
        sender = launch("send", ["send", "-to", relay_addr, "-file", str(source),
            "-events", str(run / "send.jsonl"), "-json"])
        if sender.wait(timeout=120) != 0:
            raise RuntimeError((run / "send.stderr").read_text())
        if receiver.wait(timeout=20) != 0:
            raise RuntimeError((run / "receive.stderr").read_text())
        relay.terminate()
        if relay.wait(timeout=5) != 0:
            raise RuntimeError((run / "relay.stderr").read_text())
        sent = json.loads((run / "send.json").read_text())
        received = json.loads((run / "receive.json").read_text())
        relayed = json.loads((run / "relay.json").read_text())
        expected = digest(source)
        if not (expected == digest(target) == sent["sha256"] == received["sha256"]):
            raise AssertionError("SHA-256 mismatch")
        if sent["bytes"] != args.bytes or received["bytes"] != args.bytes:
            raise AssertionError("incorrect byte count")
        if args.loss > 0 and (relayed["dropped"] == 0 or sent["retransmissions"] == 0):
            raise AssertionError("requested loss/recovery was not observed")
        events = [json.loads(line) for line in (run / "send.jsonl").read_text().splitlines()]
        if not events or events[-1]["event"] != "complete":
            raise AssertionError("missing completion telemetry")
        # An existing destination must be preserved, even when another receive is attempted.
        refusal = subprocess.run([binary, "receive", "-out", str(target)],
            capture_output=True, text=True, timeout=5)
        if refusal.returncode == 0 or "already exists" not in refusal.stderr or digest(target) != expected:
            raise AssertionError("no-clobber contract failed")
        # Stop only the receiver created here and confirm temporary output is cleaned up.
        aborted = launch("abort", ["receive", "-listen", "127.0.0.1:0", "-out", str(run / "aborted.bin")])
        ready(aborted, run / "abort.stderr", "listening ")
        aborted.terminate()
        if aborted.wait(timeout=5) == 0 or (run / "aborted.bin").exists() or list(run.glob(".ripple-*.part")):
            raise AssertionError("interrupted receiver left output behind")
        report = {"bytes": args.bytes, "sha256": expected, "sender": sent,
            "receiver": received, "relay": relayed,
            "retransmitted_sequences": sorted({e["seq"] for e in events if e["event"] == "retransmit"}),
            "checks": ["matching hashes", "exact byte counts", "completion event", "no overwrite", "interrupt cleanup"]}
        (run / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"result": str(run / "result.json"), **report}, indent=2))
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for handle in handles:
            handle.close()


if __name__ == "__main__":
    main()
