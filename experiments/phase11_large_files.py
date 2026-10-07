"""Phase 11 experiment: large files, measured.

    uv run python -m experiments.phase11_large_files [--size-mb 1024]

Runs against the compose stack (`docker compose --profile app up -d`).

Part 1 — upload the same text file through both endpoints:
  multipart  POST /files         (Starlette spools the body to a temp file first)
  stream     POST /files/stream  (raw body, socket -> hash -> MinIO)
and measure: wall time, throughput, API container peak memory (cgroup
memory.peak — includes the kernel page cache!) vs the process' own peak RSS
(VmHWM), peak temp-file disk use of the API (spooled multipart parts are
unlinked files, found through /proc/1/fd), and latency of GET /health/live
polled every 20 ms during the upload (is the event loop blocked?).
Then the worker processes the file: processing time and worker peak memory.

Part 2 — analysis in this process, no network: whole file in memory vs
streaming with different chunk sizes (peak RSS, MB/s).
"""

import argparse
import asyncio
import random
import statistics
import string
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

API = "http://localhost:8000"
MIB = 1024 * 1024


def sh(*args: str) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout.strip()


def compose(*args: str) -> str:
    return sh("docker", "compose", "--profile", "app", *args)


def peak_memory_mib(service: str) -> float:
    return int(compose("exec", "-T", service, "cat", "/sys/fs/cgroup/memory.peak")) / MIB


def peak_rss_mib(service: str) -> float:
    """Peak resident memory of the service's main process (excludes page cache)."""
    status = compose("exec", "-T", service, "cat", "/proc/1/status")
    kib = next(int(line.split()[1]) for line in status.splitlines() if line.startswith("VmHWM"))
    return kib / 1024


def spooled_bytes() -> int:
    """Total size of deleted-but-open files of the API process (= spooled upload)."""
    out = compose(
        "exec",
        "-T",
        "api",
        "sh",
        "-c",
        "for f in /proc/1/fd/*; do"
        " case $(readlink $f) in *'(deleted)'*) stat -L -c %s $f;; esac;"
        " done",
    )
    return sum(int(x) for x in out.split()) if out else 0


def make_text_file(size_mb: int) -> Path:
    rng = random.Random(42)
    words = ["".join(rng.choices(string.ascii_lowercase, k=rng.randint(2, 9))) for _ in range(5000)]
    block = bytearray()
    while len(block) < MIB:
        block += (" ".join(rng.choices(words, k=12)) + "\n").encode()
    block = bytes(block[: MIB - 1]) + b"\n"
    path = Path(tempfile.gettempdir()) / f"fileflow-{size_mb}mb.txt"
    if not path.exists() or path.stat().st_size != size_mb * MIB:
        with path.open("wb") as f:
            for _ in range(size_mb):
                f.write(block)
    return path


async def restart(service: str) -> None:
    await asyncio.to_thread(compose, "restart", service)  # also resets memory.peak
    if service == "api":
        async with httpx.AsyncClient() as client:
            for _ in range(100):
                try:
                    if (await client.get(f"{API}/health/live")).status_code == 200:
                        return
                except httpx.TransportError:
                    pass
                await asyncio.sleep(0.2)
        raise RuntimeError("api did not come back")


async def probe_liveness(stop: asyncio.Event, latencies: list[float]) -> None:
    async with httpx.AsyncClient() as client:
        while not stop.is_set():
            started = time.perf_counter()
            await client.get(f"{API}/health/live")
            latencies.append((time.perf_counter() - started) * 1000)
            await asyncio.sleep(0.02)


async def sample_spool(stop: asyncio.Event, peaks: list[int]) -> None:
    while not stop.is_set():
        peaks.append(await asyncio.to_thread(spooled_bytes))
        await asyncio.sleep(0.5)


async def file_chunks(path: Path, size: int = MIB):
    with path.open("rb") as f:
        while chunk := await asyncio.to_thread(f.read, size):
            yield chunk


async def upload(variant: str, path: Path, size_mb: int) -> dict:
    await restart("api")
    stop, latencies, spool = asyncio.Event(), [], [0]
    background = [
        asyncio.create_task(probe_liveness(stop, latencies)),
        asyncio.create_task(sample_spool(stop, spool)),
    ]
    async with httpx.AsyncClient(timeout=None) as client:
        started = time.perf_counter()
        if variant == "multipart":
            with path.open("rb") as f:
                resp = await client.post(
                    f"{API}/files", files={"file": (path.name, f, "text/plain")}
                )
        else:
            resp = await client.post(
                f"{API}/files/stream",
                params={"filename": path.name},
                content=file_chunks(path),
                headers={"Content-Type": "text/plain"},
            )
        elapsed = time.perf_counter() - started
    stop.set()
    await asyncio.gather(*background)
    resp.raise_for_status()
    return {
        "variant": variant,
        "task_id": resp.json()["task"]["id"],
        "seconds": elapsed,
        "mib_s": size_mb / elapsed,
        "api_peak_mib": peak_memory_mib("api"),
        "api_rss_mib": peak_rss_mib("api"),
        "spool_peak_mib": max(spool) / MIB,
        "live_p50": statistics.median(latencies),
        "live_p99": statistics.quantiles(latencies, n=100)[98],
        "live_max": max(latencies),
    }


async def process(task_id: str) -> dict:
    await asyncio.to_thread(compose, "up", "-d", "--no-deps", "worker")
    async with httpx.AsyncClient() as client:
        while True:
            task = (await client.get(f"{API}/tasks/{task_id}")).json()
            if task["status"] in ("COMPLETED", "FAILED"):
                break
            await asyncio.sleep(0.5)
    peak = peak_memory_mib("worker")
    await asyncio.to_thread(compose, "stop", "worker")
    return {
        "status": task["status"],
        "processing_ms": (task["result"] or {}).get("processing_ms"),
        "worker_peak_mib": peak,
    }


ANALYSIS = """
import asyncio, resource, sys, time
from app.processing import analyze
path, chunk = sys.argv[1], int(sys.argv[2])
async def chunks():
    with open(path, "rb") as f:
        if chunk == 0:
            yield f.read()          # the anti-pattern: the whole file at once
            return
        while c := f.read(chunk):
            yield c
t = time.perf_counter()
asyncio.run(analyze(chunks()))
print(time.perf_counter() - t, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
"""


def analysis_runs(path: Path) -> list[tuple[str, float, float]]:
    rows = []
    for label, chunk in [
        ("whole file", 0),
        ("64 KiB", 64 * 1024),
        ("1 MiB", MIB),
        ("8 MiB", 8 * MIB),
        ("32 MiB", 32 * MIB),
    ]:
        out = sh(sys.executable, "-c", ANALYSIS, str(path), str(chunk))
        seconds, rss = map(float, out.split())
        rows.append((label, path.stat().st_size / MIB / seconds, rss))
    return rows


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size-mb", type=int, default=1024)
    args = parser.parse_args()

    path = make_text_file(args.size_mb)
    print(f"file: {path} ({args.size_mb} MiB)\n")
    await asyncio.to_thread(compose, "up", "-d", "--build", "--wait", "api")
    await asyncio.to_thread(compose, "up", "-d", "outbox-publisher", "recovery")
    await asyncio.to_thread(compose, "stop", "worker")  # keep uploads free of worker CPU

    await restart("api")
    idle: list[float] = []
    stop = asyncio.Event()
    prober = asyncio.create_task(probe_liveness(stop, idle))
    await asyncio.sleep(10)
    stop.set()
    await prober
    print(
        f"/health/live while idle: p50 {statistics.median(idle):.1f} ms, "
        f"p99 {statistics.quantiles(idle, n=100)[98]:.1f} ms, max {max(idle):.1f} ms\n"
    )
    print(
        f"{'variant':10} {'time s':>7} {'MiB/s':>7} {'cgroup MiB':>11} {'RSS MiB':>8} "
        f"{'spool MiB':>10} {'live p50':>9} {'p99':>7} {'max ms':>7} | {'proc ms':>8} "
        f"{'worker MiB':>11}"
    )
    for variant in ("multipart", "stream"):
        u = await upload(variant, path, args.size_mb)
        p = await process(u["task_id"])
        print(
            f"{variant:10} {u['seconds']:7.1f} {u['mib_s']:7.0f} {u['api_peak_mib']:11.0f} "
            f"{u['api_rss_mib']:8.0f} {u['spool_peak_mib']:10.0f} {u['live_p50']:9.1f} "
            f"{u['live_p99']:7.1f} {u['live_max']:7.1f} | {p['processing_ms']:8} "
            f"{p['worker_peak_mib']:11.0f}"
        )

    print("\nanalysis of the same file in one process (no network):")
    print(f"{'read mode':12} {'MiB/s':>7} {'peak RSS MiB':>13}")
    for label, mib_s, rss in analysis_runs(path):
        print(f"{label:12} {mib_s:7.0f} {rss:13.0f}")

    await asyncio.to_thread(compose, "up", "-d", "worker")


if __name__ == "__main__":
    asyncio.run(main())
