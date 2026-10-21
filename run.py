"""Fair-queuing benchmark (WSGI).

Scenario
--------
A slow-ish WSGI app sits behind Granian. A handful of polite clients keep a modest,
closed-loop load on it, and one *spammer* client hammers it with a large number of
concurrent requests. The spammer must be its own flow, so it sends a distinct
``User-Agent`` (the default ``fq_inner_headers`` include ``user-agent``); the polite
clients each get their own too.

The point is to show that under FIFO the polite clients queue behind the spammer's
backlog, while under fair queuing they keep a latency close to the app's own service
time. ``official`` is the released Granian from PyPI (no scheduler support) and acts as
the reference for ``fifo``.

Metrics reported per config, for the polite clients and for the spammer separately:
request count, RPS, P50/P95/P99, mean, max and errors.

Usage
-----
    uv run python benchmarks/fair_queuing/run.py
    uv run python benchmarks/fair_queuing/run.py --configs official,fifo,fair --contention 45
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import httpx


BENCH_DIR = Path(__file__).resolve().parent
PYPI_JSON = 'https://pypi.org/pypi/granian/json'
OFFICIAL_VENV = Path('/tmp/opencode/fq-bench-official')


def granian_checkout() -> Path | None:
    """The Granian checkout this harness is sitting next to, if any (informational)."""
    for candidate in BENCH_DIR.parents:
        if (candidate / 'Cargo.toml').exists() and (candidate / 'src').is_dir():
            return candidate
    return None


# --------------------------------------------------------------------------- servers


def free_port() -> int:
    """Grab an ephemeral local port."""
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def latest_version() -> str:
    """Return the latest Granian version published on PyPI."""
    with urllib.request.urlopen(PYPI_JSON, timeout=30) as fh:
        return str(json.load(fh)['info']['version'])


def local_granian() -> list[str]:
    """The local (editable) Granian entrypoint, i.e. this working tree."""
    exe = Path(sys.executable).with_name('granian')
    return [str(exe)] if exe.exists() else ['uv', 'run', 'granian']


def ensure_official(version: str) -> list[str]:
    """Create (once) an isolated venv with the released Granian and return its binary."""
    exe = OFFICIAL_VENV / 'bin' / 'granian'
    if not exe.exists():
        py = f'{sys.version_info.major}.{sys.version_info.minor}'
        print(f'[bench] installing official granian=={version} into {OFFICIAL_VENV} (python {py})', file=sys.stderr)
        subprocess.run(['uv', 'venv', '--python', py, str(OFFICIAL_VENV)], check=True)
        subprocess.run(
            ['uv', 'pip', 'install', '--python', str(OFFICIAL_VENV / 'bin' / 'python'), f'granian=={version}'],
            check=True,
        )
    return [str(exe)]


@dataclass
class ServerSpec:
    """One Granian configuration under test."""

    name: str
    binary: list[str]
    scheduler: str | None
    blocking_threads: int
    backpressure: int
    delay_min_ms: int
    delay_max_ms: int
    version: str = ''

    def command(self, port: int) -> list[str]:
        """Full argv for this configuration."""
        cmd = [
            *self.binary,
            '--interface', 'wsgi',
            '--host', '127.0.0.1',
            '--port', str(port),
            '--workers', '1',
            '--blocking-threads', str(self.blocking_threads),
            '--backpressure', str(self.backpressure),
            '--http', '1',
            '--log-level', 'warning',
            '--working-dir', str(BENCH_DIR),
        ]
        if self.scheduler:
            cmd += ['--scheduler', self.scheduler]
        cmd.append('app:application')
        return cmd

    def env(self) -> dict[str, str]:
        """Environment for the server process, carrying the app delay."""
        env = dict(os.environ)
        env['BENCH_DELAY_MIN_MS'] = str(self.delay_min_ms)
        env['BENCH_DELAY_MAX_MS'] = str(self.delay_max_ms)
        return env


def server_version(spec: ServerSpec) -> str:
    """Best-effort version string for the report."""
    try:
        out = subprocess.run([*spec.binary, '--version'], capture_output=True, text=True, timeout=30)
        return (out.stdout or out.stderr).strip().split()[-1]
    except Exception:
        return '?'


def start_server(spec: ServerSpec, port: int, log_path: Path) -> subprocess.Popen:
    """Spawn the server in its own process group, logging to `log_path`."""
    log = log_path.open('w')
    return subprocess.Popen(
        spec.command(port),
        env=spec.env(),
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def wait_ready(port: int, proc: subprocess.Popen, timeout: float = 30.0) -> bool:
    """Wait until the server accepts TCP connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def stop_server(proc: subprocess.Popen) -> None:
    """Gracefully interrupt the server, escalating to SIGKILL."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=10)


# --------------------------------------------------------------------------- load


@dataclass
class Bucket:
    """Latency samples and error count for one group in one phase."""

    samples: list[float] = field(default_factory=list)
    errors: int = 0


class Session:
    """Carries the buckets workers record into, so phases (and the window end) can be split live."""

    def __init__(self) -> None:
        self.bucket = Bucket()
        self.spam_bucket = Bucket()


async def _hit(client: httpx.AsyncClient, url: str, user_agent: str, bucket: Bucket) -> None:
    """One request, timed and recorded into `bucket`."""
    start = time.perf_counter()
    try:
        res = await client.get(url, headers={'user-agent': user_agent})
        elapsed = time.perf_counter() - start
        if res.status_code == 200:
            bucket.samples.append(elapsed)
        else:
            bucket.errors += 1
    except Exception:
        bucket.errors += 1


async def polite_worker(session: Session, url: str, user_agent: str, stop: asyncio.Event, timeout: float) -> None:
    """Closed-loop client: one request at a time, recorded into the current phase bucket."""
    limits = httpx.Limits(max_connections=2, max_keepalive_connections=2)
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        while not stop.is_set():
            await _hit(client, url, user_agent, session.bucket)


async def spammer_worker(session: Session, url: str, user_agent: str, stop: asyncio.Event, timeout: float) -> None:
    """Hammering client: as many requests as it can keep in flight, from one flow."""
    limits = httpx.Limits(max_connections=256, max_keepalive_connections=256)
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        while not stop.is_set():
            await _hit(client, url, user_agent, session.spam_bucket)


async def measure(url: str, args: argparse.Namespace) -> tuple[Bucket, Bucket, float, Bucket]:
    """Run warmup, baseline and contention phases; return (baseline, contention, elapsed, spam)."""
    session = Session()

    polite_stop = asyncio.Event()
    polite = [
        asyncio.create_task(polite_worker(session, url, f'bench-polite-{idx}', polite_stop, args.timeout))
        for idx in range(args.polite)
    ]

    session.bucket = Bucket()
    await asyncio.sleep(args.warmup)

    baseline = Bucket()
    session.bucket = baseline
    await asyncio.sleep(args.baseline)

    contention = Bucket()
    session.bucket = contention
    session.spam_bucket = spam = Bucket()
    spam_stop = asyncio.Event()
    spammer = [
        asyncio.create_task(spammer_worker(session, url, 'bench-spammer', spam_stop, args.timeout))
        for _ in range(args.spammer)
    ]

    start = time.perf_counter()
    await asyncio.sleep(args.contention)
    elapsed = time.perf_counter() - start

    # Close the measurement window exactly here: stop both groups and make any still
    # in-flight request land in a throwaway bucket, so the reported RPS is not inflated
    # by the (possibly long) drain that follows.
    spam_stop.set()
    polite_stop.set()
    session.bucket = Bucket()
    session.spam_bucket = Bucket()

    await asyncio.gather(*spammer, return_exceptions=True)
    await asyncio.gather(*polite, return_exceptions=True)

    return baseline, contention, elapsed, spam


# --------------------------------------------------------------------------- report


def percentile(sorted_samples: list[float], pct: float) -> float:
    """Nearest-rank percentile."""
    if not sorted_samples:
        return math.nan
    rank = max(0, math.ceil(pct / 100 * len(sorted_samples)) - 1)
    return sorted_samples[rank]


def summarize(bucket: Bucket, duration: float) -> dict[str, float]:
    """Turn a bucket into the reported metrics."""
    samples = sorted(bucket.samples)
    count = len(samples)
    return {
        'n': count,
        'errors': bucket.errors,
        'rps': count / duration if duration else 0.0,
        'p50': percentile(samples, 50),
        'p95': percentile(samples, 95),
        'p99': percentile(samples, 99),
        'mean': sum(samples) / count if count else math.nan,
        'max': samples[-1] if count else math.nan,
    }


def fmt_ms(seconds: float) -> str:
    """Format a duration in seconds as milliseconds."""
    return 'n/a' if math.isnan(seconds) else f'{seconds * 1000:.0f}ms'


def print_group(label: str, stats: dict[str, float], errored: bool = True) -> None:
    """Print one metrics line."""
    line = (
        f'  {label:<9} n={stats["n"]:<5} rps={stats["rps"]:>7.2f}  '
        f'p50={fmt_ms(stats["p50"]):>8}  p95={fmt_ms(stats["p95"]):>8}  '
        f'p99={fmt_ms(stats["p99"]):>8}  mean={fmt_ms(stats["mean"]):>8}  max={fmt_ms(stats["max"]):>8}'
    )
    if errored:
        line += f'  errors={stats["errors"]:.0f}'
    print(line)


# --------------------------------------------------------------------------- main


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--configs', default='official,fifo,fair', help='comma separated: official,fifo,fair')
    parser.add_argument('--official-version', default=None, help='pin the released Granian version (default: latest)')
    parser.add_argument('--polite', type=int, default=6, help='number of polite clients')
    parser.add_argument('--spammer', type=int, default=64, help='concurrent requests from the spammer')
    parser.add_argument('--blocking-threads', type=int, default=8, help='WSGI worker pool size')
    parser.add_argument('--backpressure', type=int, default=2048, help='max concurrent connections per worker')
    parser.add_argument('--delay-min-ms', type=int, default=300, help='app service time, lower bound')
    parser.add_argument('--delay-max-ms', type=int, default=2000, help='app service time, upper bound')
    parser.add_argument('--warmup', type=float, default=4.0, help='polite-only warmup, discarded')
    parser.add_argument('--baseline', type=float, default=8.0, help='polite-only baseline phase')
    parser.add_argument('--contention', type=float, default=45.0, help='polite + spammer phase')
    parser.add_argument('--timeout', type=float, default=120.0, help='client request timeout')
    parser.add_argument('--workdir', type=Path, default=Path('/tmp/opencode/fq-bench'), help='server logs')
    return parser.parse_args()


def build_specs(args: argparse.Namespace) -> list[ServerSpec]:
    """Build the ServerSpec list matching `--configs`."""
    version = args.official_version or latest_version()
    common = dict(
        blocking_threads=args.blocking_threads,
        backpressure=args.backpressure,
        delay_min_ms=args.delay_min_ms,
        delay_max_ms=args.delay_max_ms,
    )
    specs: list[ServerSpec] = []
    for name in (item.strip() for item in args.configs.split(',') if item.strip()):
        if name == 'official':
            specs.append(ServerSpec('official', ensure_official(version), None, **common))
        elif name == 'fifo':
            specs.append(ServerSpec('fifo', local_granian(), 'fifo', **common))
        elif name == 'fair':
            specs.append(ServerSpec('fair', local_granian(), 'fair', **common))
        else:
            raise SystemExit(f'unknown config {name!r} (expected official, fifo or fair)')
    return specs


def run_config(spec: ServerSpec, args: argparse.Namespace) -> dict[str, dict[str, float]]:
    """Start the server, run the phases, stop it and return the stats."""
    args.workdir.mkdir(parents=True, exist_ok=True)
    log_path = args.workdir / f'server-{spec.name}.log'
    port = free_port()
    proc = start_server(spec, port, log_path)
    try:
        if not wait_ready(port, proc):
            print(f'!! {spec.name}: server failed to start, see {log_path}', file=sys.stderr)
            print(log_path.read_text()[-3000:], file=sys.stderr)
            raise SystemExit(1)
        baseline, contention, elapsed, spam = asyncio.run(measure(f'http://127.0.0.1:{port}/', args))
    finally:
        stop_server(proc)

    return {
        'baseline': summarize(baseline, args.baseline),
        'contention': summarize(contention, elapsed),
        'spammer': summarize(spam, elapsed),
    }


def main() -> None:
    """Run every requested configuration and print the reports."""
    args = parse_args()
    specs = build_specs(args)

    print(f'Granian fair-queuing benchmark (WSGI) - granian checkout: {granian_checkout() or "n/a"}')
    print(f'python {sys.version.split()[0]}')
    print(
        f'app service time {args.delay_min_ms}-{args.delay_max_ms}ms | '
        f'{args.polite} polite clients | spammer {args.spammer} concurrent | '
        f'blocking-threads {args.blocking_threads} | backpressure {args.backpressure}'
    )
    print(f'phases: warmup {args.warmup}s, baseline {args.baseline}s, contention {args.contention}s')

    results: dict[str, dict[str, dict[str, float]]] = {}
    versions: dict[str, str] = {}
    for spec in specs:
        spec.version = server_version(spec)
        print(f'\n=== {spec.name} (granian {spec.version}) ===', flush=True)
        print(f'scheduler: {spec.scheduler or "none (released build)"}', flush=True)
        results[spec.name] = run_config(spec, args)
        versions[spec.name] = spec.version

        print('  baseline (polite only):')
        print_group('polite', results[spec.name]['baseline'])
        print('  contention (polite + spammer):')
        print_group('polite', results[spec.name]['contention'])
        print_group('spammer', results[spec.name]['spammer'])

    print('\n=== summary (contention phase) ===')
    header = f'{"config":<10} {"granian":<9} {"polite p50":>11} {"polite p95":>11} {"polite rps":>11} {"errors":>7} {"spam rps":>9} {"spam p95":>9}'
    print(header)
    print('-' * len(header))
    for name, stats in results.items():
        polite, spam = stats['contention'], stats['spammer']
        print(
            f'{name:<10} {versions[name]:<9} {fmt_ms(polite["p50"]):>11} {fmt_ms(polite["p95"]):>11} '
            f'{polite["rps"]:>11.2f} {polite["errors"]:>7.0f} {spam["rps"]:>9.2f} {fmt_ms(spam["p95"]):>9}'
        )
    print('\npolite = everyone except the spammer; rps is the polite completion rate during contention.')


if __name__ == '__main__':
    main()
