"""Fair-queuing benchmark (WSGI, ASGI and RSGI).

Scenario
--------
A slow-ish application sits behind Granian. A handful of polite clients keep a modest,
closed-loop load on it, and one *spammer* client hammers it with a large number of
concurrent requests. The spammer must be its own flow, so it sends a distinct
``User-Agent`` (the default ``fq_inner_headers`` include ``user-agent``); every polite
client gets its own too.

* ``--interface wsgi``: the app is a plain WSGI view that blocks its worker thread for a
  random 300..2000 ms. The lane is a thread from ``--blocking-threads``.
* ``--interface asgi`` / ``rsgi``: the app burns a random mix of CPU and ``asyncio.sleep`` awaits
  (the same workload on both).
  The lane is the event loop, so ``--blocking-threads`` is forced to 1 and parallelism
  comes from ``--workers``. On ASGI/RSGI a WebSocket echo endpoint is also served, and a probe
  measures its round-trip latency, split into a *before load* and an *under load* window.

The point is to show that under FIFO the polite clients (and the WebSocket echo) queue
behind the spammer's backlog, while under fair queuing they keep a latency close to what
they get when nothing is hammering. ``official`` is the released Granian from PyPI (no
scheduler support) and acts as the reference for ``fifo``.

Usage
-----
    uv run python benchmarks/fair_queuing/run.py
    uv run python benchmarks/fair_queuing/run.py --interface asgi --contention 60
    uv run python benchmarks/fair_queuing/run.py --interface rsgi --contention 60
    uv run python benchmarks/fair_queuing/run.py --configs fifo,fair --spammer 128
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
import websockets


BENCH_DIR = Path(__file__).resolve().parent
PYPI_JSON = 'https://pypi.org/pypi/granian/json'
OFFICIAL_VENV = Path('/tmp/opencode/fq-bench-official')
#: Granian target per interface.
APP_TARGETS = {'wsgi': 'app:application', 'asgi': 'asgi_app:application', 'rsgi': 'rsgi_app:application'}


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
    interface: str
    binary: list[str]
    scheduler: str | None
    workers: int
    blocking_threads: int
    backpressure: int
    delay_min_ms: int
    delay_max_ms: int
    cpu_share: float
    max_slice_ms: int
    version: str = ''

    def command(self, port: int) -> list[str]:
        """Full argv for this configuration."""
        # ASGI/RSGI only support a single blocking thread: their lane is the event loop.
        blocking_threads = self.blocking_threads if self.interface == 'wsgi' else 1
        cmd = [
            *self.binary,
            '--interface',
            self.interface,
            '--host',
            '127.0.0.1',
            '--port',
            str(port),
            '--workers',
            str(self.workers),
            '--blocking-threads',
            str(blocking_threads),
            '--backpressure',
            str(self.backpressure),
            '--http',
            '1',
            '--log-level',
            'warning',
            '--working-dir',
            str(BENCH_DIR),
        ]
        if self.scheduler:
            cmd += ['--scheduler', self.scheduler]
        cmd.append(APP_TARGETS[self.interface])
        return cmd

    def env(self) -> dict[str, str]:
        """Environment for the server process, carrying the app knobs."""
        env = dict(os.environ)
        env['BENCH_DELAY_MIN_MS'] = str(self.delay_min_ms)
        env['BENCH_DELAY_MAX_MS'] = str(self.delay_max_ms)
        env['BENCH_CPU_SHARE'] = str(self.cpu_share)
        env['BENCH_MAX_SLICE_MS'] = str(self.max_slice_ms)
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
        self.ws_bucket = Bucket()


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


async def ws_probe(session: Session, url: str, user_agent: str, stop: asyncio.Event, args: argparse.Namespace) -> None:
    """Echo round-trip probe: send a token, wait for it back, time it, then idle a bit.

    The round trip goes through the Python application, so its latency reflects how
    responsive the event loop is. Samples land in whichever bucket the current phase
    installed, which is how the same probe is split into "before load" and "under load".
    The think time keeps the probe a probe: without it, an unloaded loop echoes in tens of
    microseconds and the probe becomes a second spammer in its own right.
    """
    headers = {'user-agent': user_agent}
    try:
        async with websockets.connect(
            url, additional_headers=headers, ping_interval=None, open_timeout=args.timeout, close_timeout=5
        ) as ws:
            counter = 0
            while not stop.is_set():
                counter += 1
                token = f'ping-{counter}'
                bucket = session.ws_bucket
                start = time.perf_counter()
                try:
                    await ws.send(token)
                    reply = await asyncio.wait_for(ws.recv(), timeout=args.timeout)
                except Exception:
                    bucket.errors += 1
                else:
                    if reply == token:
                        bucket.samples.append(time.perf_counter() - start)
                    else:
                        bucket.errors += 1
                await asyncio.sleep(args.ws_interval)
    except Exception as exc:
        print(f'[bench] websocket probe failed: {exc!r}', file=sys.stderr)


@dataclass
class Run:
    """Everything one configuration measured."""

    baseline: Bucket
    contention: Bucket
    elapsed: float
    spam: Bucket
    ws_before: Bucket
    ws_load: Bucket
    ws_before_seconds: float


async def measure(url: str, ws_url: str | None, args: argparse.Namespace) -> Run:
    """Run warmup, baseline and contention phases, then close the measurement windows."""
    session = Session()

    polite_stop = asyncio.Event()
    polite = [
        asyncio.create_task(polite_worker(session, url, f'bench-polite-{idx}', polite_stop, args.timeout))
        for idx in range(args.polite)
    ]

    ws_stop = asyncio.Event()
    ws_before, ws_load = Bucket(), Bucket()
    ws_before_seconds = args.warmup + args.baseline
    ws_tasks: list[asyncio.Task] = []
    if ws_url:
        ws_tasks = [
            asyncio.create_task(ws_probe(session, ws_url, f'bench-ws-{idx}', ws_stop, args))
            for idx in range(args.ws_clients)
        ]
        # Let the probe connect, then start recording: "before load" spans warmup + baseline.
        await asyncio.sleep(0.5)
        session.ws_bucket = ws_before

    # Warmup is discarded; the baseline tells us what the polite clients get when nothing
    # is hammering the server.
    session.bucket = Bucket()
    await asyncio.sleep(args.warmup)

    baseline = Bucket()
    session.bucket = baseline
    await asyncio.sleep(args.baseline)

    contention = Bucket()
    session.bucket = contention
    session.spam_bucket = spam = Bucket()
    session.ws_bucket = ws_load
    spam_stop = asyncio.Event()
    spammer = [
        asyncio.create_task(spammer_worker(session, url, 'bench-spammer', spam_stop, args.timeout))
        for _ in range(args.spammer)
    ]

    start = time.perf_counter()
    await asyncio.sleep(args.contention)
    elapsed = time.perf_counter() - start

    # Close the measurement windows exactly here: stop every group and make any still
    # in-flight request land in a throwaway bucket, so the reported RPS is not inflated by
    # the (possibly long) drain that follows.
    spam_stop.set()
    polite_stop.set()
    ws_stop.set()
    session.bucket = Bucket()
    session.spam_bucket = Bucket()
    session.ws_bucket = Bucket()

    await asyncio.gather(*spammer, return_exceptions=True)
    await asyncio.gather(*polite, return_exceptions=True)
    if ws_tasks:
        await asyncio.gather(*ws_tasks, return_exceptions=True)

    return Run(baseline, contention, elapsed, spam, ws_before, ws_load, ws_before_seconds)


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
    """Adaptive duration formatting, so sub-millisecond probes do not read as "0ms"."""
    if math.isnan(seconds):
        return 'n/a'
    if seconds < 1e-3:
        return f'{seconds * 1e6:.0f}us'
    if seconds < 1:
        return f'{seconds * 1e3:.1f}ms'
    return f'{seconds:.2f}s'


def print_group(label: str, stats: dict[str, float], with_rps: bool = True) -> None:
    """Print one metrics line."""
    line = f'  {label:<12} n={stats["n"]:<5}'
    if with_rps:
        line += f' rps={stats["rps"]:>7.2f}'
    line += (
        f'  p50={fmt_ms(stats["p50"]):>8}  p95={fmt_ms(stats["p95"]):>8}  '
        f'p99={fmt_ms(stats["p99"]):>8}  mean={fmt_ms(stats["mean"]):>8}  max={fmt_ms(stats["max"]):>8}'
        f'  errors={stats["errors"]:.0f}'
    )
    print(line)


# --------------------------------------------------------------------------- main


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--interface', choices=tuple(APP_TARGETS), default='wsgi')
    parser.add_argument('--configs', default='official,fifo,fair', help='comma separated: official,fifo,fair')
    parser.add_argument('--official-version', default=None, help='pin the released Granian version (default: latest)')
    parser.add_argument(
        '--polite', type=int, default=None, help='number of polite clients (default: 6 wsgi / 3 asgi and rsgi)'
    )
    parser.add_argument('--spammer', type=int, default=64, help='concurrent requests from the spammer')
    parser.add_argument('--workers', type=int, default=1, help='Granian workers')
    parser.add_argument('--blocking-threads', type=int, default=8, help='WSGI worker pool size (ignored on ASGI/RSGI)')
    parser.add_argument('--backpressure', type=int, default=2048, help='max concurrent connections per worker')
    parser.add_argument('--delay-min-ms', type=int, default=300, help='app service time, lower bound')
    parser.add_argument('--delay-max-ms', type=int, default=2000, help='app service time, upper bound')
    parser.add_argument('--cpu-share', type=float, default=0.2, help='ASGI/RSGI: probability a work slice is CPU')
    parser.add_argument('--max-slice-ms', type=int, default=50, help='ASGI/RSGI: longest single CPU/await slice')
    parser.add_argument('--ws-clients', type=int, default=1, help='ASGI/RSGI: parallel WebSocket echo probes')
    parser.add_argument('--ws-interval', type=float, default=0.05, help='ASGI/RSGI: idle time between WS echo probes')
    parser.add_argument('--warmup', type=float, default=4.0, help='polite-only warmup, discarded')
    parser.add_argument('--baseline', type=float, default=8.0, help='polite-only baseline phase')
    parser.add_argument('--contention', type=float, default=45.0, help='polite + spammer phase')
    parser.add_argument('--timeout', type=float, default=120.0, help='client request timeout')
    parser.add_argument('--workdir', type=Path, default=Path('/tmp/opencode/fq-bench'), help='server logs')
    args = parser.parse_args()
    if args.polite is None:
        args.polite = 6 if args.interface == 'wsgi' else 3
    return args


def build_specs(args: argparse.Namespace) -> list[ServerSpec]:
    """Build the ServerSpec list matching `--configs`."""
    version = args.official_version or latest_version()
    common = {
        'interface': args.interface,
        'workers': args.workers,
        'blocking_threads': args.blocking_threads,
        'backpressure': args.backpressure,
        'delay_min_ms': args.delay_min_ms,
        'delay_max_ms': args.delay_max_ms,
        'cpu_share': args.cpu_share,
        'max_slice_ms': args.max_slice_ms,
    }
    specs: list[ServerSpec] = []
    for name in (item.strip() for item in args.configs.split(',') if item.strip()):
        if name == 'official':
            specs.append(ServerSpec(name='official', binary=ensure_official(version), scheduler=None, **common))
        elif name == 'fifo':
            specs.append(ServerSpec(name='fifo', binary=local_granian(), scheduler='fifo', **common))
        elif name == 'fair':
            specs.append(ServerSpec(name='fair', binary=local_granian(), scheduler='fair', **common))
        else:
            raise SystemExit(f'unknown config {name!r} (expected official, fifo or fair)')
    return specs


def run_config(spec: ServerSpec, args: argparse.Namespace) -> dict[str, dict[str, float]]:
    """Start the server, run the phases, stop it and return the stats."""
    args.workdir.mkdir(parents=True, exist_ok=True)
    log_path = args.workdir / f'server-{spec.name}-{spec.interface}.log'
    port = free_port()
    proc = start_server(spec, port, log_path)
    try:
        if not wait_ready(port, proc):
            print(f'!! {spec.name}: server failed to start, see {log_path}', file=sys.stderr)
            print(log_path.read_text()[-3000:], file=sys.stderr)
            raise SystemExit(1)
        url = f'http://127.0.0.1:{port}/'
        ws_url = f'ws://127.0.0.1:{port}/ws_echo' if spec.interface != 'wsgi' else None
        run = asyncio.run(measure(url, ws_url, args))
    finally:
        stop_server(proc)

    return {
        'baseline': summarize(run.baseline, args.baseline),
        'contention': summarize(run.contention, run.elapsed),
        'spammer': summarize(run.spam, run.elapsed),
        'ws_before': summarize(run.ws_before, run.ws_before_seconds),
        'ws_load': summarize(run.ws_load, run.elapsed),
    }


def main() -> None:
    """Run every requested configuration and print the reports."""
    args = parse_args()
    specs = build_specs(args)

    print(
        f'Granian fair-queuing benchmark ({args.interface.upper()}) - granian checkout: {granian_checkout() or "n/a"}'
    )
    print(f'python {sys.version.split()[0]}')
    print(
        f'app service time {args.delay_min_ms}-{args.delay_max_ms}ms'
        + (f' (cpu share {args.cpu_share})' if args.interface != 'wsgi' else '')
        + f' | {args.polite} polite clients | spammer {args.spammer} concurrent | '
        + f'workers {args.workers} | '
        + (
            f'blocking-threads {args.blocking_threads}'
            if args.interface == 'wsgi'
            else f'blocking-threads 1 ({args.interface})'
        )
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
        if spec.interface != 'wsgi':
            print_group('ws echo', results[spec.name]['ws_before'], with_rps=False)
        print('  contention (polite + spammer):')
        print_group('polite', results[spec.name]['contention'])
        print_group('spammer', results[spec.name]['spammer'])
        if spec.interface != 'wsgi':
            print_group('ws echo', results[spec.name]['ws_load'], with_rps=False)

    print(f'\n=== summary ({args.interface.upper()}, contention phase) ===')
    header = (
        f'{"config":<10} {"granian":<9} {"polite p50":>11} {"polite p95":>11} {"polite rps":>11} '
        f'{"errors":>7} {"spam rps":>9} {"spam p95":>9}'
    )
    print(header)
    print('-' * len(header))
    for name, stats in results.items():
        polite, spam = stats['contention'], stats['spammer']
        print(
            f'{name:<10} {versions[name]:<9} {fmt_ms(polite["p50"]):>11} {fmt_ms(polite["p95"]):>11} '
            f'{polite["rps"]:>11.2f} {polite["errors"]:>7.0f} {spam["rps"]:>9.2f} {fmt_ms(spam["p95"]):>9}'
        )
    print('\npolite = everyone except the spammer; rps is the polite completion rate during contention.')

    if args.interface != 'wsgi':
        print('\n=== websocket echo round trip ===')
        ws_header = f'{"config":<10} {"before load p50":>15} {"before load p95":>15} {"under load p50":>15} {"under load p95":>15}'
        print(ws_header)
        print('-' * len(ws_header))
        for name, stats in results.items():
            before, load = stats['ws_before'], stats['ws_load']
            print(
                f'{name:<10} {fmt_ms(before["p50"]):>15} {fmt_ms(before["p95"]):>15} '
                f'{fmt_ms(load["p50"]):>15} {fmt_ms(load["p95"]):>15}'
            )
        print('\nws echo = application-level round trip through the Python app (n can be small under load).')


if __name__ == '__main__':
    main()
