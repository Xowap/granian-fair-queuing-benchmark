"""ASGI application used by the fair-queuing benchmark.

An HTTP request is served as a random sequence of CPU bursts and ``asyncio.sleep`` awaits,
summing to a target service time (defaults 300..2000 ms). The CPU bursts are the point: on
ASGI the lane is the event loop thread, so a burst holds the lane and is what the fair
scheduler measures and debits, while an await hands the lane back.

A WebSocket endpoint echoes whatever it receives, so the benchmark can probe the event
loop's responsiveness with a round trip that goes through the Python application.

Everything is read from the environment so the runner can tune it without editing this file.
"""

import asyncio
import os
import random
import time


DELAY_MIN_S = float(os.environ.get('BENCH_DELAY_MIN_MS', '300')) / 1000.0
DELAY_MAX_S = float(os.environ.get('BENCH_DELAY_MAX_MS', '2000')) / 1000.0
#: Probability that a slice of work is CPU (blocking the loop) rather than an await.
CPU_SHARE = float(os.environ.get('BENCH_CPU_SHARE', '0.2'))
#: Longest single slice, so one step never holds the loop for an unreasonable time.
MAX_SLICE_S = float(os.environ.get('BENCH_MAX_SLICE_MS', '50')) / 1000.0
BODY = b'ok' * 32


def _plan() -> list[tuple[str, float]]:
    """Random list of ('cpu'|'await', seconds) slices summing to a target service time.

    Slices are short and independently chosen, so a request interleaves many brief CPU
    bursts with awaits instead of holding the loop once for the whole service time. That
    is what makes step *ordering* matter more than any single step's duration.
    """
    total = random.uniform(DELAY_MIN_S, DELAY_MAX_S)
    plan: list[tuple[str, float]] = []
    remaining = total
    while remaining > 1e-9:
        part = min(remaining, random.uniform(MAX_SLICE_S / 4, MAX_SLICE_S))
        plan.append(('cpu' if random.random() < CPU_SHARE else 'await', part))
        remaining -= part
    return plan


def _spin(seconds: float) -> None:
    """Busy-wait, holding the event loop; this is the lane occupancy of an ASGI request."""
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        pass


async def _http(_scope, _receive, send) -> None:
    """Serve one HTTP request: a random mix of CPU work and awaits, then a small body."""
    probe = os.environ.get('BENCH_PROBE_STEPS')
    started = time.perf_counter()
    nominal = cpu_lag = await_lag = 0.0
    for kind, seconds in _plan():
        nominal += seconds
        t0 = time.perf_counter()
        if kind == 'cpu':
            _spin(seconds)
            cpu_lag += time.perf_counter() - t0 - seconds
        else:
            await asyncio.sleep(seconds)
            await_lag += time.perf_counter() - t0 - seconds
    if probe:
        ua = dict(_scope['headers']).get(b'user-agent', b'?').decode()
        if 'polite' in ua:
            import sys

            print(
                f'[req] {ua} nominal={nominal:.2f} wall={time.perf_counter() - started:.2f} '
                f'cpu_lag={cpu_lag:.2f} await_lag={await_lag:.2f}',
                file=sys.stderr,
                flush=True,
            )
    await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
    await send({'type': 'http.response.body', 'body': BODY})


async def _websocket(_scope, receive, send) -> None:
    """Echo WebSocket: send back every message, until the client disconnects."""
    await send({'type': 'websocket.accept'})
    while True:
        message = await receive()
        if message['type'] == 'websocket.disconnect':
            return
        if message['type'] != 'websocket.receive':
            continue
        if message.get('text') is not None:
            await send({'type': 'websocket.send', 'text': message['text']})
        elif message.get('bytes') is not None:
            await send({'type': 'websocket.send', 'bytes': message['bytes']})


def _start_loop_probe(loop) -> None:
    """Debug aid: log the event loop's ready-queue depth and live task count."""
    import sys
    import threading

    def sample():
        while True:
            time.sleep(0.5)
            print(f'[loop] ready={len(loop._ready)} tasks={len(asyncio.all_tasks(loop))}', file=sys.stderr, flush=True)

    threading.Thread(target=sample, daemon=True).start()


async def _lifespan(_scope, receive, send) -> None:
    """Minimal ASGI lifespan support, required by Granian's default ASGI worker."""
    while True:
        message = await receive()
        if message['type'] == 'lifespan.startup':
            if os.environ.get('BENCH_PROBE_LOOP'):
                _start_loop_probe(asyncio.get_running_loop())
            await send({'type': 'lifespan.startup.complete'})
        elif message['type'] == 'lifespan.shutdown':
            await send({'type': 'lifespan.shutdown.complete'})
            return


async def application(scope, receive, send) -> None:
    """ASGI entrypoint."""
    if scope['type'] == 'http':
        await _http(scope, receive, send)
    elif scope['type'] == 'websocket':
        await _websocket(scope, receive, send)
    else:
        await _lifespan(scope, receive, send)
