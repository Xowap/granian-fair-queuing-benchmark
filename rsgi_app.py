"""RSGI application used by the fair-queuing benchmark.

Same workload as ``asgi_app``, through Granian's RSGI protocol: an HTTP request is a random
sequence of short CPU bursts and ``asyncio.sleep`` awaits (see ``asgi_app._plan``), and a
WebSocket endpoint echoes whatever it receives.
"""

import asyncio
import os
import sys
import time

from asgi_app import BODY, _plan, _spin

from granian.rsgi import WebsocketMessageType


async def _http(scope, protocol) -> None:
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
        ua = scope.headers.get('user-agent', '?')
        if 'polite' in ua:
            print(
                f'[req] {ua} nominal={nominal:.2f} wall={time.perf_counter() - started:.2f} '
                f'cpu_lag={cpu_lag:.2f} await_lag={await_lag:.2f}',
                file=sys.stderr,
                flush=True,
            )
    protocol.response_bytes(200, [('content-type', 'text/plain')], BODY)


async def _websocket(_scope, protocol) -> None:
    """Echo WebSocket: send back every message, until the client disconnects.

    RSGI requires the handler to end with `protocol.close()`: returning without it leaves
    no status for Granian, which then aborts the worker (an upstream bug, at least in 2.8.5).
    """
    transport = await protocol.accept()
    while True:
        message = await transport.receive()
        if message.kind == WebsocketMessageType.close:
            protocol.close(1000)
            return
        if message.kind == WebsocketMessageType.bytes:
            await transport.send_bytes(message.data)
        else:
            await transport.send_str(message.data)


async def application(scope, protocol) -> None:
    """RSGI entrypoint."""
    if scope.proto == 'ws':
        await _websocket(scope, protocol)
    else:
        await _http(scope, protocol)
