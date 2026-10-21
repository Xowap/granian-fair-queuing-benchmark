"""Slow-ish WSGI application used by the fair-queuing benchmark.

Every request to anything but ``/__ping`` blocks the worker thread for a delay drawn
uniformly from ``[BENCH_DELAY_MIN_MS, BENCH_DELAY_MAX_MS]`` (defaults 300..2000 ms), then
returns a small body. The delay is read from the environment so the benchmark runner can
tune it without editing this file.
"""

import os
import random
import time


DELAY_MIN_S = float(os.environ.get('BENCH_DELAY_MIN_MS', '300')) / 1000.0
DELAY_MAX_S = float(os.environ.get('BENCH_DELAY_MAX_MS', '2000')) / 1000.0
BODY = b'ok' * 32


def application(environ, protocol):
    """WSGI entrypoint: sleep like a slow view, then answer."""
    if environ['PATH_INFO'] == '/__ping':
        protocol('200 OK', [('content-type', 'text/plain')])
        return [b'pong']

    time.sleep(random.uniform(DELAY_MIN_S, DELAY_MAX_S))
    protocol('200 OK', [('content-type', 'text/plain')])
    return [BODY]
