# Fair-queuing benchmark (WSGI)

A small, self-contained harness to measure what the `--scheduler fair` option does to a
slow WSGI app under abuse. It runs the released Granian from PyPI and a local Granian
checkout side by side, so it doubles as a regression check for the default (`fifo`)
scheduler.

## Scenario

A slow-ish WSGI app (`app.py`, service time drawn uniformly from **300–2000 ms**) is served
by a single Granian worker with a small Python thread pool (`--blocking-threads 8`).

* **Polite clients** — a handful of closed-loop clients (one request at a time each). They
  represent normal traffic and *should* see a latency close to the app's own service time.
* **Spammer** — one client with a large number of concurrent requests, continuously. It is
  what the fair scheduler is supposed to contain.

Each client sends a distinct `User-Agent`, and the default `fq_inner_headers` include
`user-agent`, so the spammer maps to its own *inner flow* while every polite client maps to
another. Without that, all clients would share one flow and fair queuing would have nothing
to separate.

Three configurations are compared:

| config | what it is |
| --- | --- |
| `official` | the released Granian from PyPI (`granian==<latest>` in an isolated venv); has no scheduler flag, and is the reference for the local `fifo` |
| `fifo` | this working tree, `--scheduler fifo` (the default) |
| `fair` | this working tree, `--scheduler fair` |

## Running

The harness needs `httpx` and, for the `fifo`/`fair` configs, a Granian checkout installed
in the interpreter it runs under (it uses the `granian` script sitting next to
`sys.executable`). From a Granian checkout:

```bash
uv run python benchmarks/fair_queuing/run.py
uv run python benchmarks/fair_queuing/run.py --configs fifo,fair --contention 60 --spammer 128
```

Or from this repo, if Granian is installed in the current environment:

```bash
python run.py --configs official
python /path/to/granian/benchmarks/fair_queuing/run.py   # fifo/fair need that venv
```

Phases per config: `--warmup` (polite only, discarded), `--baseline` (polite only,
reported), `--contention` (polite *and* spammer, reported). The measurement window closes
exactly when the spammer is stopped; anything still in flight is discarded, so the reported
RPS is not inflated by the drain that follows (which is long for FIFO).

Reported per config, for polite clients and for the spammer separately: request count, RPS,
P50/P95/P99, mean, max and errors. The summary table is the thing to read.

## What to look for

* `official` and `fifo` should be equivalent — that is the regression check.
* With `fair`, the polite P50/P95 should stay near the `baseline` numbers (the app's own
  service time), while the spammer's latency grows and its share of the completions drops.

## Example result

`uv run python benchmarks/fair_queuing/run.py` (Python 3.14.6, 1 worker, 8 blocking
threads, 6 polite clients, 64-way spammer, 45 s of contention), contention phase:

| config | polite p50 | polite p95 | polite rps | errors | spam rps | spam p95 |
| --- | --- | --- | --- | --- | --- | --- |
| official 2.8.5 | 10526 ms | 11345 ms | 0.69 | 0 | 7.42 | 11413 ms |
| fifo 2.8.5 | 10074 ms | 11433 ms | 0.67 | 0 | 7.87 | 10943 ms |
| fair 2.8.5 | **1265 ms** | **2045 ms** | **4.76** | 0 | 3.73 | 27672 ms |

Baseline (no spammer) was ~1.1–1.2 s P50 for all three, i.e. under `fair` the polite
clients are essentially unaffected by the spammer, and their throughput is ~7× higher.
Total server throughput is the same for all three (~8 req/s), so nothing is being made
faster — the completions are just redistributed away from the spammer.

## Caveats

* The load generator and the server share the machine, so CPU contention affects the
  numbers; compare configs within a single run, not across machines.
* Sample counts for the polite clients under FIFO are small by construction (their requests
  take ~10 s each), so P95 there is rough. Increase `--contention` to tighten it.
* The spammer is intentionally one flow: real abusers may spread across many fingerprints,
  in which case containment is bounded by the outer prefix (`fq_outer_prefix_v4/v6`).
