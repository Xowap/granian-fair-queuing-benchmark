# A few bad clients shouldn't take your site down: fair queuing in Granian

Sometimes what gets a website in trouble isn't a traffic spike from real users. It's a
handful of aggressive clients: a scraper that opens dozens of connections in parallel, a
vulnerability scanner walking every URL, a misconfigured bot retrying in a tight loop. In
volume they're a tiny fraction of your visitors, but they behave very differently: they send
requests as fast as they can and they never stop.

When each request costs real computation (rendering a page, running queries, calling a
model), that difference is what hurts. A server has a fixed number of lanes to run Python code
on: the threads of a WSGI pool, or the single event loop of an ASGI/RSGI worker. A
first-come, first-served queue gives those lanes to whoever asks most, so a single client
holding 64 requests in flight gets about 64 times the share of someone browsing normally. The
polite clients, who should be seeing the content without any problem, end up waiting behind
the aggressive client's backlog: their pages go from about one second to ten seconds, and the
site looks down to them.

**Fair queuing** fixes that by sharing the lanes per *client* instead of per *request*.
Requests are grouped into flows by client fingerprint (an IP prefix, then the browser
fingerprint: user agent, language, session cookie…), and each flow is charged for the lane
time its requests actually use. When a lane frees up, it goes to the waiting flow that has
used the least, so a client that floods the server only competes against its own backlog.

This is implemented in Granian behind `--scheduler fair`, on the
[`feature/kff-272-python-side-http-fair-queuing`](https://github.com/Xowap/granian/tree/feature/kff-272-python-side-http-fair-queuing)
branch, for WSGI, ASGI and RSGI. The measurements below show it does what it should: under
the same abuse, polite clients keep a latency close to what they get on an idle server, and
the aggressive client is slowed down, not cut off. It keeps being served, with zero errors,
taking about half of the capacity instead of nearly all of it.

## Results

Each scenario has a few polite clients and one aggressive client (the *spammer*) on a single
Granian worker. The tables cover the contention phase, when the spammer is active. The
baseline is what the polite clients get with no spammer at all. `official` is the released
Granian 2.8.5 from PyPI, `fifo` and `fair` are the branch build with `--scheduler fifo` (the
default) and `--scheduler fair`.

### At a glance

Polite clients, median latency:

| scenario | baseline (no spammer) | official 2.8.5 | fifo | **fair** |
| --- | --- | --- | --- | --- |
| WSGI, slow views | 1.11–1.25 s | 9.76 s | 9.94 s | **1.21 s** |
| ASGI, slow mixed CPU/await requests | 1.72–1.84 s | 7.57 s | 8.41 s | **2.17 s** |
| RSGI, slow mixed CPU/await requests | 1.38–1.61 s | 5.88 s | 6.93 s | **1.87 s** |
| ASGI, flood of short CPU-bound requests | 150–152 ms | 12.90 s | 12.90 s | **200 ms** |
| RSGI, flood of short CPU-bound requests | 149–153 ms | 12.86 s | 12.83 s | **203 ms** |

WebSocket echo round trip on the async interfaces, median (an interactive client sharing the
worker; see methodology):

| scenario | before the spammer | official 2.8.5 | fifo | **fair** |
| --- | --- | --- | --- | --- |
| ASGI, slow mixed requests | 6–15 ms | 341 ms | 337 ms | **35 ms** |
| RSGI, slow mixed requests | 8 ms | 356 ms | 389 ms | **32 ms** |
| ASGI, flood of short requests | 101–153 ms | 14.64 s | 13.34 s | **149 ms** |
| RSGI, flood of short requests | 104–152 ms | 11.05 s | 15.74 s | **151 ms** |

`official` and `fifo` are indistinguishable everywhere, as expected: `fifo` is Granian's
historical queue, so the branch doesn't regress the default behaviour.

### WSGI: slow views

1 worker, 8 blocking threads; each view blocks its thread for 300–2000 ms. 6 polite clients,
spammer with 64 requests in flight, 45 s of contention.

| config | polite p50 | polite p95 | polite rps | spammer p50 | spammer rps | errors |
| --- | --- | --- | --- | --- | --- | --- |
| official 2.8.5 | 9.76 s | 11.56 s | 0.71 | 9.70 s | 7.76 | 0 |
| fifo | 9.94 s | 11.03 s | 0.69 | 9.85 s | 7.71 | 0 |
| **fair** | **1.21 s** | **2.04 s** | **4.87** | 19.06 s | 3.91 | 0 |

With no spammer, the polite clients get 1.11–1.25 s p50 and about 5 req/s. Under `fair` they
keep that despite the spammer (7× their `fifo` throughput), and the spammer still gets
3.9 req/s.

### ASGI and RSGI: slow mixed requests

1 worker (one event loop); each request is a random sequence of short CPU bursts and
`asyncio.sleep` awaits, totalling 300–2000 ms, about 20% of it CPU. 4 polite clients, spammer
with 32 requests in flight, 30 s of contention, plus one WebSocket echo client.

| interface | config | polite p50 | polite p95 | polite rps | spammer p50 | spammer rps | ws echo p50 | ws echo p95 | errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ASGI | official 2.8.5 | 7.57 s | 11.60 s | 0.57 | 7.22 s | 4.67 | 341.4 ms | 650.6 ms | 0 |
| ASGI | fifo | 8.41 s | 14.16 s | 0.47 | 7.43 s | 4.53 | 336.6 ms | 657.2 ms | 0 |
| ASGI | **fair** | **2.17 s** | **3.65 s** | **1.67** | 11.43 s | 3.03 | **35.3 ms** | **102.2 ms** | 0 |
| RSGI | official 2.8.5 | 5.88 s | 12.68 s | 0.57 | 7.43 s | 4.50 | 356.1 ms | 644.4 ms | 0 |
| RSGI | fifo | 6.93 s | 11.94 s | 0.53 | 7.21 s | 4.53 | 388.9 ms | 623.1 ms | 0 |
| RSGI | **fair** | **1.87 s** | **3.75 s** | **1.93** | 9.57 s | 3.23 | **32.0 ms** | **106.9 ms** | 0 |

Baselines with no spammer: polite p50 1.72–1.84 s (ASGI) and 1.38–1.61 s (RSGI), WebSocket
echo 6–15 ms.

### ASGI and RSGI: flood of short CPU-bound requests

Same setup, but each request is a single 40–60 ms CPU burst, and the spammer keeps 256
requests in flight: a deep backlog that saturates the loop. 3 polite clients.

| interface | config | polite p50 | polite p95 | polite rps | spammer p50 | spammer rps | ws echo p50 | ws echo p95 | errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ASGI | official 2.8.5 | 12.90 s | 12.92 s | 0.40 | 12.88 s | 27.83 | 14.64 s | 25.61 s | 0 |
| ASGI | fifo | 12.90 s | 13.16 s | 0.40 | 12.91 s | 27.83 | 13.34 s | 25.77 s | 0 |
| ASGI | **fair** | **200 ms** | **257 ms** | **14.87** | 24.73 s | 13.40 | **149 ms** | **218 ms** | 0 |
| RSGI | official 2.8.5 | 12.86 s | 13.02 s | 0.40 | 12.90 s | 27.70 | 11.05 s | 17.99 s | 0 |
| RSGI | fifo | 12.83 s | 12.98 s | 0.40 | 12.90 s | 27.87 | 15.74 s | 25.62 s | 0 |
| RSGI | **fair** | **203 ms** | **262 ms** | **14.90** | 25.38 s | 13.23 | **151 ms** | **254 ms** | 0 |

Baselines: polite p50 149–153 ms at about 20 req/s. Under FIFO the polite clients drop to
0.4 req/s and wait 13 s, the same as the spammer: everyone queues behind the same backlog.

### What it costs, and who pays

* **Nothing is made faster.** Total throughput is the same under all three configurations
  (about 8 req/s for WSGI, about 28 req/s for the flood scenarios). Fair queuing only
  redistributes completions.
* **The spammer pays, but keeps being served.** It gets 45–65% of the capacity instead of
  90% or more, with zero errors. Its own latency grows (p50 10–25 s), but that's the latency of
  its own backlog: with 32 to 256 requests queued, each of them waits for the others from the
  same client, not for everybody else's.
* **Overhead.** On the async interfaces, `fair` adds one future and two loop callbacks per
  request step. That's invisible in these scenarios, where a step is milliseconds of work; it
  hasn't been measured on apps serving very cheap requests at high rates.

## Methodology

### Setup

* One machine: AMD Ryzen AI 7 350 (16 threads), Linux 7.0, Python 3.14.6. The load generator
  and the server share it, over loopback.
* `official`: Granian 2.8.5 installed from PyPI into an isolated virtualenv.
* `fifo` and `fair`: the branch linked above, built in release mode, run with
  `--scheduler fifo` and `--scheduler fair` and otherwise default `--fq-*` settings.
* Every configuration runs one worker (`--workers 1`), HTTP/1.1, and 2048 connections of
  backpressure. WSGI uses 8 blocking threads; ASGI/RSGI are forced to 1 by Granian, their
  lane being the event loop.

### Applications

* `app.py` (WSGI): sleeps for a random 300–2000 ms, holding its pool thread, then returns a
  small body.
* `asgi_app.py` (ASGI) and `rsgi_app.py` (RSGI, same workload): picks a random total service
  time (300–2000 ms by default), and fills it with short slices (at most `--max-slice-ms`,
  50 ms by default), each one either a busy-wait CPU burst (with probability `--cpu-share`)
  or an `asyncio.sleep`. CPU bursts hold the event loop, awaits hand it back. Both also serve
  a WebSocket endpoint that echoes every message.

### Clients

* **Polite clients**: closed loop, one request at a time, each with its own `User-Agent`.
* **Spammer**: one client, a single `User-Agent`, keeping `--spammer` requests in flight at
  all times. Using a distinct user agent per client is what puts them in distinct flows (the
  default `fq_inner_headers` start with `user-agent`); all clients share the same IP.
* **WebSocket echo client** (ASGI/RSGI): sends a token, waits for it to come back, records
  the round trip, waits 50 ms (`--ws-interval`), repeats. Its round trip goes through the
  Python app, so it measures how responsive the event loop is to an interactive client.

### Phases and metrics

Each configuration gets a fresh server and three phases:

1. **Warmup** (polite clients only, discarded).
2. **Baseline** (polite clients only, reported): what they get on an otherwise idle server.
3. **Contention** (polite clients and spammer, reported).

The measurement window closes exactly when the spammer stops; requests still in flight are
discarded, so throughput isn't inflated by the drain that follows. Latency is measured
client-side per request; p50/p95 are nearest-rank percentiles; rps is completed requests per
second of the window, per group. The WebSocket echo is split into *before the spammer*
(warmup and baseline) and *under load* (contention).

### Scenarios

| scenario | command |
| --- | --- |
| WSGI, slow views | `run.py --interface wsgi` (6 polite, 64-way spammer, 4 s / 8 s / 45 s phases) |
| ASGI/RSGI, slow mixed requests | `run.py --interface asgi --warmup 3 --baseline 10 --contention 30 --polite 4 --spammer 32` |
| ASGI/RSGI, flood of short requests | `run.py --interface asgi --warmup 3 --baseline 8 --contention 30 --polite 3 --spammer 256 --delay-min-ms 40 --delay-max-ms 60 --cpu-share 1.0 --max-slice-ms 60` |

Replace `asgi` with `rsgi` for the RSGI runs. Each run measures `official`, `fifo` and `fair`
in turn, about 2.5–4 minutes per run. Each scenario was run once on the final code; the
`official` and `fifo` numbers match earlier runs of the same scenarios, and shorter smoke
runs of `fair` gave the same picture.

### Running it

The harness needs `httpx`, `websockets`, and a Granian build installed in the interpreter it
runs under (it uses the `granian` script next to `sys.executable`) for `fifo` and `fair`.
Clone this repository into `benchmarks/fair_queuing` of a checkout of the branch, then:

```bash
uv run maturin develop --release
uv run python benchmarks/fair_queuing/run.py --interface wsgi
uv run python benchmarks/fair_queuing/run.py --interface asgi --contention 45
uv run python benchmarks/fair_queuing/run.py --interface rsgi --configs fifo,fair --spammer 128
```

The released Granian is installed on first use into `/tmp/opencode/fq-bench-official`; server
logs go to `--workdir` (`/tmp/opencode/fq-bench` by default). Two debugging probes are
available on the async apps:

* `BENCH_PROBE_LOOP=1`: the server logs the event loop's ready-queue depth and live task count.
* `BENCH_PROBE_STEPS=1`: the server logs, for each polite request, how much its CPU slices
  and awaits overran their nominal duration. The await overrun is the time spent waiting to
  get the loop back.

### A note on the async interfaces

The first version of the fair scheduler made no measurable difference on ASGI, and this
benchmark is how that was found. The fair queue only ordered the events Granian hands to
Python (new requests, completed I/O); once a request had started it was a plain asyncio
task, and asyncio's own first-come, first-served ready queue decided whose step ran next. The
probes above showed it: as soon as the spammer started, all its requests were live tasks
competing equally for the loop, and the polite requests lost seconds waiting at every await.

The branch now treats the event loop as a fair lane, like a WSGI thread. Before each step, a
request waits for its turn from the scheduler, and when the step suspends it gives the lane
back and its flow is charged for the CPU time used. Turns are granted by a dispatch scheduled
behind the tasks already ready to run, so the choice is made among all of them. Requests stay
regular asyncio tasks.

### Limitations

* Load generator and server share the machine; compare configurations within a run, not
  across machines.
* Polite sample counts under FIFO are small by construction (their requests take seconds), so
  those p95s are rough. Lengthen `--contention` to tighten them.
* The spammer is a single fingerprint. An abuser rotating user agents spreads over several
  inner flows, and is then contained by its IP prefix (`fq_outer_prefix_v4/v6`) instead; all
  clients here share one IP, so that level isn't exercised.
* A running step is never interrupted: a polite request still waits for the step in progress,
  so long CPU bursts without awaits (`--max-slice-ms`) limit how well an async spammer is
  contained.
* An RSGI WebSocket handler must end with `protocol.close()`: returning without it makes
  Granian (2.8.5 included) abort the whole worker on an invalid status code.
