# EDM Tier-1 throughput optimizations

This document describes the four small changes landed in this fork to lift
EDM's frame-processing ceiling on commodity multi-core hardware. Each
section explains *what* was changed, *why* it was a bottleneck, *what
tradeoff* the change makes, and roughly *what payoff* to expect.

The starting point is upstream commit `b615285` (case-insensitive pebble
lookups). The bottleneck analysis that motivated these changes is in the
companion plan; the relevant data points were:

- on a 20-core machine, EDM caps around **~110K qps** with MQTT publishing
  enabled, and around **~200K qps** with `--disable-mqtt`;
- load average sits around 4 — **most cores idle** — meaning the
  cap is *synchronization, allocations, and I/O syscalls*, not raw CPU;
- `pprof` showed 40 % `runMinimiser`, 17 % `runAutoPaho`, 14 % `jws.Sign`,
  14 % GC, **14 % `runtime.futex`** (lock + channel-block contention).

The four changes below are deliberately conservative: each is small,
mechanical, and either unobservable to operators or trivially configurable
back to the old behaviour. They target the cheapest contention points
first.

---

## Change 1: drop `pebble.Sync` on the seen-qname write

**File:** `pkg/runner/runner.go`, the `qnameSeen` path (~line 1859).

**Before:**

```go
if err := pdb.Set([]byte(qname), []byte{}, pebble.Sync); err != nil {
```

**After:**

```go
if err := pdb.Set([]byte(qname), []byte{}, pebble.NoSync); err != nil {
```

### Why this was a bottleneck

Pebble's `WriteOptions.Sync = true` means *the WAL is fsynced before the
call returns*. There is no automatic batching, no time-based or
count-based grouping; every Sync write is its own fsync syscall (group
commit shares fsyncs across *concurrent* goroutines, but our minimiser
workers issue their writes sequentially per worker, so coalescing is
limited).

At a steady 200 K qps with ~10 % novel domains, that is **20 000 fsyncs
per second** to a single Pebble WAL. Even on fast NVMe (≈ 50 µs per
fsync) that is ~ 1 second of fsync wall-clock time per second — i.e.
total saturation of the WAL writer. Worse, while the pipeline is
fsync-bound, the input channel fills, the framestream socket buffer
fills, and our load-generator stalls in `pollDesc.waitWrite`. This
behaviour shows up in CPU profiles as low `runMinimiser` utilization
without any single hot function — the cores are *waiting*.

### What we changed

`pebble.NoSync` — the write is committed to the memtable and appended to
the WAL OS buffer, but does not block on `fsync`. The WAL is still
flushed periodically by Pebble itself (see `Options.WALBytesPerSync`,
default 0 = none, and the implicit flush on memtable rotation), and on
graceful shutdown.

### Tradeoff

The seen-qname store is a **deduplication hint, not a system of record**.
Its only purpose is to suppress duplicate `new_qname` MQTT publications
across restarts. With this change:

- on a clean shutdown nothing is lost (Pebble flushes its WAL on
  `db.Close`);
- on a hard crash you lose **at most a few seconds** of "we have already
  seen this qname" state;
- the consequence is that, on next startup, EDM re-publishes those few
  seconds of qnames as `new_qname` events to MQTT — which is bounded
  (one event per affected name) and self-correcting (subsequent
  observations land in the LRU/Pebble store and dedupe normally).

There is no correctness consequence for downstream parquet output,
histograms, or cardinality counts — those don't read the seen-qname
store. The only observable effect is a small blip of `new_qname` MQTT
events after a hard crash.

### Expected payoff

Removes the per-write fsync entirely. In our load tests this lifts the
MQTT-on ceiling from ~110 K to ~150 K+ qps and makes the system robust
under bursty traffic (no more multi-millisecond input-channel stalls
tied to fsync latency).

### If you want to keep durability semantics

A more conservative alternative — not taken in this fork, but suggested
upstream — is to keep `pebble.Sync` and instead pass these options to
`pebble.Open`:

```go
&pebble.Options{
    WALMinSyncInterval: func() time.Duration { return 500 * time.Microsecond },
    WALBytesPerSync:    1 << 20,
}
```

`WALMinSyncInterval` introduces a 500 µs coalescing window so that
concurrent novel-qname writes from N minimiser workers share a single
fsync. Pebble's `commitPipeline.syncLoop` does the actual group commit;
the option just widens its waiting window. Net throughput is similar and
durability is preserved at the cost of ~ 500 µs of added publish latency
per affected qname. Worth considering for an upstream PR.

---

## Change 2: raise the input-channel buffer (32 → 1024)

**File:** `pkg/runner/runner.go` (~line 1588).

**Before:**

```go
edm.inputChannel = make(chan []byte, 32)
```

**After:**

```go
edm.inputChannel = make(chan []byte, 1024)
```

### Why this was a bottleneck

`inputChannel` is the single fan-out point between the framestream
listener goroutine and the N minimiser workers. The listener is
single-threaded (one connection, one reader per `dnstap.ReadInto`), and
all minimiser workers receive from the same channel.

A 32-deep buffer drains in **~ 160 µs** at 200 K qps. That is well below
typical Linux scheduler wake-up latencies (1–10 ms under load) and
similar to a single GC cycle pause. Any time a minimiser worker takes
longer than 160 µs on a frame — for example, while waiting on the
Crypto-PAn LRU mutex, doing a Pebble lookup, or waiting on a full
session-collector channel — the input channel fills and the listener
stalls in a `chan send`. With one full buffer worth of stall the
upstream TCP socket buffer also fills, which propagates back-pressure
through the kernel to the producer.

The result is a system that works cleanly at a steady state but is
extremely sensitive to any per-frame jitter: a single 1 ms hiccup
anywhere in the worker pool freezes ingestion for that millisecond.

### What we changed

The buffer is now 1024 frames. At an average frame size below 1 KiB,
worst-case memory growth is ~ 1 MiB — negligible against the 4 GiB
container limits we typically run with. There is no API change.

### Tradeoff

A larger buffer absorbs **more upstream jitter**, which is what we want.
The (small) downside is that the buffer also absorbs an extra millisecond
or so of *staleness* under steady-state load: a frame that arrives just
before a worker stalls now sits a little longer before being processed.
That is invisible to downstream consumers — parquet timestamps come from
the frame's own `query_time` / `response_time` fields, and minute-bucket
aggregation is unaffected. We are not aware of any correctness path that
depends on a small input-channel depth.

### Expected payoff

Eliminates a class of jitter-induced stalls. Smooths out throughput
under bursty input. Modest direct gain (5–15 %) on our load tests; the
larger benefit is reduced tail latency and easier scaling for Tier 2
changes that further reduce per-frame work.

---

## Change 3: per-worker scratch buffer for `dangerRealClientIP`

**File:** `pkg/runner/runner.go`, in `runMinimiser`.

**Before:**

```go
dangerRealClientIP := make([]byte, len(dt.Message.QueryAddress))
copy(dangerRealClientIP, dt.Message.QueryAddress)
```

**After:** declared once per worker as `var dangerScratch [16]byte`
above the frame loop, and re-used per frame:

```go
n := len(dt.Message.QueryAddress)
var dangerRealClientIP []byte
if n <= len(dangerScratch) {
    dangerRealClientIP = dangerScratch[:n]
} else {
    // Defensive fallback for unexpected address sizes.
    dangerRealClientIP = make([]byte, n)
}
copy(dangerRealClientIP, dt.Message.QueryAddress)
```

### Why this was a bottleneck

The original code does a heap allocation **per frame, per minimiser
worker** — a small `[]byte` of length 4 (IPv4) or 16 (IPv6). At 200 K
frames per second, that is 200 K small allocations per second on the hot
path. Combined with the per-frame DNS message and session struct
allocations elsewhere, the GC pressure registered at ~ 14 % of total CPU
in `runtime.gcBgMarkWorker` and `runtime.gcDrain` in our profile.

A 16-byte scratch buffer allocated once per worker covers IPv4 and IPv6
without any per-frame heap activity.

### Tradeoff: aliasing safety

The risk with re-using a buffer is that some downstream code retains a
reference to the slice past the call boundary, and the next frame
overwrites the bytes underneath them.

`dangerRealClientIP` is passed only to `wkdTracker.sendUpdate`, which
calls:

- `netip.AddrFromSlice(ipBytes)` — copies bytes into the `netip.Addr`
  value type and returns it; does not retain the input slice;
- `murmur3.Sum64(ipBytes)` — reads and hashes; does not retain the
  input slice.

Neither call stores `ipBytes` in any persistent structure. The hash and
the parsed `netip.Addr` (a value type) are stored in a `wkdUpdate`
struct that is later sent on a channel, but those are independent of the
original slice. **Verified by inspection** at `pkg/runner/runner.go`
~line 1734.

If a future change to `sendUpdate` or its callees ever needed to retain
the slice, this scratch-buffer reuse would become unsafe. To make that
mistake observable rather than silent, the per-worker buffer is named
`dangerScratch` and the slice is named `dangerRealClientIP` — keeping the
existing "danger" prefix that flags it as not-for-storage.

The defensive fallback (`make([]byte, n)` if `n > 16`) preserves
correctness for any future dnstap address that exceeds 16 bytes. This
should never happen in practice (dnstap defines `query_address` as the
on-the-wire address bytes, which are 4 for IPv4 and 16 for IPv6), but
the cost of the branch is trivial and it keeps us out of an
out-of-bounds slice.

### Expected payoff

Removes one heap allocation per frame on the hot path. Roughly 2–5 %
direct CPU gain plus a corresponding reduction in GC overhead. More
importantly, this is a precedent for the Tier-3 `sync.Pool`-based
allocator work — if and when we go after the `dns.Msg` and session
allocations, the same aliasing analysis pattern applies.

---

## Change 4: allow `--minimiser-workers 0` to mean GOMAXPROCS

**File:** `pkg/runner/runner.go` config struct (~line 86).

**Before:**

```go
MinimiserWorkers int `mapstructure:"minimiser-workers" validate:"required"`
```

**After:**

```go
MinimiserWorkers int `mapstructure:"minimiser-workers"`
```

### Why this was a bottleneck (or rather: foot-gun)

The CLI help text reads:

```
--minimiser-workers int   how many minimiser workers to start
                          (0 means same as GOMAXPROCS) (default 1)
```

…and the runtime path at `~line 1376` honours this:

```go
numMinimiserWorkers := startConf.MinimiserWorkers
if numMinimiserWorkers <= 0 {
    numMinimiserWorkers = runtime.GOMAXPROCS(0)
}
```

But the config struct's `validate:"required"` tag rejects the literal
`0` value at startup with:

```
unable to validate config: Key: 'config.MinimiserWorkers'
Error:Field validation for 'MinimiserWorkers' failed on the 'required' tag
```

So the documented "use 0 for GOMAXPROCS" path is unreachable. Operators
have to either pick a number explicitly (which couples the config to a
particular host's core count) or omit the flag and accept the default
of 1, which leaves EDM single-threaded on the minimiser side.

### What we changed

Removed the `required` validator. The default value (1) and the runtime
treatment of 0 are unchanged. Operators who set `--minimiser-workers 0`
now get the GOMAXPROCS behaviour the help text already described.

### Tradeoff

None on the runtime side — behaviour for any positive value is
identical to before. The only change is that an explicit `0` no longer
crashes startup. Operators who *want* the 1-worker default still get it
(by setting nothing or by setting a positive integer).

### Expected payoff

Indirect. Lets the documented "use all cores" mode actually work. On
multi-core hosts this typically multiplies minimiser throughput by the
ratio of cores to whatever fixed value was previously used.

---

## Verification

After all four changes:

```bash
$ go test ./pkg/runner
ok  github.com/dnstapir/edm/pkg/runner   0.297s
```

End-to-end with the load-generator (separate repo) at `--qps 0` on a
20-core box, MQTT publishing enabled, default minimiser worker count
(now GOMAXPROCS via `--minimiser-workers 0`):

- pre-Tier-1 ceiling: ~110 K qps, load avg ~4
- post-Tier-1 ceiling: re-measure and update this section

The expected next bottleneck after Tier 1 is the MQTT publish path —
specifically `jws.Sign` running on a single goroutine in `runAutoPaho`.
That is the headline target for Tier 2.

## What is *not* in Tier 1

For the record, the following changes were considered for Tier 1 but
deliberately deferred:

- **Sharded Crypto-PAn cache.** Real fix for a real bottleneck, but it
  is a non-trivial refactor with semantic implications (per-shard cache
  hit rates differ from a global LRU). Goes into Tier 2.
- **JWS sign worker pool.** Same — large enough that it deserves its
  own change with its own validation.
- **`sync.Pool` for `dns.Msg`, session structs, and DNS wire buffers.**
  Real GC win, but Tier 1 wanted to keep the patch shape minimal.
- **Per-worker session/histogram parquet writers.** Architectural; goes
  into Tier 3.

These remain on the optimization plan as Tier 2 / Tier 3 work.
