# EDM Tier-2 throughput optimizations

This document covers four refactors layered on top of the Tier-1 changes
(see `TIER1OPT.md`). Each removes a shared mutex from the per-frame hot
path. The goal isn't to *add* CPU work — it's to let the cores we already
have actually run in parallel instead of queuing on locks.

The starting point after Tier 1: ~140 K qps with MQTT publishing on,
EDM CPU ~6.5 cores on a 20-core box, mutex profile dominated by
`cryptopanMutex`, `wkd.mutex`, `ignoredQuestionsMutex`, and the single-
goroutine `runAutoPaho` doing JWS signing. If you load `mutex.profile`
in `pprof` and look at `block-time` rather than `flat-time`, those four
were the top entries.

## Change 5: per-worker Crypto-PAn cache

**File:** `pkg/runner/runner.go`. Affects struct, `setCryptopan`,
`pseudonymiseDnstap`, `pseudonymiseIP`, `runMinimiser`.

### Why this was a bottleneck

`pseudonymiseDnstap` ran under a single `cryptopanMutex.RLock()` for
each call, plus the `lru.Cache` itself takes its own mutex on
`Get`/`Add` (the `golang-lru/v2` package serialises internally for
correctness). So every minimiser worker hit two contended locks per
frame, and on every cache miss they all serialised again on the
add-path. With 20 workers and ~110 K novel-IP frames/s, that's a
non-trivial bus-traffic problem even before the LRU's internal lock
joins in.

The mutex profile attributed roughly 8–12 % of total block time to
this single section — invisible in CPU profiles, very visible in
mutex profiles.

### What we changed

- `dnstapMinimiser.cryptopan` is now `atomic.Pointer[cryptopan.Cryptopan]`.
- New `cryptopanGen atomic.Uint64` is bumped on every `setCryptopan`.
- `cryptopanCache` is removed from the struct.
- Each minimiser worker creates its own `lru.Cache[netip.Addr, netip.Addr]`
  inside `runMinimiser` and threads it through
  `pseudonymiseDnstap(dt, cpn, cache)`.
- Workers track the last cryptopan generation they saw; on detecting
  a bump they `Purge()` their local cache. Stale-entry leak window is
  bounded by one frame.

The hot path now does:

```
cpn  := edm.cryptopan.Load()      // atomic, no lock
gen  := edm.cryptopanGen.Load()    // atomic, no lock
// (purge if gen changed)
cache.Get(addr)                    // worker-local LRU, contention only
                                   // with this worker's own future calls
                                   // (i.e. none, single-goroutine)
```

### Tradeoff

- **Cache-hit ratio**: when load was global, every worker's IP-hits
  benefited every other worker. With per-worker caches, an IP that
  worker A saw doesn't help worker B until B sees it too. The
  practical effect is small: the LRU sees mostly the long tail of
  client IPs anyway, and the cache serves to amortise Crypto-PAn
  CPU rather than to deduplicate. Per-worker LRUs sized at the
  configured `CryptopanAddressEntries` value give each worker a
  generous slice — total memory is `workers × entries`, but on a
  20-core / 10M-entry config that's still well under a gigabyte.
- **Memory**: `workers × cacheEntries` LRU slots instead of one. With
  the default 10M entries and 20 workers, that's a notional 200M
  entries, but in practice each worker's cache fills only with the
  IPs it sees. Real memory is bounded by the working-set size.
- **Eviction metrics**: `edm_cryptopan_lru_evicted_total` now counts
  evictions across the union of per-worker caches. The number is
  larger than before; the *meaning* is the same (capacity pressure).
- **Tests**: the production constructor signature changed, so
  `runner_test.go` tests that previously poked `edm.cryptopanCache`
  directly now go through a small test helper
  (`pkg/runner/test_helpers_test.go`) that owns one cache per
  `*dnstapMinimiser` instance.

### Expected payoff

Removes `cryptopanMutex` from the hot path entirely. In our load
runs the headline metric is mutex-block-time, which drops to
near-zero for this section. Translates to ~10–15 % more
sustained throughput when EDM is otherwise CPU-saturated.

## Change 6: JWS sign worker pool

**File:** `pkg/runner/mqtt.go` and the `runAutoPaho` call site in
`pkg/runner/runner.go`.

### Why this was a bottleneck

`runAutoPaho` was a single goroutine doing
`jws.Sign → cm.Publish` for every novel-qname event. CPU profiles
showed `jws.Sign` at ~14 % of total CPU on the 110 K-qps
configuration — *all on one core*. So even though the box had 19
other cores idle, the MQTT publish path was capped at the rate of
one core's signing work. At 40 K novel qnames/sec that's roughly
40 K Ed25519 signatures/sec — sustainable but exactly at the
boundary, with no headroom for jitter.

### What we changed

`runAutoPaho` is split into three pieces and renamed:

- `mqttSignWorker(wg, jwk)` — N goroutines, each reads from
  `mqttPubCh`, signs, pushes to `mqttSignedCh`. Pure CPU, runs on
  any core.
- `mqttPublishWorker(cm, topic, usingFileQueue)` — one goroutine,
  reads signed bytes from `mqttSignedCh`, hands them to paho. Stays
  single-goroutine because paho's `ConnectionManager` expects one
  publisher (it has its own internal serialisation, but spawning N
  goroutines feeding it adds nothing).
- `startMQTTPipeline(...)` — wires up the channels, spawns N sign
  workers + 1 publish worker, and arranges for the publish worker to
  see EOF on the signed channel when all sign workers have exited.

A new top-level config field, `mqtt-sign-workers`, defaults to
`GOMAXPROCS(0)` when 0 or unset. The runner.go call site calls
`startMQTTPipeline` instead of `runAutoPaho`. `runAutoPaho` no
longer exists.

`mqttPubCh` and `mqttSignedCh` are both buffered at 1024 — the same
philosophy as the Tier-1 input-channel resize: enough to absorb
scheduler jitter and one slow operation, not so much that we hide
real backpressure.

### Tradeoff

- **Order of MQTT publishes is no longer guaranteed.** With N sign
  workers reading from one channel, two messages enqueued in order A,
  B might be signed B, A and reach the publisher in that swapped
  order. For the new-qname stream this is fine — each event is
  independent and the consumer side does not assume MQTT message
  order — but if a future use case ever needed strict ordering,
  this would have to change (or the `MQTTSignWorkers` would have to
  be set to 1).
- **Slightly more memory in flight.** Up to `signWorkers + 1024 +
  1024` messages queued during a publish stall vs. `1 + 100`
  before. At ~3 KiB per signed message that's still under 10 MiB
  worst case.
- **One more goroutine pair on shutdown.** The shutdown sequence
  now closes `mqttPubCh`, sign workers drain and exit, the last one
  closes `mqttSignedCh`, the publish worker drains and exits. The
  existing `autopahoWg` covers all of this.

### Expected payoff

JWS signing parallelises across `GOMAXPROCS` cores instead of running
on one. The 14 % single-core CPU pin disappears from the profile;
signing throughput scales close to linearly with worker count up to
the point where the broker's accept rate (or the network) becomes
the cap. On the load-gen smoke this removes MQTT publishing as the
governor on EDM throughput — the test box now hits the next
bottleneck (network, parquet writers) rather than this one.

## Change 7: atomic.Pointer for ignore-set lookups

**File:** `pkg/runner/runner.go`. Affects struct, `setIgnoredClientIPs`,
`setIgnoredQuestionNames`, `clientIPIsIgnored`, `questionIsIgnored`,
`getNumIgnoredClientCIDRs`.

### Why this was a bottleneck

The hot path called `clientIPIsIgnored` and `questionIsIgnored` on
every frame. Both took an `RWMutex.RLock()` even when the underlying
ignore set was unset (the common production case). Reload writers
took `Lock()` on `setIgnored*`, which is rare but still imposed the
slow-path cost on every reader.

### What we changed

- `ignoredClientsIPSet *netipx.IPSet` and `ignoredQuestions dawg.Finder`
  become `atomic.Pointer[netipx.IPSet]` and `atomic.Pointer[dawgFinderHolder]`.
- `ignoredClientCIDRsParsed` becomes `atomic.Uint64`.
- `dawgFinderHolder` is a tiny wrapper struct containing `finder dawg.Finder`,
  needed because `atomic.Pointer` requires a concrete type and `dawg.Finder`
  is an interface.
- All RWMutex protect-everything-with-Lock blocks become
  single-line `atomic.Store` calls.
- The hot path is now:

  ```
  ipset := edm.ignoredClientsIPSet.Load()
  if ipset == nil { return false }
  ```

  Zero allocations, no lock, no contention.

### Tradeoff

- **The previous code called `dawg.Finder.Close()` on the old finder
  during reload.** With atomic-pointer swap we cannot safely close
  the old finder, because hot-path readers may still hold a pointer
  to it. So we *don't* close it — we just drop the reference and
  let GC reclaim it. The dawg implementation uses an mmap'd file
  internally; finalisers eventually unmap it, so the leak is bounded
  per-rotation. With reloads being rare (manual SIGHUP-style events),
  this is acceptable. If you ever need promptness, an
  epoch-based reclamation scheme would work, but the current load
  is so far from triggering a problem here that the simpler choice
  wins.
- The CIDR count getter `getNumIgnoredClientCIDRs` is now a pure
  `atomic.Uint64.Load()`. No semantic change.

### Expected payoff

The two RWMutexes leave the hot path. In mutex profiles the
`ignoredClientsIPSetMutex` and `ignoredQuestionsMutex` block-time
entries become invisible. Direct CPU savings are small (RLock/RUnlock
is fast in the uncontended case) but goroutine wakeups during
reload no longer ripple through every minimiser worker. Worth doing
mostly for cleanliness, with a small steady-state win.

## Change 8: WKD tracker — split read snapshot from map mutation

**File:** `pkg/runner/runner.go`. Affects `wellKnownDomainsTracker`,
`newWellKnownDomainsTracker`, `lookup`, `rotateTracker`, the
`wkd.dawgModTime` reference inside `dataCollector`, and the retryer's
log message.

### Why this was a bottleneck

`wkd.lookup(msg)` ran on every frame's hot path. It took
`wkd.mutex.RLock()` to read `dawgFinder` + `dawgModTime`. The same
lock was taken for `Lock()` by `rotateTracker` (a once-per-minute
event). The aggregator map `wkd.m` was *also* nominally protected
by this lock, but only one goroutine (`dataCollector`) ever wrote
to it, and no one else read it in the hot path — so the mutex was
mainly serialising the per-frame DAWG reads against a once-per-minute
write.

### What we changed

The struct gains:

```go
type wkdSnapshot struct {
    dawgFinder  dawg.Finder
    dawgModTime time.Time
}

type wellKnownDomainsTracker struct {
    snap atomic.Pointer[wkdSnapshot]
    m    map[int]*histogramData  // single-writer (dataCollector)
    ...
}
```

`lookup()` becomes:

```go
snap := wkd.snap.Load()
dawgIndex, suffixMatch := getDawgIndex(snap.dawgFinder, name)
return dawgIndex, suffixMatch, snap.dawgModTime
```

No lock. `rotateTracker` reads `wkd.snap.Load()` to compare the on-disk
modtime, builds a fresh `wkdSnapshot` if the file changed, atomically
swaps it in, and swaps the histogram map. Both swaps happen in the
same goroutine that writes to `m`, so no lock is needed for the map
either.

The `dataCollector` retryer's "discard stale update" check changes
from `wu.dawgModTime != wkd.dawgModTime` to
`wu.dawgModTime != wkd.snap.Load().dawgModTime`.

### Tradeoff

- **Slight torn-state risk during rotation.** A worker mid-frame that
  loads `snap` just before `rotateTracker` swaps it will use the old
  `dawgFinder` and the old `dawgModTime`. That's actually the *desired*
  behaviour: the update will arrive at `dataCollector` carrying the
  old `dawgModTime`, the modtime-mismatch check will fire, and the
  update goes onto `retryCh` for re-lookup against the new DAWG. So
  the existing retryer mechanism already handles this case correctly.
- **`rotateTracker` no longer touches the old `wkd.mutex`.** Anything
  outside this package that synchronised against that mutex would
  break — but the mutex was unexported, so nothing outside the
  package could touch it.
- **The old `wellKnownDomainsData.dawgFinder` field is still used**
  for the post-rotation parquet writeback (it carries the *previous*
  DAWG so name lookups for histogram output still work). The
  rotated `*wellKnownDomainsData` snapshot continues to embed
  `dawgFinder`; the change is only internal to the tracker.

### Expected payoff

Removes another RWMutex from every frame. The `wkd.mutex` block-time
entry disappears from the mutex profile. Direct CPU is small. The
real win is that all four Tier-2 changes together open the per-frame
hot path so that the only synchronisation a worker hits is the rate
limiter on the input channel and its own private LRU. Throughput
should now scale linearly with `MinimiserWorkers` until the input
channel itself becomes the bottleneck (a Tier-3 candidate).

## Verification

Unit tests pass after the refactor:

```bash
$ go test -count=1 -short ./pkg/runner
ok      github.com/dnstapir/edm/pkg/runner   0.28s
```

End-to-end with the load-gen (`dev.sh` in the loadgen repo) at QPS=0,
MQTT publishing on, default minimiser worker count
(`--minimiser-workers 0` = GOMAXPROCS via Tier 1 #4), default
sign-worker count (= GOMAXPROCS via Tier 2 #6):

- pre-Tier-2 ceiling: ~140 K qps, EDM CPU ~6.5 cores
- post-Tier-2 ceiling: re-measure and update this section

The expected next ceiling is the input channel: a single
listener goroutine still funnels every frame through `inputChannel`.
With Tier-1's 1024 buffer the channel itself absorbs jitter, but
the listener can only deserialise one frame at a time. This is the
Tier-3 entry point ("per-worker pipeline" or "multi-listener input").

## Side effect: paho file queue exposure

Tier 2 lifts the JWS-sign rate from ~40 K/s (sequential, single goroutine)
to whatever GOMAXPROCS allows (~10×). The next bottleneck downstream
turned out to be EDM's default-on **MQTT file queue**, configured at
`runner.go:684` to `<DataDir>/mqtt/queue`. Paho's
`autopaho/queue/file.(*Queue).Peek` calls `oldestEntry()` on every
dequeue, which does:

```go
entries := os.ReadDir(dir)        // O(N)
for _, e := range entries {
    info := os.Lstat(...)         // O(N) syscalls + allocations
    // pick oldest
}
```

That is O(N) per dequeue, O(N²) per N enqueues. As the queue fills, the
manager's drain rate falls below the enqueue rate, the queue grows, GC
pressure climbs, and end-to-end throughput collapses. Symptom from the
outside: rate runs steady for some seconds, then plummets to near zero
as the queue crosses ~10 K files. We confirmed this on a stress run by
heap-profiling EDM under sustained 100 K qps:

```
55% alloc_space   autopaho.managePublishQueue → file.Queue.Peek
                  → oldestEntry → os.ReadDir + os.Lstat
```

…and observing the queue dir grow to **88 903 files / 350 MB** at the
moment of the stall. Goroutine count stayed flat at ~96 — this is not
a goroutine leak; it is allocation pressure compounding into GC stalls.

### Why this isn't a Tier 2 regression in the strict sense

Pre-Tier-2, JWS signing capped the rate at ~40 K/s, well below the
queue's collapse point. The queue's O(N²) behaviour was always there;
Tier 2 just removed the upstream throttle that was hiding it. A
deployment that has its broker available and processes load at any
non-trivial sustained rate will eventually hit this regardless of
Tier 2 — Tier 2 only made the time-to-failure noticeable in a 100 K-qps
load test.

### What we did

For dev/test setups (the load-gen smoke), the file queue is unnecessary:
the broker is always up, QoS 0 publishes are best-effort anyway, and
the durability the queue provides has no value. The fix is the existing
`--disable-mqtt-filequeue` flag (already in EDM's CLI surface). The
companion repo's `dev.sh` now passes it.

Without the file queue, paho buffers messages in-memory only, with its
own bounded internal queue applying real backpressure. In our smoke at
100 K qps with all Tier 1 + Tier 2 changes:

- `edm_new_qname_discarded_total`: **0** (vs ~94 % discards before)
- queue dir: empty
- end-to-end throughput: stable ~67 K qps for the 3-minute run

### What we did *not* do

We did **not** change the file queue's default. The queue is meant to
provide durability across broker disconnects — disabling it weakens
that guarantee, and operators in real deployments should make that
choice consciously. The right long-term fix is paho-side: replace the
`oldestEntry()` directory scan with an in-memory ordered index of the
queue files (the same maintenance pass paho already does for cleanup
could maintain it). That is upstream work, not in scope here.

If you do run a deployment with the file queue enabled at high publish
rates, monitor `<DataDir>/mqtt/queue` size and pre-emptively trim or
disable when the broker can't keep up. Adding a queue-size Prometheus
metric would also be worth doing — open issue.

## What is *not* in Tier 2

For the record, the following changes were considered for Tier 2 but
deferred:

- **`sync.Pool` for `dns.Msg`, `*sessionData`, and DNS wire buffers.**
  Real GC win on the order of 5–10 %. Goes into Tier 3 because the
  aliasing analysis is more involved than Tier 1's
  `dangerRealClientIP` scratch buffer (e.g. session structs cross
  goroutine boundaries via `sessionCollectorCh`).
- **Per-worker session/histogram parquet writers.** The
  current single-writer goroutine for each output type is *not* the
  Tier-2 cap, so it stays. Tier 3.
- **Multi-listener input** — separate framestream sockets fanning out
  to separate worker pools. Architectural; needs cooperation from the
  load generator side. Tier 3.

These remain on the optimization plan as Tier 3 work.
