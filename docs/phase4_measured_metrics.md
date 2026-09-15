# Phase 4: sharded MetricsRegistry (measured)

## Bottleneck (measured)

While this phase landed, the live Phase 0 harness could not run:
`curl http://127.0.0.1:9006/health` → connection refused
(`run_scenarios.sh --dry-run` recorded `server_up=0`). No cluster QPS claimed.

A later laptop / WSL2 stand-in run is in
[laptop_wsl2_experiment.md](laptop_wsl2_experiment.md) (still not a server QPS).

In-process evidence on the pre-change process-wide `mutex_` (Release, g++ -O2):

| threads | total observe iters | wall_s | ns/iter |
|--------:|--------------------:|-------:|--------:|
| 1 | 400000 | 0.051 | 128 |
| 2 | 400000 | 0.168 | 420 |
| 4 | 400000 | 0.167 | 418 |
| 8 | 400000 | 0.252 | 630 |

ns/iter rises with thread count → request-path `observe_*` serialized on one mutex
(plus string key concat under that lock for HTTP). That is the L1/request observe tax
after Phase 3 isolated Redis/Kafka/log from the 302 path.

## Change (one)

`MetricsRegistry` uses 16 shards hashed by `std::this_thread::get_id()`.
`observe_*` locks only the calling thread's shard. `render_prometheus()` / getters
aggregate under per-shard locks (scrape may serialize; request path must not share one mutex).

## After (same microbench, not QPS)

| threads | total observe iters | wall_s | ns/iter |
|--------:|--------------------:|-------:|--------:|
| 1 | 400000 | 0.062 | 156 |
| 2 | 400000 | 0.038 | 95 |
| 4 | 400000 | 0.087 | 218 |
| 8 | 400000 | 0.047 | 118 |

8-thread wall dropped ~5× on this machine for the same observe mix. Not a server QPS claim.
