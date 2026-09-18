# Documentation index

Reading map for this repository. Start with the top-level
[README](../README.md) (English) or [README_zh-CN](../README_zh-CN.md)
(Chinese), then follow the phases in order.

## Design & measurements (read in order)

| Doc | Language | What it covers |
| --- | --- | --- |
| [business_assumptions.md](business_assumptions.md) | EN | Assumptions that bound the optimization program; later phases must not quietly expand them. |
| [phase0_baseline.md](phase0_baseline.md) | EN | Fixed-arrival-rate baseline harness. Claims no QPS number by itself. |
| [laptop_wsl2_experiment.md](laptop_wsl2_experiment.md) | EN | One honest laptop/WSL2 run backing the Phase 0 tables — explicitly not a server result. |
| [phase1_baseline.md](phase1_baseline.md) | ZH | Short-URL read/write benchmark entry points; webbench pages/min vs RPS labeling correction. |
| [phase2_cache_consistency.md](phase2_cache_consistency.md) | ZH | Redis cache layer: consistency, penetration, breakdown, avalanche. |
| [phase3_kafka_delivery.md](phase3_kafka_delivery.md) | EN | Click stats off the redirect path via a lossy bounded Kafka queue. |
| [phase4_sharding.md](phase4_sharding.md) | ZH | Thin sharding router: `short_code` → `database.table`. |
| [phase4_measured_metrics.md](phase4_measured_metrics.md) | EN | Sharded `MetricsRegistry`: measured bottleneck and result. |
| [phase5_observability.md](phase5_observability.md) | ZH | `/metrics` on the C++ server and the Python click consumer. |

## Quality & reviews

- [code_review_2026-09-17.md](code_review_2026-09-17.md) — full-tree review;
  two remotely triggerable crashes found and fixed, nine findings deferred.
- [code_review_2026-09-18.md](code_review_2026-09-18.md) — second full-tree
  review by three parallel module-scoped reviewers (network/concurrency,
  short-URL business logic, log & middleware/config); six P1 fixes with
  regression tests, updated deferred backlog.

## Conventions

- Benchmark docs distinguish **measured** numbers from estimates and always
  state the environment. Do not copy numbers between environments.
- Each phase doc appends; historical data is never overwritten.
- Code review docs list findings with `file:line`, severity, trigger, and
  fix/regression status; anything not fixed goes to the deferred backlog,
  which is mirrored in the top-level README "Known issues".
