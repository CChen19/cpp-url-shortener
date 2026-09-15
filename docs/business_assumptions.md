# Business assumptions

These assumptions bound the optimization program. Later phases must not quietly expand them.

## Product shape

1. **Read-heavy, write-light.** Short-URL create (`POST /api/shorten`) must be reliable. Redirect (`GET /{code}`) must stay low-latency under load.
2. **Skewed access.** Production traffic is hotspot-heavy. Every performance claim must be checked on at least three mixes: single hot code, many codes uniform, and Zipf.
3. **Immutable mappings.** After create, `short_code -> long_url` does not change. Optional `expire_at` is allowed. There is no ops console for rewrite or bulk admin.
4. **Redirect outranks click stats.** Serving a correct redirect is more important than recording a click. Analytics must not gate the redirect path.
5. **Single-process multicore first.** Optimize one process on one machine before any distributed control plane, Canal, or service-mesh work. Redis and Kafka remain adapters, not a new core I/O model.

## Hard constraints

### Cache TTL is not business expiry

- Redis/cache TTL controls **how long a value may stay in cache**.
- Business expiry (`expire_at`) controls **whether the mapping is still valid**.
- A cached value that is still within cache TTL but past `expire_at` must **not** return `302`.
- A cache miss means refill from authoritative storage (MySQL), not "not found".
- An expired mapping must return a non-redirect outcome (today: `410`), never a stale redirect.

### Click analytics semantics

- A "click" means a **server-processed redirect request**, not proven client receipt of the destination page.
- Redirect must not block on stats publish (Kafka or otherwise).
- Under overload, stats may lose events **if and only if** drops are counted/observable (metrics/logs). Silent unbounded loss is not acceptable.

## Out of scope for this program

- Expanding the existing `4x4` MySQL schema/table router into a distributed sharding platform.
- io_uring, DPDK, lock-free queues, RCU, custom allocators, C++ coroutine rewrites, custom storage engines, Canal, service mesh, or cluster control planes (see standing program orders).
