# Code review — 2026-09-18

Second full-tree quality review. Method differs from the
[2026-09-17 review](code_review_2026-09-17.md): instead of one pass, three
module-scoped reviewers worked the tree in parallel (network/concurrency,
short-URL business logic, log/middleware/config), each with its own Release
build and its own repro programs; a main controller arbitrated findings,
independently re-verified each accepted one, then fixed.

Result: six P1 defects found and fixed, each with a regression test (suite
grows 8 → 12 targets; 12/12 passing, re-verified on a clean build). The
deferred backlog is refreshed: new P2/P3 findings merged with the
still-open 09-17 leftovers, everything severity-ordered below. No feature
work.

Unless noted, `file:line` in *Deferred* refers to the tree as reviewed
(pre-fix); fix anchors refer to the post-fix tree.

## Method

- **Worker A — network & concurrency**: `http/`, `webserver.*`,
  `timer/`, `threadpool/`, `lock/`. Verified the timer/listener concurrency
  design (no races found in the timer list, no double-close, graceful
  shutdown order confirmed via strace) and found the EMFILE livelock and the
  pipelined-request drop.
- **Worker B — short-URL business**: `shorturl/`, `handler/`, `sql/`.
  Verified base62 round-trips, shard-router naming vs schema, bloom
  semantics, singleflight merge, cache expiry consistency; found the
  snowflake cross-instance collision and the L1 stale-FIFO eviction.
- **Worker C — log / middleware / config**: `log/`, `CGImysql/`,
  `analytics/`, `observability/`, `config/`, `consumer/`. Found the
  rotation `fputs(NULL)` crash, the half-applied config, and the consumer
  poison-pill loop.
- Each worker ran the 8-target ctest suite green on a private Release build
  before reporting (so findings are against a known-good baseline), and each
  shipped compile-and-run repro programs (`/tmp/tws-review-A|B|C/`).
- The main controller re-verified every accepted finding independently
  before fixing, and fixed only the six P1s; everything else went to the
  deferred backlog below.

**Lesson from a fixed fix.** The first version of the pipelining fix moved
the leftover request bytes with an in-place `memmove` relative to
`init()` — but `init()` memsets the whole read buffer, and the regions
overlap, so the wrong ordering silently zeroed exactly the bytes the fix
was meant to preserve. The threaded driver test
(`tests/test_http_pipelining.cpp`, socketpair harness driving
`read_once()`/`process()`/`write()` in the production split) caught it on
the first run. The landed version stages the bytes to a stack buffer first,
then `init()`, then restores (`http/http_conn.cpp:441`). Two takeaways:
overlapping-buffer reasoning deserves a test even when it looks obvious,
and a fix that has not been executed against its regression test is not a
fix yet — this one would have shipped a correctness fix that corrupted the
very requests it protected.

## Fixed

### 1. EMFILE/ENFILE accept livelock + error-log flood (remote DoS)

- **Trigger**: open connections reaching the process fd limit (~1000 under
  the default `ulimit -n 1024`); also triggered by any transient
  `accept()` resource failure.
- **Root cause** (`webserver.cpp`, `dealclientdata`): every `accept()`
  failure — including EMFILE/ENFILE — only logged `LOG_ERROR` and returned.
  Under level-triggered epoll the listen fd re-fires immediately → the
  event loop hot-spins on failing accepts. The `m_user_count >= MAX_FD`
  guard (65536) can never fire first under a 1024 fd limit.
- **Evidence** (pre-fix): `ulimit -n 64` + 61 concurrent connects →
  400 CPU ticks / 4 s (one core pegged) and **~243 MB** of "accept error"
  log in ~60 s (five 48 MB rotated files). Disk-fill DoS plus a badly
  degraded event loop.
- **Fix**: `pause_accept()` (`webserver.cpp:232`) — on
  EMFILE/ENFILE/ENOBUFS/ENOMEM, `EPOLL_CTL_DEL` the listen fd, set
  `m_listen_paused`, log once; both the LT and ET accept paths handle it.
  `resume_accept()` (`webserver.cpp:243`) re-arms the listen fd on the next
  timer tick (`eventLoop`, `webserver.cpp:506`), giving a bounded,
  self-healing backoff instead of a hot spin.
- **Verification** (post-fix): under the same flood, 10 s of CPU sampling
  shows **0 ticks** and log growth of **454 bytes** total; the
  pause → (fds freed) → resume-on-tick cycle observed working.

### 2. Pipelined / same-segment keep-alive requests silently dropped

- **Trigger**: two (or more) requests arriving in one `read_once()` —
  explicit pipelining, or a fast serial client whose second request lands
  in the same TCP segment as the first.
- **Root cause** (`http/http_conn.cpp`): `process_read()` returns
  `GET_REQUEST` at the end of the first request, leaving subsequent bytes
  in `m_read_buf`; after sending the keep-alive response, `write()` called
  `init()`, which clears `m_read_buf`/`m_read_idx` — the unparsed bytes
  were dropped without a trace while the response advertised
  `Connection: keep-alive`. The client then waited for a second response
  until the 15 s idle timeout. Additionally `parse_content()` did not
  advance `m_checked_idx` past a request body, so bytes after a body could
  not parse as the next request either.
- **Evidence** (pre-fix): repro script sends two `GET /health` in a single
  `send()` on a keep-alive connection → exactly **1** response arrives,
  under both LT+LT and ET+ET; sequential (non-pipelined) control works.
- **Fix**: (a) `parse_content()` consumes the body
  (`m_checked_idx += m_content_length`, `http/http_conn.cpp:361`) so the
  bytes after it frame as the next request; (b) `retain_pipelined_bytes()`
  (`http/http_conn.cpp:441`) preserves leftover bytes across the parser
  reset (stage to stack buffer → `init()` → restore — see the Method note
  on why the order matters); (c) `has_pipelined_input()`
  (`http/http_conn.h:84`) plus a re-dispatch in `dealwithwrite`
  (`webserver.cpp:387`) re-enqueues the connection to the worker pool
  after the response is sent, so the buffered request is processed instead
  of waiting for socket bytes that will never trigger epoll.
- **Verification**: end-to-end repro script (real sockets): both responses
  delivered, under LT and ET. New `http_pipelining` ctest covers
  body-consumption, piggybacked requests, and the reset-survival path;
  pre-fix code fails it (only one response ever produced).

### 3. Log rotation `fopen` failure → `fputs(NULL)` SEGV (hot-path crash)

- **Trigger**: rotation fires on day rollover or every `m_split_lines`
  lines; if `fopen` fails at that moment (disk full, log directory removed/
  chmodded, fd exhaustion), the very next log write crashes the process.
- **Root cause** (`log/log.cpp`): rotation's `fopen` was unchecked, leaving
  `m_fp == NULL`; both the synchronous fallback `fputs` and the async flush
  thread's `fputs` dereferenced it. Deterministic, not intermittent: once
  NULL, every subsequent line crashes.
- **Evidence**: repro with `split_lines=4` + `chmod 0555` on the log dir
  mid-run → ASan: `SEGV on unknown address 0x0 ... in fputs ←
  Log::write_log`.
- **Fix**: the old `FILE*` is fclosed and NULLed under a guard; a failed
  `fopen` keeps `m_fp == NULL`, reports one `strerror(errno)` line to
  stderr, and is retried on the next write (the rotation condition now
  includes `m_fp == NULL`, `log/log.cpp:104`; error path
  `log/log.cpp:130`); every `fputs` path NULL-checks (`log/log.cpp:188`,
  `log/log.h:46`).
- **Verification**: new `log_rotation_nullfp` ctest reproduces the failure
  (read-only dir mid-run) and asserts the process survives and keeps
  retrying; pre-fix code dies mid-test.

### 4. `Config::load()` failure left a half-applied config

- **Trigger**: any YAML type error part-way through `config.yaml`
  (e.g. `port: "abc"` after some valid keys).
- **Root cause** (`config/config.cpp`): `load()` assigned fields in place;
  a `YAML::Exception` mid-file returned `false` but every field parsed
  before the error was already overwritten. `main()` then claimed
  "using defaults" while the process actually ran a half-default /
  half-file mix with wrong port, thread count, or DB credentials.
- **Evidence**: repro loads a config whose later section is malformed →
  `load()` returns false, but `port` 9006→1234 and `thread_num` 8→4 had
  leaked in.
- **Fix**: load-or-nothing — snapshot `const Config rollback = *this`
  before any mutation (`config/config.cpp:43`); the catch handler restores
  `*this = rollback` (`config/config.cpp:174`).
- **Verification**: new `config_rollback` ctest asserts that after a failed
  load every pre-load value is intact; pre-fix code fails on the first two
  assertions.

### 5. Kafka consumer poison-pill crash loop stalls the partition

- **Trigger**: one undeliverable message — non-UTF-8 payload, valid JSON
  missing required fields, or a permanent MySQL rejection (data too long,
  constraint violation). The old code died on it; after restart it
  re-consumed the same offset and died again, forever: the partition (and
  group) stopped consuming.
- **Root cause** (`consumer/click_consumer.py`): the insert path was
  `except Exception: raise`; `UnicodeDecodeError` and `KeyError` fell into
  that same branch; `msg.error()` was re-raised for *every* error class.
- **Fix**: (a) `parse_event()` never raises — undecodable / malformed /
  missing-required-field messages are counted (`events_invalid_total`),
  logged with partition/offset, and skipped (committable);
  (b) `insert_once()` classifies MySQL errors: permanent errnos
  {1048, 1054, 1146, 1264, 1265, 1366, 1406, 3819} are poison pills —
  counted, logged (`click_event_poison`), skipped; transient errors are
  retried up to 5 times with exponential backoff and a fresh connection;
  (c) retries exhausted → exit non-zero **without committing** (the offset
  is retried after restart: no data loss, no stall);
  (d) `msg.error()` now exits only on `fatal()`, logs-and-continues on
  transient errors (e.g. broker rolling restart).
- **Verification**: logic-level — Python reproductions confirm
  `UnicodeDecodeError`/`KeyError` reached the old `raise` branch and are
  now classified as invalid-and-skip. No live Kafka/MySQL in this
  environment, so the broker-integration path is verified by code review
  only (honest limitation; the error classes are unit-level covered by
  construction).

### 6. Snowflake IDs had no worker-id bits → cross-instance collisions

- **Trigger**: two instances (LB behind ≥2 nodes) minting IDs in the same
  second — systematically guaranteed, not probabilistic.
- **Root cause** (`shorturl/snowflake.cpp`): layout was
  `(seconds << 12) | sequence` with no machine bits, so identical clocks +
  identical sequence = identical ID = identical base62 short code.
- **Evidence**: two generators, same second, 100 IDs each → **100/100
  collisions** (only 100 distinct values out of 200). Single-instance
  uniqueness was fine (400k IDs, no duplicates).
- **Fix**: layout is now `(seconds << 22) | (worker << 12) | sequence`
  with 10 worker bits (`shorturl/snowflake.h:22`); the worker id defaults
  to pid-derived (atomic, set at static init — two same-host instances
  disagree without any configuration), and
  `SnowflakeIdGenerator::set_worker_id()` pins it; `main()` feeds
  `snowflake.worker_id` from the config (`config/config.h:76`,
  `main.cpp:24`) for managed deployments; values are masked to 0..1023,
  negatives keep the default.
- **Trade-off**: the ID value grows ~2^10×, so base62 short codes get
  roughly one character longer sooner (log₆₂(2¹⁰) ≈ 1.7); still comfortably
  inside the `VARCHAR(16)` code column. Accepted: collisions are worse
  than longer codes.
- **Verification**: new `snowflake_worker` ctest pins the new layout
  (worker bits extractable at 12..21), pinning/clamping/negative-ignored
  semantics, and cross-worker uniqueness. Pre-fix collision count (100/100)
  vs post-fix distinct IDs across two workers.

## Behavior change

**Bytes beyond `Content-Length` are no longer discarded.** They are now
consumed and parsed as the next request on the keep-alive connection, per
RFC framing (fix 2a). Previously they were silently dropped with
`init()`. Clients that (incorrectly) sent a body longer than `CL` and
relied on the old drop will now get their excess bytes parsed as a request;
garbage residue yields 400. Conformant clients see only the fix: pipelined
and piggybacked requests now work.

## Deferred findings (reported, not fixed)

All P2/P3. Items tagged **[09-17 #n]** come from the previous review's
deferred list and are still open (severity re-assessed by this round).
Of the nine 09-17 leftovers: the `fopen(NULL)` crash (part of #5) is fixed
this round; #1, #2, #3, #4, #6, #7, #8, #9 and the rest of #5 remain.

### P2

1. **L1 cache stale-FIFO twin entries evict hot keys**
   (`shorturl/short_url_cache.cpp:234`, with lazy delete at 161–186).
   `get()`-lazy-deleted entries leave a stale code in `positive_fifo`; a
   re-inserted key then has a FIFO twin, and eviction pops the *stale* twin
   but evicts the *fresh* map entry. Repro'd 4/4 deterministically:
   freshly refreshed hot key evicted under byte pressure while older keys
   survive. Fix directions: move-to-tail + dedup on update, skip-stale on
   evict, or a real LRU list.
2. **Bloom default capacity unbounded false-positive growth, no rebuild**
   (`shorturl/short_url_cache.cpp:382`, `config/config.cpp`). Default
   1 Mbit, k=7: measured FP ≈0.68% at 100k codes, **78.2% at 500k**,
   99.1% at 1M. `inserted_count()` exists but nobody watches it. Soft mode
   loses penetration protection; hard mode + multi-instance can 404 valid
   codes (documented limitation). Fix directions: size bits to the expected
   code count, alarm/auto-rebuild at a waterline, hard-mode auto-downgrade.
3. **Singleflight waiters have no timeout**
   (`shorturl/singleflight.cpp:50`). A leader stuck in a MySQL query holds
   all same-key waiters indefinitely (8 waiters blocked ≥1500 ms in the
   repro), widening a per-key failure to worker-pool exhaustion.
   `wait_for` + 503 on timeout, or a loader statement timeout.
4. **Snowflake spins under the mutex during clock rollback**
   (`shorturl/snowflake.cpp`, `wait_next_second`) **[09-17 #6, extended]**.
   A −3 s rollback blocked `next_id()` ~3967 ms with the lock held;
   minute-level rollbacks would stall all creates. Fail fast beyond a small
   threshold; condition-variable the wait.
5. **Duplicate-code retry exhaustion returns 503 with empty detail**
   (`handler/short_url_handler.cpp:125,152-154`). The DUP path never sets
   `db_error`, so clients get a semantics-wrong 503 with no diagnostics.
6. **`block_queue` timed-pop timeout conversion wrong + spurious-wakeup
   early return** (`log/block_queue.h:180-186`). `tv_nsec` uses `×1000`
   (ms→µs confusion) and drops `now.tv_usec`; `if` instead of `while`.
   Measured: `pop(500)` returns in ~0.01 ms; `pop(2500)` waits 2 s; spurious
   returns at 91/199 ms. Dormant today (only untimed pop is used) but any
   future user inherits broken semantics.
7. **`~Log` races the async flush thread** (`log/log.cpp:17-23`,
   `log/log.h:42-46`) **[part of 09-17 #5]**. No stop flag/join/drain:
   intermittent SEGV storm at exit (one ASan run: 7,275,337 DEADLYSIGNAL
   lines, 1/4 runs hang) plus lost tail lines. Structural; loss quantity
   not reproduced on this machine.
8. **Over-long log names overflow `char[128]` members**
   (`log/log.cpp:56-57`) **[part of 09-17 #5]**. `strcpy`/`strncpy` without
   bounds; intra-object overflow that ASan cannot see; a 212-char name
   fopen'ed fine while already corrupting neighbors. `snprintf` + length
   check.
9. **`DestroyPool` with blocked waiters resets the semaphore → SEGV**
   (`CGImysql/sql_connection_pool.cpp:196-227`). 3/3 SIGSEGV in repro;
   safe today only because of an implicit "join workers first" ordering.
10. **`m_Port = Port` int→char narrowing** (`CGImysql/sql_connection_pool.cpp:51`).
    Hits `operator=(char)`: 3306 becomes one byte `0xEA`. Latent (main
    flow uses the config int), but a loaded gun for any reader of
    `pool.m_Port`.
11. **Structured logger: silent line loss + illegal JSONL on control
    bytes** (`observability/structured_logger.cpp:139-145,154-168`)
    **[09-17 #3]**. Open failure drops lines with zero metrics; raw
    `<0x20` bytes break `json.loads` for downstream parsers.
12. **`log.path` (and log tuning) in config.yaml are ignored**
    (`webserver.cpp:115-117` hardcodes `"./ServerLog"`, 2000, 800000, 800;
    `log_path` has zero consumers).
13. **Consumer per-message lag sampling + synchronous commit**
    (`consumer/click_consumer.py`). Two broker round trips plus a sync
    commit per message cap throughput; sample lag on a timer, batch
    commits.
14. **Consumer `msg.error()` classification is coarse**
    (`consumer/click_consumer.py`) **[09-17 adjacent; partially mitigated
    this round]**. Fatal-vs-transient is now split; finer per-errno policy
    (which transient errors should still count/reconnect) remains open.

### P3 (selected)

15. Accepted fd never validated against `MAX_FD` (`webserver.cpp`)
    **[09-17 #2]** — reachable only with `RLIMIT_NOFILE` > 65536.
16. LT `read_once` treats `EINTR`/`EAGAIN` as fatal (`http/http_conn.cpp:187-190`)
    **[09-17 #1]**.
17. `arm_epoll_if_current` check→`modfd` TOCTOU; `m_sockfd`,
    `m_pending_close` etc. cross threads as plain fields
    (`http/http_conn.cpp:73-82`).
18. Response ≥ 8192 buffer → `process_write` failure closes the connection
    with an empty reply; no 500 fallback (`http/http_conn.cpp`).
19. Duplicate `Content-Length` last-wins, no 400 (`http/http_conn.cpp:315-329`)
    **[09-17 #4]**.
20. `inet_ntoa` portability (`http/http_conn.cpp:557`) — verified *not* a
    bug on this glibc (TLS buffer); use `inet_ntop` for old libc/musl.
21. `listen` backlog 5 (`webserver.cpp:160`); bursts overflow the accept
    queue (compounds EMFILE scenarios).
22. Threadpool ctor: `pthread_create` mid-failure → UAF
    (`threadpool/threadpool.h:45-59`).
23. Method mismatch returns 404, not 405 (`http/router.cpp:74`).
24. `sem::wait(timeout)` uses `CLOCK_REALTIME` (`lock/locker.h:36-57`).
25. `localtime()` (not `_r`) from worker threads in `write_log`
    (`log/log.cpp:42,78`) **[part of 09-17 #5]**.
26. `LOG_*` macros flush per line and read caller-scope `m_close_log`
    (`log/log.h:59-66`) **[part of 09-17 #5]**.
27. Timestamp `snprintf(m_buf, 48)` runs before the buf-size clamp:
    `buf_size < 48` overflows the heap (`log/log.cpp:136`); ASan-verified
    with buf_size=32. Dormant (callers hardcode 2000).
28. SQL pool has no `mysql_ping`/keepalive; `wait_timeout`-expired
    connections fail their first use (`CGImysql/sql_connection_pool.cpp`).
29. Snowflake overflow `throw` has no catch on the call chain → would
    `std::terminate` (`shorturl/snowflake.cpp:26`).
30. `/health` is a static 200, reflects no dependency health
    (`handler/health_handler.cpp:5-10`).
31. Metrics `escape_label_value` misses `\n`; `le` labels render
    `0.001000`-style (`observability/metrics_registry.cpp`) **[09-17 #7]**.
32. `show_error` writes raw text, not valid HTTP (`timer/lst_timer.cpp`)
    **[09-17 #8]**.
33. Benign notes **[09-17 #9]**: `threadpool::m_joined` unsynchronized;
    `m_pending_close` plain bool (fold into #17 if the I/O model changes).
34. Other Worker-B notes: negative cache is L1-only → ≤30 s phantom-404
    window across instances (`handler/short_url_handler.cpp:217`);
    `expire_at` format-checked but not range/semantics-checked
    (`:63-84`); no long-URL idempotency; bloom single global mutex (only
    matters in hard mode); shard router has no resharding path.

## Verification

```text
Baseline (pre-fix, all three reviewer builds, Release): ctest 8/8
  (http_protocol, http_parse_e2e, conn_generation, local_cache,
   phase3_isolate, metrics_shards, log_overflow, mysql_pool_timeout)

Post-fix tree: 12 ctest targets (8 prior + http_pipelining,
log_rotation_nullfp, config_rollback, snowflake_worker): **12/12 passing**
on a clean Release build (gcc 9.4.0 / Ubuntu 20.04), confirmed twice
independently.

End to end (real sockets, repro scripts from the review round):
  pipelined keep-alive (2 GETs, one send):
    pre-fix : 1 response (LT+LT and ET+ET), client hangs to idle timeout
    post-fix: 2 responses, LT and ET
  EMFILE flood (ulimit -n 64, 61 connectors):
    pre-fix : 400 CPU ticks / 4 s (one core pegged), ~243 MB error log / 60 s
    post-fix: 0 ticks / 10 s, +454 B log, pause → resume-on-tick cycle correct

Fix-of-the-fix: first pipelining retention version (memmove/init ordering)
was caught by the threaded driver test on its first run; see Method.
```

Consumer integration (live broker + MySQL) was *not* exercised in this
environment; fix 5 is verified at the logic level and by code review, as
noted in its section.
