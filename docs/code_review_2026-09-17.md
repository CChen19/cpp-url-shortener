# Code review — 2026-09-17

Quality review of the full tree (no feature work). Method: manual read of every
module, then targeted exploitation of two suspects against an ASan build of
the real server, then fixes with regression tests.

Result: two remotely triggerable crashes found and fixed; the rest of the
concurrency design (connection generations, singleflight, bounded queues,
sharded metrics, origin budgets) held up under review. 8/8 ctest targets pass;
the Release server survives both formerly crashing requests.

## Fixed: request-line NULL dereference (remote crash)

- **Trigger**: one unauthenticated request — `GET http://host HTTP/1.1`
  (absolute-form URI without a path).
- **Root cause** (`http/http_conn.cpp`, `parse_request_line`): after stripping
  `http://`, `strchr(m_url, '/')` returns `NULL` when the authority has no
  path, and the *next* line called `strncasecmp(NULL, "https://", 8)`.
- **Evidence**: ASan on the running server — `SEGV on unknown address 0x0` in
  `__interceptor_strncasecmp` ← `parse_request_line` ← `process_read` ←
  worker thread. The whole process died on a single request.
- **Fix**: reject with `BAD_REQUEST` as soon as the post-`http://` `strchr`
  returns `NULL`. The `https://` branch and the final `!m_url` check are now
  never reached with a null pointer.
- **Regression**: `tests/test_http_parse_e2e.cpp` drives the real
  `http_conn::process()` over a socketpair (event loop does `read_once()`,
  worker does `process()`, same split as production). Asserts: absolute-form
  without path → 400 (used to SEGV); absolute-form with path → parses; plain
  path → parses; unknown method → 400.

## Fixed: `Log::write_log` heap buffer overflow (remote crash)

- **Trigger**: any logged line longer than the 2000-byte `m_buf`. Request
  lines are logged (`LOG_INFO("%s", text)` per line, logging on by default),
  so a request line of ~2000+ characters is enough.
- **Root cause** (`log/log.cpp`): `vsnprintf` returns the *would-be* length on
  truncation; the code then wrote `m_buf[n + m] = '\n'` and
  `m_buf[n + m + 1] = '\0'` using that unclamped offset — up to kilobytes
  past the allocation.
- **Evidence**: ASan PoC — `heap-buffer-overflow`, WRITE of size 1 located 25
  bytes past the 2000-byte `m_buf` from `Log::init`. End-to-end against the
  server: a 4000-char request line produced `heap-use-after-free` at
  `log.cpp:141` and killed the process.
- **Fix**: clamp both the prefix length `n` and the formatted length `m` to
  `m_log_buf_size` before writing the newline/NUL terminator; long lines are
  truncated instead of corrupting the heap.
- **Regression**: `tests/test_log_overflow.cpp` writes a 4000-char payload and
  asserts the appended line is ≤ 2100 bytes, contains no run of 1965+ `A`s,
  and that a short line written afterwards stays intact. The pre-fix code
  fails these assertions (full 4000-char payload + corruption).

## Verification

```text
build (Release, gcc): OK, no new warnings from the touched lines
ctest: 8/8 passed (6 existing + http_parse_e2e + log_overflow), 3 consecutive runs
Release server, end to end:
  GET http://noslash.example HTTP/1.1   -> HTTP/1.1 400  (was: SEGV, process died)
  GET /<4000 chars> HTTP/1.1            -> HTTP/1.1 503  (was: heap corruption, process died)
  process alive after both
```

## Deferred findings (reported, not fixed)

Ordered by severity within the deferred set.

1. `read_once` treats `EINTR` as fatal (`http/http_conn.cpp`). A `SIGALRM`
   (fires every 5 s) landing inside the nonblocking `recv` closes the
   connection spuriously. Retry on `EINTR` in both the LT and ET branches.
2. `dealclientdata` guards `m_user_count >= MAX_FD` but never validates the
   accepted **fd value** (`webserver.cpp`). With `RLIMIT_NOFILE > 65536`,
   `users[connfd]` / `users_timer[connfd]` index out of bounds. Add
   `connfd >= MAX_FD` → `show_error` + close.
3. `StructuredLogger::json_escape` escapes `\n \r \t` but not other control
   bytes (`\b`, `\f`, `< 0x20`) (`observability/structured_logger.cpp`). A
   hostile User-Agent can emit invalid JSONL. Reject or `\u00XX`-escape.
4. Duplicate `Content-Length` headers: last one wins (`parse_headers`). A
   strict single-CL check is cheap smuggling hardening.
5. Legacy log module (`log/log.cpp`, `log/block_queue.h`):
   `LOG_*` macros call `flush()` per line from caller threads (hot-path
   contention even in async mode); a failed `fopen` on day rollover leaves
   `m_fp == NULL` → `fputs(NULL)`; the async thread never exits and races
   `~Log`'s `fclose` at process exit (this test suite works around it with
   `_Exit`); `localtime` (not `_r`) is called from worker threads.
6. `SnowflakeIdGenerator::wait_next_second` spins (1 ms sleeps) while holding
   the generator mutex for up to 1 s at > 4096 ids/second
   (`shorturl/snowflake.cpp`). Rare; bounded by MySQL acquire timeout anyway.
7. `MetricsRegistry::escape_label_value` does not escape `\n`
   (`observability/metrics_registry.cpp`). Low impact today: label values are
   internal route patterns and status classes.
8. `Utils::show_error` writes raw text, not valid HTTP, when the server is
   full (`timer/lst_timer.cpp`). Cosmetic; a 503 would be cleaner.
9. Benign notes: `threadpool::m_joined` is unsynchronized (single-threaded
   use today); `http_conn::m_pending_close` is a plain bool crossing
   worker→event-loop (safe in practice via syscall boundaries; would be worth
   an atomic if the I/O model ever changes).

## Environment note

AddressSanitizer on this machine (WSL2 kernel
6.18.33.2-microsoft-standard-WSL2) DEADLYSIGNAL-loops sporadically — a
hello-world ASan binary reproduced it (millions of report lines across 20
runs). Both overflow proofs above were captured from ad hoc ASan runs before
the fix; `-fsanitize=address` stays out of ctest targets so the suite stays
deterministic here.
