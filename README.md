# LockTracing.jl

[![CI](https://github.com/IanButterworth/LockTracing.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/IanButterworth/LockTracing.jl/actions/workflows/CI.yml)

Find out **where** your `ReentrantLock` conflicts happen.

Base can already count lock conflicts with `Base.@lock_conflicts`, but a count doesn't
tell you which code is contending. LockTracing records a backtrace for every conflict
and aggregates them by call site.

Based on [JuliaLang/julia#55120](https://github.com/JuliaLang/julia/pull/55120).

## Usage

```julia
using LockTracing

@tracelocks run_workload()
LockTracing.report()
```

```
53 lock conflict(s) at 2 call site(s):

  41 conflict(s) (77.4%)
    refresh_cache!(c::Cache) at cache.jl:87
    getindex(c::Cache, k::String) at cache.jl:31 [inlined]
    handle(req::Request) at server.jl:112
    (::var"#serve##0")() at server.jl:44

  12 conflict(s) (22.6%)
    log_event(msg::String) at logging.jl:14
    handle(req::Request) at server.jl:118
    (::var"#serve##0")() at server.jl:44
```

The full stack is printed for each site (Base's internal `lock` frames are trimmed off
the top); `report(; maxsites, maxframes)` shortens the output.

Or manually:

```julia
LockTracing.start()
run_workload()
LockTracing.stop()
LockTracing.report()          # or LockTracing.results() for the raw data
LockTracing.reset!()
```

A "conflict" is one `lock(::ReentrantLock)` call that failed its fast path and had to
wait — the same event Base's `@lock_conflicts` counts.

## Two modes

### `deep=false` (default)

Base's lock slow path already calls `Threads.inc_lock_conflict_count()` on every
conflict, gated by `Threads.lock_profiling()`. LockTracing overwrites that one tiny
cold-path function to also capture a backtrace, so `Base.lock` itself is left alone and
nothing about the locking implementation is duplicated.

Reports conflict counts per call site.

### `deep=true`

```julia
@tracelocks deep=true run_workload()
```

Additionally replaces `Base.lock(::ReentrantLock)` with an instrumented copy of Base's
implementation, which can also see the lock object and how long the wait took:

```
53 conflict(s) (100.0%), waited 1.22 s total / 89.1 ms max, 1 lock instance(s)
```

This copies Base's `slowlock` body (see `_install_deep` in `src/LockTracing.jl`), so it
depends on Base internals and needs checking against new Julia versions. It errors up
front if the internals it needs are missing; `LockTracing.deep_supported()` tells you
whether the running version is OK. Verified on 1.12 – 1.14; 1.11 is basic mode only.

## Caveats

- Both modes overwrite a method in Base. That invalidates code (expect a recompilation
  pause on the first `start()`), and the replacement stays installed for the rest of the
  session — it just does nothing once tracing is stopped.
- Because a redefined method only becomes visible in a new world age, the traced
  workload must not run in the same top-level expression as `start()`. `@tracelocks` and
  `LockTracing.trace(f)` handle this with `invokelatest`; at the REPL or in a script the
  plain `start()` / `stop()` form is fine too.
- Recording allocates on the conflict path, so measured wait times in `deep` mode are
  inflated relative to an uninstrumented run. Use them to compare sites, not as absolute
  truth.
- Conflicts on threads that did not exist when `start()` was called are dropped; the
  report says how many.
- Only `ReentrantLock` is traced. `Threads.SpinLock` and locks taken inside the runtime
  (C code) are invisible.

## Tests

```
julia --project -t4 test/runtests.jl
```
