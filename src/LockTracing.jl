"""
    LockTracing

Find out *where* your `ReentrantLock` conflicts happen.

Base already counts lock conflicts (`Base.@lock_conflicts`), but only as a number.
This package records a backtrace for each conflict and aggregates them by call site,
so you can see which code is actually contending.

```julia
using LockTracing

LockTracing.start()
run_workload()
LockTracing.stop()
LockTracing.report()
```

See [`start`](@ref) for the two available modes.
"""
module LockTracing

using Base: StackTraces
using Base.Threads

export @tracelocks

const RawBacktrace = typeof(backtrace())

# ---------------------------------------------------------------------------
# storage
# ---------------------------------------------------------------------------

mutable struct SiteStats
    count::Int
    total_ns::UInt64
    max_ns::UInt64
    const locks::Set{UInt}   # objectids of the distinct lock instances (deep mode only)
end
SiteStats() = SiteStats(0, UInt64(0), UInt64(0), Set{UInt}())

mutable struct ThreadState
    const sites::Dict{RawBacktrace,SiteStats}
    recursing::Bool
end
ThreadState() = ThreadState(Dict{RawBacktrace,SiteStats}(), false)

const OFF = 0
const BASIC = 1
const DEEP = 2

const MODE = Threads.Atomic{Int}(OFF)
# indexed by threadid; sized when tracing starts
const STATES = Ref(Union{Nothing,ThreadState}[])
const DROPPED = Threads.Atomic{Int}(0)

const HOOK_INSTALLED = Ref(false)
const DEEP_INSTALLED = Ref(false)
const PROFILING_ENABLED = Ref(false)

@inline function _state()
    states = STATES[]
    tid = Threads.threadid()
    if tid > length(states)
        # a thread was added after `start()`; we can't grow the vector safely from here
        Threads.atomic_add!(DROPPED, 1)
        return nothing
    end
    st = @inbounds states[tid]
    if st === nothing
        st = ThreadState()
        @inbounds states[tid] = st
    end
    return st
end

# Called from the hook installed into `Threads.inc_lock_conflict_count`, i.e. from
# inside `Base.lock`'s slow path. Must not itself take a `ReentrantLock`.
function _record(t0::UInt64, rl::Union{Base.ReentrantLock,Nothing})
    st = _state()
    st === nothing && return nothing
    st.recursing && return nothing   # e.g. a finalizer that locks, run from our allocation
    st.recursing = true
    try
        bt = backtrace()
        stats = get!(SiteStats, st.sites, bt)
        stats.count += 1
        if rl !== nothing
            ns = time_ns() - t0
            stats.total_ns += ns
            stats.max_ns = max(stats.max_ns, ns)
            length(stats.locks) < 1024 && push!(stats.locks, objectid(rl))
        end
    finally
        st.recursing = false
    end
    return nothing
end

# entry point for the cheap hook (no lock identity, no wait time)
_record_basic() = MODE[] == BASIC ? _record(UInt64(0), nothing) : nothing
# entry point for the deep `Base.lock` replacement
_record_deep(rl::Base.ReentrantLock, t0::UInt64) = MODE[] == DEEP ? _record(t0, rl) : nothing

# ---------------------------------------------------------------------------
# instrumentation
# ---------------------------------------------------------------------------

# Base's lock slow path already calls `Threads.inc_lock_conflict_count()` on every
# conflict, gated by `Threads.lock_profiling()`. Overwriting that tiny cold-path
# function is all we need for backtraces -- no copy of `slowlock` required.
function _install_hook()
    HOOK_INSTALLED[] && return nothing
    @eval Base.Threads function inc_lock_conflict_count()
        $(@__MODULE__)._record_basic()
        return atomic_add!(LOCK_CONFLICT_COUNT, 1)
    end
    HOOK_INSTALLED[] = true
    return nothing
end

# Deep mode needs the lock object and the wait duration, neither of which the hook
# can see, so here we do have to replace `Base.lock(::ReentrantLock)`. The slow path
# below is a copy of Base's, with the record call added; keep it in sync with
# `base/lock.jl` if it ever changes.
"""
    LockTracing.deep_supported()

Whether `start(deep=true)` works on the running Julia version. Deep mode copies Base's
lock slow path, so it is version specific; basic mode works everywhere.
"""
deep_supported() = all(n -> isdefined(Base, n), (:LOCKED_BIT, :PARKED_BIT, :MAX_SPIN_ITERS, :wait_no_relock))

function _install_deep()
    DEEP_INSTALLED[] && return nothing
    deep_supported() || error("LockTracing deep mode does not support this Julia version " *
                              "($VERSION): Base's lock internals differ. Use `start(deep=false)`.")
    @eval Base begin
        function _locktracing_slowlock(rl::ReentrantLock)
            Threads.lock_profiling() && Threads.inc_lock_conflict_count()
            t0 = time_ns()
            c = rl.cond_wait
            ct = current_task()
            iteration = 1
            while true
                state = @atomic :monotonic rl.havelock
                # Grab the lock if it isn't locked, even if there is a queue on it
                if state & LOCKED_BIT == 0
                    GC.disable_finalizers()
                    result = (@atomicreplace :acquire :monotonic rl.havelock state => (state | LOCKED_BIT))
                    if result.success
                        rl.reentrancy_cnt = 0x0000_0001
                        @atomic :release rl.locked_by = ct
                        $(@__MODULE__)._record_deep(rl, t0)
                        return
                    end
                    GC.enable_finalizers()
                    continue
                end

                if state & PARKED_BIT == 0
                    # If there is no queue, try spinning a few times
                    if iteration <= MAX_SPIN_ITERS
                        Base.yield()
                        iteration += 1
                        continue
                    end

                    # If still not locked, try setting the parked bit
                    @atomicreplace :monotonic :monotonic rl.havelock state => (state | PARKED_BIT)
                end

                # lock the `cond_wait`
                lock(c.lock)

                # Last check before we wait to make sure `unlock` did not win the race
                # to the `cond_wait` lock and cleared the parked bit
                state = @atomic :acquire rl.havelock
                if state != LOCKED_BIT | PARKED_BIT
                    unlock(c.lock)
                    continue
                end

                # It was locked, so now wait for the unlock to notify us
                wait_no_relock(c)

                # Loop back and try locking again
                iteration = 1
            end
        end

        @inline function lock(rl::ReentrantLock)
            trylock(rl) || _locktracing_slowlock(rl)
            return
        end
    end
    DEEP_INSTALLED[] = true
    return nothing
end

# ---------------------------------------------------------------------------
# control
# ---------------------------------------------------------------------------

"""
    LockTracing.start(; deep=false, reset=true)

Start recording a backtrace for every `ReentrantLock` conflict (i.e. every time a
`lock` call fails its fast path and has to wait).

- `deep=false` (default) hooks `Threads.inc_lock_conflict_count`, the cold-path
  function Base already calls on each conflict. Cheap, and it leaves `Base.lock`
  itself untouched. Records the conflict call site only.
- `deep=true` additionally replaces `Base.lock(::ReentrantLock)` with an
  instrumented copy, so the report can also show how long each site waited and how
  many distinct lock instances were involved. Higher overhead, and it depends on
  Base internals.

Both modes overwrite a method in Base, which invalidates code and prints a method
overwrite warning; the replacement stays installed for the rest of the session but
does nothing once [`stop`](@ref) is called.

!!! note
    The overwritten method only becomes visible in a new world age, so the traced
    workload must not run in the same top-level expression (or the same enclosing
    function call) as `start()`. At the REPL or in a script that is the normal case.
    If in doubt, use [`trace`](@ref) or [`@tracelocks`](@ref), which handle it.
"""
function start(; deep::Bool=false, reset::Bool=true)
    MODE[] == OFF || error("LockTracing is already running; call LockTracing.stop() first")
    reset && reset!()
    resize!(STATES[], 0)
    append!(STATES[], fill(nothing, Threads.maxthreadid()))
    if deep
        _install_deep()
    else
        _install_hook()
    end
    if !PROFILING_ENABLED[]
        Threads.lock_profiling(true)
        PROFILING_ENABLED[] = true
    end
    MODE[] = deep ? DEEP : BASIC
    return nothing
end

"""
    LockTracing.stop()

Stop recording. Collected data is kept; see [`report`](@ref) and [`results`](@ref).
"""
function stop()
    MODE[] = OFF
    if PROFILING_ENABLED[]
        Threads.lock_profiling(false)
        PROFILING_ENABLED[] = false
    end
    return nothing
end

"""
    LockTracing.reset!()

Discard all recorded conflicts.
"""
function reset!()
    for st in STATES[]
        st === nothing || empty!(st.sites)
    end
    DROPPED[] = 0
    return nothing
end

"""
    LockTracing.trace(f; deep=false)

Run `f()` with tracing enabled and return its value, then stop tracing. Previously
recorded conflicts are discarded first. Follow with [`report`](@ref).

`f` is called via `invokelatest` so that the instrumentation installed by
[`start`](@ref) is visible to it.
"""
function trace(f; deep::Bool=false)
    start(; deep)
    try
        return Base.invokelatest(f)
    finally
        stop()
    end
end

"""
    @tracelocks [deep=true] expr

Run `expr` with tracing enabled and return its value. Previously recorded conflicts
are discarded first. Follow with [`report`](@ref).

```julia
@tracelocks run_workload()
LockTracing.report()
```
"""
macro tracelocks(args...)
    isempty(args) && throw(ArgumentError("@tracelocks needs an expression"))
    ex = last(args)
    deep = false
    for opt in args[1:end-1]
        if opt isa Expr && opt.head === :(=) && opt.args[1] === :deep
            deep = opt.args[2]
        else
            throw(ArgumentError("unsupported option to @tracelocks: $opt"))
        end
    end
    return :($trace(() -> $(esc(ex)); deep=$(esc(deep))))
end

# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------

const SKIP_FRAMES = Set([:_record, :_record_basic, :_record_deep, :backtrace,
                         :inc_lock_conflict_count, :atomic_add!, :modifyfield!,
                         :slowlock, :_locktracing_slowlock, :lock])

# internal, but also `@lock`'s own `macro expansion` frame, which comes from lock.jl
_isinternal(f) = f.func in SKIP_FRAMES ||
    (f.func === Symbol("macro expansion") && basename(string(f.file)) == "lock.jl")

function _trim(frames)
    i = firstindex(frames)
    while i <= lastindex(frames) && _isinternal(frames[i])
        i += 1
    end
    return frames[i:end]
end

"""
    LockTracing.results(; trim=true)

Return the recorded conflicts, merged across threads and sorted by descending count.
Each entry is a `NamedTuple` with fields `count`, `total_ns`, `max_ns`, `nlocks` and
`stacktrace`. `total_ns`/`max_ns`/`nlocks` are only populated in `deep` mode.

`trim=false` keeps the internal `lock`/`slowlock` frames at the top of each stack.
"""
function results(; trim::Bool=true)
    # Aggregate on the symbolized stack: two conflicts at the same call site can have
    # different raw backtraces (different instruction pointers deeper in the stack).
    merged = Dict{Vector{Tuple{Symbol,Symbol,Int}},Tuple{SiteStats,Vector{StackTraces.StackFrame}}}()
    for st in STATES[]
        st === nothing && continue
        for (bt, s) in st.sites
            frames = StackTraces.stacktrace(bt)
            trim && (frames = _trim(frames))
            key = [(f.func, f.file, Int(f.line)) for f in frames]
            m, _ = get!(merged, key) do
                (SiteStats(), frames)
            end
            m.count += s.count
            m.total_ns += s.total_ns
            m.max_ns = max(m.max_ns, s.max_ns)
            union!(m.locks, s.locks)
        end
    end
    out = map(collect(values(merged))) do (s, frames)
        (; count=s.count, total_ns=s.total_ns, max_ns=s.max_ns,
           nlocks=length(s.locks), stacktrace=frames)
    end
    sort!(out; by=r -> r.count, rev=true)
    return out
end

_ns(ns) = ns < 1_000 ? "$(ns) ns" :
          ns < 1_000_000 ? string(round(ns / 1e3; digits=1), " μs") :
          ns < 1_000_000_000 ? string(round(ns / 1e6; digits=1), " ms") :
          string(round(ns / 1e9; digits=2), " s")

"""
    LockTracing.report([io=stdout]; maxsites=10, maxframes=typemax(Int))

Print the recorded lock conflicts, most contended call site first.

The full stack is printed for each site, with Base's internal `lock` frames trimmed off
the top. Pass `maxframes` to shorten it.
"""
function report(io::IO=stdout; maxsites::Integer=10, maxframes::Integer=typemax(Int))
    rs = results()
    total = sum(r -> r.count, rs; init=0)
    if total == 0
        println(io, "No lock conflicts recorded.")
        DROPPED[] > 0 && println(io, "($(DROPPED[]) conflicts dropped, see LockTracing docs)")
        return nothing
    end
    println(io, "$total lock conflict(s) at $(length(rs)) call site(s):")
    for r in first(rs, maxsites)
        pct = round(100 * r.count / total; digits=1)
        print(io, "\n  ", r.count, " conflict(s) (", pct, "%)")
        if r.total_ns > 0
            print(io, ", waited ", _ns(r.total_ns), " total / ", _ns(r.max_ns), " max")
        end
        r.nlocks > 0 && print(io, ", ", r.nlocks, " lock instance(s)")
        println(io)
        for f in first(r.stacktrace, maxframes)
            println(io, "    ", f)
        end
        length(r.stacktrace) > maxframes && println(io, "    ⋮")
    end
    length(rs) > maxsites && println(io, "\n  ⋮ ($(length(rs) - maxsites) more call site(s))")
    if DROPPED[] > 0
        println(io, "\n$(DROPPED[]) conflict(s) dropped: they happened on threads that did not " *
                    "exist when tracing started.")
    end
    return nothing
end

end # module
