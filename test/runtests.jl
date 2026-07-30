using LockTracing
using Test

# A contended lock, with two distinct call sites.
const L = ReentrantLock()

hot_site() = @lock L (yield(); nothing)
other_site() = @lock L (yield(); nothing)

function contend(f, n=200)
    @sync for _ in 1:4
        Threads.@spawn for _ in 1:n
            f()
        end
    end
end

function workload()
    contend(hot_site, 200)
    contend(other_site, 50)
end

@testset "basic mode" begin
    @tracelocks workload()
    rs = LockTracing.results()
    @test !isempty(rs)
    @test sum(r -> r.count, rs) > 0
    # the top site's stack should start at our code, not inside Base's lock internals
    top = first(rs)
    @test !isempty(top.stacktrace)
    @test occursin("runtests.jl", string(top.stacktrace[1].file))
    @test !any(f -> f.func in (:lock, :slowlock, :inc_lock_conflict_count), top.stacktrace)
    @test top.total_ns == 0   # no timing in basic mode
    io = IOBuffer()
    LockTracing.report(io)
    @test occursin("lock conflict", String(take!(io)))
end

@testset "reset!/stop" begin
    LockTracing.reset!()
    @test isempty(LockTracing.results())
    workload()   # not tracing
    @test isempty(LockTracing.results())
end

@testset "deep mode" begin
    if !LockTracing.deep_supported()
        @info "deep mode not supported on Julia $VERSION, skipping"
        @test_throws ErrorException LockTracing.start(deep=true)
    else
        @tracelocks deep = true workload()
        rs = LockTracing.results()
        @test !isempty(rs)
        @test any(r -> r.total_ns > 0, rs)
        @test all(r -> r.max_ns <= r.total_ns, rs)
        @test any(r -> r.nlocks >= 1, rs)
        # `Base.lock` must still work correctly after being replaced
        n = Threads.Atomic{Int}(0)
        l = ReentrantLock()
        @sync for _ in 1:4
            Threads.@spawn for _ in 1:1000
                @lock l Threads.atomic_add!(n, 1)
            end
        end
        @test n[] == 4000
        @test Base.@lock_conflicts(contend(hot_site, 50)) > 0
    end
end

@testset "double start errors" begin
    LockTracing.start()
    @test_throws ErrorException LockTracing.start()
    LockTracing.stop()
end
