const std = @import("std");
const Store = @import("../store.zig").Store;
const KeyValueAllocator = @import("../kv_allocator.zig");
const bench_runner = @import("bench_runner.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Clock = @import("../clock.zig");

const BenchContext = struct {
    threaded: Io.Threaded,
    clock: Clock,
    store: Store,
    keys: [][]const u8,
    values: [][]const u8,
    allocator: Allocator,
    counter: usize = 0,

    pub fn init(allocator: Allocator, key_count: usize) !BenchContext {
        var ctx = BenchContext{
            .threaded = .init_single_threaded,
            .clock = undefined,
            .store = undefined,
            .keys = try allocator.alloc([]const u8, key_count),
            .values = try allocator.alloc([]const u8, key_count),
            .allocator = allocator,
        };
        errdefer allocator.free(ctx.keys);
        errdefer allocator.free(ctx.values);

        ctx.clock = Clock.init(ctx.threaded.io(), 0);
        ctx.store = try Store.init(allocator, ctx.threaded.io(), &ctx.clock, .{});
        errdefer ctx.store.deinit();

        for (ctx.keys, 0..) |*key, i| {
            key.* = try std.fmt.allocPrint(allocator, "key:{d:0>8}", .{i});
        }

        for (ctx.values, 0..) |*value, i| {
            value.* = try std.fmt.allocPrint(allocator, "value:{d:0>8}:data", .{i});
        }

        return ctx;
    }

    /// Wrapper for benchmark runner that uses default key count
    pub fn initDefault(allocator: Allocator) !BenchContext {
        return init(allocator, 10_000);
    }

    pub fn deinit(self: *BenchContext) void {
        for (self.keys) |key| {
            self.allocator.free(key);
        }
        for (self.values) |value| {
            self.allocator.free(value);
        }
        self.allocator.free(self.keys);
        self.allocator.free(self.values);
        self.store.deinit();
    }
};

/// Benchmark SET operations
fn benchSet(ctx: *BenchContext) !void {
    const idx = @mod(ctx.counter, ctx.keys.len);
    try ctx.store.set(ctx.keys[idx], ctx.values[idx]);
    ctx.counter += 1;
}

/// Benchmark GET operations (after pre-populating)
fn benchGet(ctx: *BenchContext) !void {
    const idx = @mod(ctx.counter, ctx.keys.len);
    _ = ctx.store.get(ctx.keys[idx]);
    ctx.counter += 1;
}

/// Benchmark mixed read-write workload (70% reads, 30% writes)
fn benchMixed(ctx: *BenchContext) !void {
    const counter = ctx.counter;
    ctx.counter += 1;
    const idx = @mod(counter, ctx.keys.len);

    if (@mod(counter, 10) < 7) {
        // 70% reads
        _ = ctx.store.get(ctx.keys[idx]);
    } else {
        // 30% writes
        try ctx.store.set(ctx.keys[idx], ctx.values[idx]);
    }
}

/// Benchmark DELETE operations
fn benchDelete(ctx: *BenchContext) !void {
    const idx = @mod(ctx.counter, ctx.keys.len);
    _ = ctx.store.delete(ctx.keys[idx]);
    ctx.counter += 1;
}

/// Benchmark EXISTS operations
fn benchExists(ctx: *BenchContext) !void {
    const idx = @mod(ctx.counter, ctx.keys.len);
    _ = ctx.store.exists(ctx.keys[idx]);
    ctx.counter += 1;
}

/// Benchmark short string operations (inline storage optimization)
fn benchShortString(ctx: *BenchContext) !void {
    const idx = @mod(ctx.counter, ctx.keys.len);
    const short_value = "short";
    try ctx.store.set(ctx.keys[idx], short_value);
    _ = ctx.store.get(ctx.keys[idx]);
    ctx.counter += 1;
}

/// Benchmark integer operations (automatic type optimization)
fn benchInteger(ctx: *BenchContext) !void {
    const idx = @mod(ctx.counter, ctx.keys.len);
    try ctx.store.setInt(ctx.keys[idx], @as(i64, @intCast(idx)));
    _ = ctx.store.get(ctx.keys[idx]);
    ctx.counter += 1;
}

const EvictionBenchContext = struct {
    threaded: Io.Threaded,
    clock: Clock,
    kv_allocator: KeyValueAllocator,
    store: Store,
    keys: [][]const u8,
    values: [][]const u8,
    allocator: Allocator,
    counter: usize = 0,

    pub fn initDefault(allocator: Allocator) !EvictionBenchContext {
        const key_count = 256;
        const value_len = 1024;

        var ctx = EvictionBenchContext{
            .threaded = .init_single_threaded,
            .clock = undefined,
            .kv_allocator = undefined,
            .store = undefined,
            .keys = try allocator.alloc([]const u8, key_count),
            .values = try allocator.alloc([]const u8, key_count),
            .allocator = allocator,
        };
        errdefer allocator.free(ctx.keys);
        errdefer allocator.free(ctx.values);

        ctx.clock = Clock.init(ctx.threaded.io(), 0);
        ctx.kv_allocator = try KeyValueAllocator.init(allocator, 256 * 1024, .allkeys_lru);
        errdefer ctx.kv_allocator.deinit();

        ctx.store = try Store.init(ctx.kv_allocator.allocator(), ctx.threaded.io(), &ctx.clock, .{
            .initial_capacity = 64,
        });
        errdefer ctx.store.deinit();

        ctx.kv_allocator.attachStore(&ctx.store);

        for (ctx.keys, 0..) |*key, i| {
            key.* = try std.fmt.allocPrint(allocator, "evict-key:{d:0>4}", .{i});
        }

        for (ctx.values, 0..) |*value, i| {
            const buffer = try allocator.alloc(u8, value_len);
            @memset(buffer, @as(u8, @truncate(i)));
            value.* = buffer;
        }

        return ctx;
    }

    pub fn deinit(self: *EvictionBenchContext) void {
        for (self.keys) |key| {
            self.allocator.free(key);
        }
        for (self.values) |value| {
            self.allocator.free(value);
        }
        self.allocator.free(self.keys);
        self.allocator.free(self.values);
        self.store.deinit();
        self.kv_allocator.deinit();
    }
};

fn benchEvictionPressure(ctx: *EvictionBenchContext) !void {
    const idx = @mod(ctx.counter, ctx.keys.len);
    const read_idx = @mod(ctx.counter / 2, ctx.keys.len);
    ctx.counter += 1;

    try ctx.store.set(ctx.keys[idx], ctx.values[idx]);
    _ = ctx.store.get(ctx.keys[read_idx]);
}

pub fn runAllBenchmarks(allocator: Allocator) !void {
    var results: std.ArrayList(bench_runner.BenchmarkResult) = .empty;

    defer results.deinit(allocator);

    std.debug.print("\n=== Store Micro-Benchmarks ===\n\n", .{});

    // Benchmark 1: Pure SET operations
    {
        const result = try bench_runner.runBenchmarkAdvanced(
            allocator,
            .{
                .name = "Store.set (sequential)",
                .iterations = 500_000,
                .warmup_iterations = 50_000,
                .track_latency = true,
                .track_memory = true,
            },
            BenchContext,
            BenchContext.initDefault,
            BenchContext.deinit,
            benchSet,
        );
        try results.append(allocator, result);
        bench_runner.printResult(result);
    }

    // Benchmark 2: Pure GET operations (pre-populate store first)
    {
        var ctx = try BenchContext.init(allocator, 10_000);
        defer ctx.deinit();

        // Pre-populate with data
        for (ctx.keys, ctx.values) |key, value| {
            try ctx.store.set(key, value);
        }

        const result = try bench_runner.runBenchmarkAdvanced(
            allocator,
            .{
                .name = "Store.get (cached)",
                .iterations = 500_000,
                .warmup_iterations = 50_000,
                .track_latency = true,
                .track_memory = false, // Already populated
            },
            BenchContext,
            BenchContext.initDefault,
            BenchContext.deinit,
            benchGet,
        );
        try results.append(allocator, result);
        bench_runner.printResult(result);
    }

    // Benchmark 3: Mixed workload (70% reads, 30% writes)
    {
        var ctx = try BenchContext.init(allocator, 10_000);
        defer ctx.deinit();

        // Pre-populate with data
        for (ctx.keys, ctx.values) |key, value| {
            try ctx.store.set(key, value);
        }

        const result = try bench_runner.runBenchmarkAdvanced(
            allocator,
            .{
                .name = "Store.mixed (70R/30W)",
                .iterations = 500_000,
                .warmup_iterations = 50_000,
                .track_latency = true,
                .track_memory = true,
            },
            BenchContext,
            BenchContext.initDefault,
            BenchContext.deinit,
            benchMixed,
        );
        try results.append(allocator, result);
        bench_runner.printResult(result);
    }

    // Benchmark 4: Short string optimization
    {
        const result = try bench_runner.runBenchmarkAdvanced(
            allocator,
            .{
                .name = "Store.set (short strings)",
                .iterations = 500_000,
                .warmup_iterations = 50_000,
                .track_latency = true,
                .track_memory = true,
            },
            BenchContext,
            BenchContext.initDefault,
            BenchContext.deinit,
            benchShortString,
        );
        try results.append(allocator, result);
        bench_runner.printResult(result);
    }

    // Benchmark 5: Integer type optimization
    {
        const result = try bench_runner.runBenchmarkAdvanced(
            allocator,
            .{
                .name = "Store.setInt (integers)",
                .iterations = 500_000,
                .warmup_iterations = 50_000,
                .track_latency = true,
                .track_memory = true,
            },
            BenchContext,
            BenchContext.initDefault,
            BenchContext.deinit,
            benchInteger,
        );
        try results.append(allocator, result);
        bench_runner.printResult(result);
    }

    // Benchmark 6: EXISTS operations
    {
        var ctx = try BenchContext.init(allocator, 10_000);
        defer ctx.deinit();

        // Pre-populate with data
        for (ctx.keys, ctx.values) |key, value| {
            try ctx.store.set(key, value);
        }

        const result = try bench_runner.runBenchmarkAdvanced(
            allocator,
            .{
                .name = "Store.exists",
                .iterations = 500_000,
                .warmup_iterations = 50_000,
                .track_latency = true,
                .track_memory = false,
            },
            BenchContext,
            BenchContext.initDefault,
            BenchContext.deinit,
            benchExists,
        );
        try results.append(allocator, result);
        bench_runner.printResult(result);
    }

    // Benchmark 7: DELETE operations
    {
        const result = try bench_runner.runBenchmarkAdvanced(
            allocator,
            .{
                .name = "Store.delete",
                .iterations = 50_000, // Increased for better accuracy
                .warmup_iterations = 5_000,
                .track_latency = true,
                .track_memory = true,
            },
            BenchContext,
            BenchContext.initDefault,
            BenchContext.deinit,
            benchDelete,
        );
        try results.append(allocator, result);
        bench_runner.printResult(result);
    }

    // Benchmark 8: Budgeted eviction pressure
    {
        const result = try bench_runner.runBenchmarkAdvanced(
            allocator,
            .{
                .name = "Store.eviction (budgeted)",
                .iterations = 50_000,
                .warmup_iterations = 5_000,
                .track_latency = true,
                .track_memory = true,
            },
            EvictionBenchContext,
            EvictionBenchContext.initDefault,
            EvictionBenchContext.deinit,
            benchEvictionPressure,
        );
        try results.append(allocator, result);
        bench_runner.printResult(result);
    }

    // Print summary table
    bench_runner.printResults(results.items);
}
