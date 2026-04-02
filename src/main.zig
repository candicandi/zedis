const std = @import("std");
const Server = @import("server.zig");
const Config = @import("config.zig");
const builtin = @import("builtin");

const log = std.log.scoped(.main);

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // std.Io.Evented is the platform-selected fiber/coroutine backend:
    //   Linux                                  → std.Io.Uring   (io_uring)
    //   macOS / iOS / tvOS / watchOS / …       → std.Io.Dispatch (Grand Central Dispatch)
    //   FreeBSD / NetBSD / DragonFly / OpenBSD → std.Io.Kqueue
    //
    // All connections are handled as coroutines on the main thread —
    // no per-connection OS threads.
    if (std.Io.Evented == void) @compileError("platform has no fiber-based Io backend");

    var evented: std.Io.Evented = undefined;
    switch (comptime builtin.os.tag) {
        .linux => try std.Io.Uring.init(&evented, allocator, .{
            // SQ ring size = 2^log2_ring_entries. Default is 3 (only 8 entries).
            .log2_ring_entries = 12, // 2^12 = 4096
            // Additional worker threads beyond main. 0 = single-threaded event loop.
            .thread_limit = 0,
        }),
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => {
            // Dispatch manages its own thread pool; no explicit thread_limit.
            try std.Io.Dispatch.init(&evented, allocator, .{});
        },
        .dragonfly, .freebsd, .netbsd, .openbsd => try std.Io.Kqueue.init(&evented, allocator, .{
            // Total threads including main. Must not be 0. 1 = single-threaded.
            .n_threads = 1,
        }),
        else => @compileError("platform has no fiber-based Io backend"),
    }

    defer std.Io.Evented.deinit(&evented);
    const io = evented.io();

    const cfg = Config.readConfig(allocator, io, init.minimal.args) catch |err| {
        log.err("Failed to read config: {s}", .{@errorName(err)});
        return err;
    };

    // Create and start the server with configuration
    var redis_server = Server.initWithConfig(allocator, cfg.bind, cfg.port, cfg, io) catch |err| {
        log.err("Failed to initialize server: {s}", .{@errorName(err)});
        return err;
    };
    defer redis_server.deinit();

    log.info("Zedis server listening on {s}:{d}", .{ cfg.bind, cfg.port });

    // Log config
    log.info("Configuration loaded:", .{});
    log.info("  Network: {s}:{d}", .{ cfg.bind, cfg.port });
    log.info("  AOF enabled: {}", .{cfg.appendonly});
    log.info("  Memory budget: {d} MB", .{cfg.totalMemoryBudget() / (1024 * 1024)});
    log.info("  Max clients: {d}", .{cfg.max_clients});
    log.info("  Eviction policy: {s}", .{@tagName(cfg.eviction_policy)});
    if (cfg.requiresAuth()) {
        log.info("  Auth: enabled", .{});
    } else {
        log.info("  Auth: disabled", .{});
    }

    redis_server.listen() catch |err| {
        log.err("Server error: {s}", .{@errorName(err)});
        return err;
    };
}
