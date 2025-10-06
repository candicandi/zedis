const std = @import("std");
const server = @import("server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const host = "127.0.0.1";
    const port = 6379;

    // Create and start the server.
    var redis_server = server.Server.init(allocator, host, port) catch |err| {
        std.log.err("Error server init: {s}", .{@errorName(err)});
        return;
    };
    defer redis_server.deinit();

    std.log.info("Zig Redis server listening on {s}:{d}", .{ host, port });

    redis_server.listen() catch |err| {
        std.log.err("Error on server {any}", .{@errorName(err)});
    };
}
