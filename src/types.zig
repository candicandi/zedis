const std = @import("std");
const Connection = std.net.Server.Connection;
const Allocator = std.mem.Allocator;
const time = std.time;
const Store = @import("./store.zig").Store;
const CommandRegistry = @import("./commands/registry.zig").CommandRegistry;
const server_config = @import("./server_config.zig");
const KeyValueAllocator = @import("./kv_allocator.zig").KeyValueAllocator;
const Server = @import("./server.zig").Server;

pub const ConnectionContext = struct {
    server: *Server,
    connection: std.net.Server.Connection,
};
