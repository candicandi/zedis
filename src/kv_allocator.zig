const std = @import("std");
const Config = @import("config.zig");
const Store = @import("store.zig").Store;
const Clock = @import("clock.zig");
const mem = std.mem;

const KeyValueAllocator = @This();

base_allocator: mem.Allocator,
memory_used: std.atomic.Value(usize),
memory_budget: usize,
eviction_policy: Config.EvictionPolicy,
store: ?*Store = null,

pub fn init(base_allocator: mem.Allocator, budget: usize, eviction_policy: Config.EvictionPolicy) !KeyValueAllocator {
    return .{
        .base_allocator = base_allocator,
        .memory_used = .init(0),
        .memory_budget = budget,
        .eviction_policy = eviction_policy,
        .store = null,
    };
}

pub fn attachStore(self: *KeyValueAllocator, store: *Store) void {
    self.store = store;
}

pub fn deinit(_: *KeyValueAllocator) void {}

pub fn allocator(self: *KeyValueAllocator) mem.Allocator {
    return .{
        .ptr = self,
        .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .free = free,
            .remap = mem.Allocator.noRemap,
        },
    };
}

fn ensureBudget(self: *KeyValueAllocator, additional_bytes: usize) bool {
    if (additional_bytes > self.memory_budget) return false;

    while (self.memory_used.load(.acquire) > self.memory_budget - additional_bytes) {
        if (self.eviction_policy == .noeviction) return false;

        const store = self.store orelse {
            std.debug.assert(false);
            return false;
        };
        if (!store.evictOne(self.eviction_policy)) return false;
    }

    return true;
}

fn alloc(ctx: *anyopaque, len: usize, ptr_align: mem.Alignment, ret_addr: usize) ?[*]u8 {
    const self: *KeyValueAllocator = @ptrCast(@alignCast(ctx));

    if (!self.ensureBudget(len)) return null;

    const ptr = self.base_allocator.rawAlloc(len, ptr_align, ret_addr) orelse return null;
    _ = self.memory_used.fetchAdd(len, .monotonic);
    return ptr;
}

fn resize(ctx: *anyopaque, buf: []u8, buf_align: mem.Alignment, new_len: usize, ret_addr: usize) bool {
    const self: *KeyValueAllocator = @ptrCast(@alignCast(ctx));

    if (new_len > buf.len) {
        if (!self.ensureBudget(new_len - buf.len)) return false;
    }

    if (!self.base_allocator.rawResize(buf, buf_align, new_len, ret_addr)) return false;

    if (new_len > buf.len) {
        _ = self.memory_used.fetchAdd(new_len - buf.len, .monotonic);
    } else {
        _ = self.memory_used.fetchSub(buf.len - new_len, .monotonic);
    }
    return true;
}

fn free(ctx: *anyopaque, buf: []u8, buf_align: mem.Alignment, ret_addr: usize) void {
    const self: *KeyValueAllocator = @ptrCast(@alignCast(ctx));

    self.base_allocator.rawFree(buf, buf_align, ret_addr);
    _ = self.memory_used.fetchSub(buf.len, .monotonic);
}

pub fn getMemoryUsage(self: *KeyValueAllocator) usize {
    return self.memory_used.load(.acquire);
}

pub fn getMemoryBudget(self: *KeyValueAllocator) usize {
    return self.memory_budget;
}

test "KeyValueAllocator tracks alloc free and resize" {
    const testing = std.testing;

    var kv = try KeyValueAllocator.init(testing.allocator, 1024, .noeviction);
    defer kv.deinit();

    const kv_alloc = kv.allocator();

    var slice = try kv_alloc.alloc(u8, 128);
    defer kv_alloc.free(slice);

    try testing.expectEqual(@as(usize, 128), kv.getMemoryUsage());

    slice = try kv_alloc.realloc(slice, 256);
    try testing.expectEqual(@as(usize, 256), kv.getMemoryUsage());

    slice = try kv_alloc.realloc(slice, 64);
    try testing.expectEqual(@as(usize, 64), kv.getMemoryUsage());
}

test "KeyValueAllocator returns out of memory without eviction" {
    const testing = std.testing;

    var kv = try KeyValueAllocator.init(testing.allocator, 2048, .noeviction);
    defer kv.deinit();

    var clock = Clock.init(testing.io, 0);
    var store = try Store.init(kv.allocator(), testing.io, &clock, .{ .initial_capacity = 4 });
    defer store.deinit();

    kv.attachStore(&store);

    var value: [256]u8 = undefined;
    @memset(&value, 'x');

    var key_buf: [32]u8 = undefined;
    var saw_oom = false;
    var i: usize = 0;
    while (i < 128) : (i += 1) {
        const key = try std.fmt.bufPrint(&key_buf, "key-{d}", .{i});
        store.set(key, value[0..]) catch |err| {
            try testing.expectEqual(error.OutOfMemory, err);
            saw_oom = true;
            break;
        };
    }

    try testing.expect(saw_oom);
    try testing.expect(kv.getMemoryUsage() <= kv.getMemoryBudget());
}

test "KeyValueAllocator evicts under allkeys_lru pressure" {
    const testing = std.testing;

    var kv = try KeyValueAllocator.init(testing.allocator, 4096, .allkeys_lru);
    defer kv.deinit();

    var clock = Clock.init(testing.io, 0);
    var store = try Store.init(kv.allocator(), testing.io, &clock, .{ .initial_capacity = 4 });
    defer store.deinit();

    kv.attachStore(&store);

    var value: [256]u8 = undefined;
    @memset(&value, 'y');

    var key_buf: [32]u8 = undefined;
    var last_key: [32]u8 = undefined;
    var last_key_len: usize = 0;

    var i: usize = 0;
    while (i < 64) : (i += 1) {
        const key = try std.fmt.bufPrint(&key_buf, "key-{d}", .{i});
        @memcpy(last_key[0..key.len], key);
        last_key_len = key.len;
        try store.set(key, value[0..]);
    }

    try testing.expect(kv.getMemoryUsage() <= kv.getMemoryBudget());
    try testing.expect(store.size() < 64);
    try testing.expect(store.get(last_key[0..last_key_len]) != null);
    try testing.expect(store.get("key-0") == null);
}
