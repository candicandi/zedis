const std = @import("std");
const storeModule = @import("../store.zig");
const Store = storeModule.Store;
const ZedisObject = storeModule.ZedisObject;
const Client = @import("../client.zig").Client;
const Value = @import("../parser.zig").Value;
const resp = @import("./resp.zig");

pub fn set(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    const key = args[1].asSlice();
    const value = args[2].asSlice();

    try store.set(key, value);

    try resp.writeOK(writer);
}

pub fn get(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    const key = args[1].asSlice();
    const value = store.get(key);

    if (value) |v| {
        switch (v.value) {
            .string => |s| try resp.writeBulkString(writer, s),
            .short_string => |ss| try resp.writeBulkString(writer, ss.asSlice()),
            .int => |i| {
                try resp.writeIntBulkString(writer, i);
            },
            .list => return error.WrongType,
        }
    } else {
        try resp.writeNull(writer);
    }
}

pub fn incr(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    const key = args[1].asSlice();
    const new_value = try incrDecr(store, key, 1);

    try resp.writeIntBulkString(writer, new_value);
}

pub fn decr(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    const key = args[1].asSlice();
    const new_value = try incrDecr(store, key, -1);

    try resp.writeIntBulkString(writer, new_value);
}

fn incrDecr(store_ptr: *Store, key: []const u8, value: i64) !i64 {
    const current_value = store_ptr.map.get(key);
    if (current_value) |v| {
        var new_value: i64 = undefined;

        switch (v.value) {
            .string => |_| {
                const intValue = std.fmt.parseInt(i64, v.value.string, 10) catch {
                    return error.ValueNotInteger;
                };
                new_value = std.math.add(i64, intValue, value) catch {
                    return error.ValueNotInteger;
                };
            },
            .int => |_| {
                new_value = std.math.add(i64, v.value.int, value) catch {
                    return error.ValueNotInteger;
                };
            },
            else => return error.WrongType,
        }

        const int_object = ZedisObject{ .value = .{ .int = new_value } };
        try store_ptr.putObject(key, int_object);

        return new_value;
    } else {
        // Redis behavior: non-existent key is treated as 0, then the operation is applied
        const new_value = std.math.add(i64, 0, value) catch {
            return error.ValueNotInteger;
        };
        try store_ptr.setInt(key, new_value);
        return new_value;
    }
}

pub fn del(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    var deleted: u32 = 0;
    for (args[1..]) |key| {
        if (store.delete(key.asSlice())) {
            deleted += 1;
        }
    }

    try resp.writeInt(writer, deleted);
}

pub fn expire(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    const key = args[1].asSlice();
    const expiration_seconds = args[2].asInt() catch {
        return resp.writeInt(writer, 0);
    };

    const result = if (expiration_seconds < 0)
        store.delete(key)
    else
        store.expire(key, std.time.milliTimestamp() + (expiration_seconds * 1000)) catch false;

    try resp.writeInt(writer, @intFromBool(result));
}

pub fn expireAt(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    const key = args[1].asSlice();
    const current_time = std.time.milliTimestamp();
    const expiration_timestamp = args[2].asInt() catch {
        return resp.writeInt(writer, 0);
    };

    const result = if (expiration_timestamp <= current_time)
        store.delete(key)
    else
        store.expire(key, expiration_timestamp) catch false;

    try resp.writeInt(writer, @intFromBool(result));
}

const testing = std.testing;

test "incrDecr helper function with string integer" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.set("key1", "100");

    const result = try incrDecr(&store, "key1", 50);
    try testing.expectEqual(@as(i64, 150), result);

    const stored_value = store.get("key1");
    try testing.expect(stored_value != null);
    try testing.expectEqual(@as(i64, 150), stored_value.?.value.int);
}

test "incrDecr helper function with integer overflow" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setInt("key1", std.math.maxInt(i64));

    const result = incrDecr(&store, "key1", 1);
    try testing.expectError(error.ValueNotInteger, result);
}

test "incrDecr helper function with non-existent key" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    const result = try incrDecr(&store, "nonexistent", 1);
    try testing.expectEqual(@as(i64, 1), result);

    const stored_value = store.get("nonexistent");
    try testing.expect(stored_value != null);
    try testing.expectEqual(@as(i64, 1), stored_value.?.value.int);
}
