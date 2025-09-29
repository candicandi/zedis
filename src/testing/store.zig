const std = @import("std");
const Store = @import("../store.zig").Store;
const ZedisObject = @import("../store.zig").ZedisObject;
const ZedisValue = @import("../store.zig").ZedisValue;
const ValueType = @import("../store.zig").ValueType;
const StoreError = @import("../store.zig").StoreError;
const testing = std.testing;

test "Store init and deinit" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try testing.expectEqual(@as(u32, 0), store.size());
}

test "Store setString and get" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("key1", "hello");
    try testing.expectEqual(@as(u32, 1), store.size());

    const result = store.get("key1");
    try testing.expect(result != null);
    try testing.expectEqualStrings("hello", result.?.value.string);
    try testing.expect(result.?.expiration == null);
}

test "Store setInt and get" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setInt("counter", 42);
    try testing.expectEqual(@as(u32, 1), store.size());

    const result = store.get("counter");
    try testing.expect(result != null);
    try testing.expectEqual(@as(i64, 42), result.?.value.int);
    try testing.expect(result.?.expiration == null);
}

test "Store setObject with ZedisObject" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    const obj = ZedisObject{ .value = .{ .string = "test" }, .expiration = 12345 };
    try store.setObject("key1", obj);

    const result = store.get("key1");
    try testing.expect(result != null);
    try testing.expectEqualStrings("test", result.?.value.string);
    try testing.expectEqual(@as(i64, 12345), result.?.expiration.?);
}

test "Store getString with string value" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("key1", "hello world");

    const result = try store.getString(allocator, "key1");
    try testing.expect(result != null);
    try testing.expectEqualStrings("hello world", result.?);
}

test "Store getString with integer value" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setInt("counter", -123);

    const result = try store.getString(allocator, "counter");
    try testing.expect(result != null);
    try testing.expectEqualStrings("-123", result.?);
}

test "Store getString with non-existing key" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    const result = try store.getString(allocator, "nonexistent");
    try testing.expect(result == null);
}

test "Store getInt with integer value" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setInt("counter", 999);

    const result = try store.getInt("counter");
    try testing.expect(result != null);
    try testing.expectEqual(@as(i64, 999), result.?);
}

test "Store getInt with string value" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("number", "456");

    const result = try store.getInt("number");
    try testing.expect(result != null);
    try testing.expectEqual(@as(i64, 456), result.?);
}

test "Store getInt with invalid string" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("text", "hello");

    try testing.expectError(error.NotAnInteger, store.getInt("text"));
}

test "Store getInt with non-existing key" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    const result = try store.getInt("nonexistent");
    try testing.expect(result == null);
}

test "Store delete existing key" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("key1", "value1");
    try testing.expectEqual(@as(u32, 1), store.size());
    try testing.expect(store.exists("key1"));

    const deleted = store.delete("key1");
    try testing.expect(deleted);
    try testing.expectEqual(@as(u32, 0), store.size());
    try testing.expect(!store.exists("key1"));
}

test "Store delete non-existing key" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    const deleted = store.delete("nonexistent");
    try testing.expect(!deleted);
}

test "Store exists" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try testing.expect(!store.exists("key1"));

    try store.setString("key1", "value1");
    try testing.expect(store.exists("key1"));

    _ = store.delete("key1");
    try testing.expect(!store.exists("key1"));
}

test "Store getType" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try testing.expect(store.getType("nonexistent") == null);

    try store.setString("str_key", "hello");
    try testing.expectEqual(ValueType.string, store.getType("str_key").?);

    try store.setInt("int_key", 42);
    try testing.expectEqual(ValueType.int, store.getType("int_key").?);
}

test "Store overwrite existing key" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("key1", "original");
    try testing.expectEqual(@as(u32, 1), store.size());

    const result1 = store.get("key1");
    try testing.expect(result1 != null);
    try testing.expectEqualStrings("original", result1.?.value.string);

    try store.setString("key1", "updated");
    try testing.expectEqual(@as(u32, 1), store.size());

    const result2 = store.get("key1");
    try testing.expect(result2 != null);
    try testing.expectEqualStrings("updated", result2.?.value.string);
}

test "Store overwrite string with integer" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("key1", "hello");
    try testing.expectEqual(ValueType.string, store.getType("key1").?);

    try store.setInt("key1", 123);
    try testing.expectEqual(ValueType.int, store.getType("key1").?);
    try testing.expectEqual(@as(i64, 123), store.get("key1").?.value.int);
}

test "Store overwrite integer with string" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setInt("key1", 456);
    try testing.expectEqual(ValueType.int, store.getType("key1").?);

    try store.setString("key1", "world");
    try testing.expectEqual(ValueType.string, store.getType("key1").?);
    try testing.expectEqualStrings("world", store.get("key1").?.value.string);
}

test "Store expire functionality" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("key1", "value1");
    try testing.expect(!store.isExpired("key1"));

    const success = try store.expire("key1", 12345);
    try testing.expect(success);
    try testing.expect(store.isExpired("key1"));

    const obj = store.get("key1");
    try testing.expect(obj != null);
    try testing.expectEqual(@as(i64, 12345), obj.?.expiration.?);
}

test "Store expire non-existing key" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    const success = try store.expire("nonexistent", 12345);
    try testing.expect(!success);
}

test "Store delete removes from expiration map" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("key1", "value1");
    _ = try store.expire("key1", 12345);

    const deleted = store.delete("key1");
    try testing.expect(deleted);
    try testing.expect(!store.isExpired("key1"));
}

test "Store multiple keys with different types" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("str1", "hello");
    try store.setString("str2", "world");
    try store.setInt("int1", 123);
    try store.setInt("int2", -456);

    try testing.expectEqual(@as(u32, 4), store.size());

    try testing.expectEqualStrings("hello", store.get("str1").?.value.string);
    try testing.expectEqualStrings("world", store.get("str2").?.value.string);
    try testing.expectEqual(@as(i64, 123), store.get("int1").?.value.int);
    try testing.expectEqual(@as(i64, -456), store.get("int2").?.value.int);
}

test "Store empty string values" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("empty", "");

    const result = store.get("empty");
    try testing.expect(result != null);
    try testing.expectEqualStrings("", result.?.value.string);

    const str_result = try store.getString(allocator, "empty");
    try testing.expect(str_result != null);
    try testing.expectEqualStrings("", str_result.?);
}

test "Store zero integer values" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setInt("zero", 0);

    const result = store.get("zero");
    try testing.expect(result != null);
    try testing.expectEqual(@as(i64, 0), result.?.value.int);

    const int_result = try store.getInt("zero");
    try testing.expect(int_result != null);
    try testing.expectEqual(@as(i64, 0), int_result.?);

    const str_result = try store.getString(allocator, "zero");
    try testing.expect(str_result != null);
    try testing.expectEqualStrings("0", str_result.?);
}

test "Store createList and getList" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try testing.expect(try store.getList("mylist") == null);

    const list = try store.createList("mylist");
    try testing.expectEqual(@as(usize, 0), list.len());

    const retrieved_list = try store.getList("mylist");
    try testing.expect(retrieved_list != null);
    try testing.expectEqual(@as(usize, 0), retrieved_list.?.len());
}

test "Store list append and insert operations" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    const list = try store.createList("test_append_insert");

    try testing.expectEqual(@as(usize, 0), list.len());

    try list.append(.{ .string = "first" });
    try testing.expectEqual(@as(usize, 1), list.len());
    try testing.expectEqualStrings("first", list.getByIndex(0).?.string);

    try list.append(.{ .string = "second" });
    try testing.expectEqual(@as(usize, 2), list.len());
    try testing.expectEqualStrings("second", list.getByIndex(1).?.string);

    try list.prepend(.{ .string = "zero" });
    try testing.expectEqual(@as(usize, 3), list.len());
    try testing.expectEqualStrings("zero", list.getByIndex(0).?.string);
    try testing.expectEqualStrings("first", list.getByIndex(1).?.string);
    try testing.expectEqualStrings("second", list.getByIndex(2).?.string);
}

test "Store list with mixed value types" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    const list = try store.createList("test_mixed_values");

    try list.append(.{ .string = "hello" });
    try list.append(.{ .int = 42 });
    try list.append(.{ .string = "world" });

    try testing.expectEqual(@as(usize, 3), list.len());
    try testing.expectEqualStrings("hello", list.getByIndex(0).?.string);
    try testing.expectEqual(@as(i64, 42), list.getByIndex(1).?.int);
    try testing.expectEqualStrings("world", list.getByIndex(2).?.string);
}

test "Store getList with wrong type" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("notalist", "hello");

    const list = store.getList("notalist");
    try testing.expect(list == StoreError.WrongType);
}

test "Store list type checking" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    _ = try store.createList("mylist");
    try testing.expectEqual(ValueType.list, store.getType("mylist").?);
}

test "Store overwrite string with list" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    try store.setString("key1", "hello");
    try testing.expectEqual(ValueType.string, store.getType("key1").?);

    _ = try store.createList("key1");
    try testing.expectEqual(ValueType.list, store.getType("key1").?);

    const list = try store.getList("key1");
    try testing.expect(list != null);
    try testing.expectEqual(@as(usize, 0), list.?.len());
}

test "Store overwrite list with string" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    const list = try store.createList("key1");
    try list.append(.{ .string = "item" });
    try testing.expectEqual(ValueType.list, store.getType("key1").?);

    try store.setString("key1", "hello");
    try testing.expectEqual(ValueType.string, store.getType("key1").?);
    try testing.expectEqualStrings("hello", store.get("key1").?.value.string);

    const retrieved_list = store.getList("key1");
    try testing.expect(retrieved_list == StoreError.WrongType);
}

test "Store delete list key" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    const list = try store.createList("mylist");
    try list.append(.{ .string = "item1" });
    try list.append(.{ .string = "item2" });

    try testing.expect(store.exists("mylist"));
    try testing.expectEqual(@as(u32, 1), store.size());

    const deleted = store.delete("mylist");
    try testing.expect(deleted);
    try testing.expect(!store.exists("mylist"));
    try testing.expectEqual(@as(u32, 0), store.size());
    try testing.expect(try store.getList("mylist") == null);
}

test "Store empty list operations" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    const list = try store.createList("test_empty_ops");
    try testing.expectEqual(@as(usize, 0), list.len());

    try list.append(.{ .string = "" });
    try testing.expectEqual(@as(usize, 1), list.len());
    try testing.expectEqualStrings("", list.getByIndex(0).?.string);

    try list.append(.{ .int = 0 });
    try testing.expectEqual(@as(usize, 2), list.len());
    try testing.expectEqual(@as(i64, 0), list.getByIndex(1).?.int);
}
