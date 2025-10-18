const std = @import("std");
const testing = std.testing;
const ts_mod = @import("../time_series.zig");
const TimeSeries = ts_mod.TimeSeries;
const Duplicate_Policy = @import("../time_series.zig").Duplicate_Policy;
const EncodingType = @import("../time_series.zig").EncodingType;

test "TimeSeries: basic uncompressed storage" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0, // no retention
        .BLOCK,
        100, // max samples per chunk
        .Uncompressed,
        0, // no ignore time diff
        0.0, // no ignore val diff
    );
    defer ts.deinit();

    try ts.addSample(1000, 10.5);
    try ts.addSample(1001, 20.5);
    try ts.addSample(1002, 30.5);

    try testing.expectEqual(@as(u64, 3), ts.total_samples);
    try testing.expect(ts.tail != null);
    try testing.expectEqual(@as(i64, 1002), ts.tail.?.last_ts);
}

test "TimeSeries: basic compressed storage" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0,
        .BLOCK,
        100,
        .DeltaXor,
        0,
        0.0,
    );
    defer ts.deinit();

    try ts.addSample(1000, 100.0);
    try ts.addSample(1010, 105.0);
    try ts.addSample(1020, 110.0);

    try testing.expectEqual(@as(u64, 3), ts.total_samples);
}

test "TimeSeries: duplicate policy BLOCK" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0,
        .BLOCK,
        100,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    try ts.addSample(1000, 10.0);

    // Duplicate timestamp should return error
    const result = ts.addSample(1000, 20.0);
    try testing.expectError(error.TSDB_DuplicateTimestamp, result);
}

test "TimeSeries: duplicate policy FIRST" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0,
        .FIRST,
        100,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    try ts.addSample(1000, 10.0);
    try ts.addSample(1000, 20.0); // Should be ignored

    try testing.expectEqual(@as(u64, 1), ts.total_samples);
    try testing.expectEqual(@as(f64, 10.0), ts.last_sample.?.value);
}

test "TimeSeries: duplicate policy LAST" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0,
        .LAST,
        100,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    try ts.addSample(1000, 10.0);
    try ts.addSample(1000, 20.0); // Should update

    try testing.expectEqual(@as(u64, 2), ts.total_samples);
    try testing.expectEqual(@as(f64, 20.0), ts.last_sample.?.value);
}

test "TimeSeries: duplicate policy MIN" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0,
        .MIN,
        100,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    try ts.addSample(1000, 10.0);
    try ts.addSample(1000, 20.0); // Should be ignored (10 < 20)

    try testing.expectEqual(@as(f64, 10.0), ts.last_sample.?.value);

    try ts.addSample(1001, 30.0);
    try ts.addSample(1001, 5.0); // Should be kept (5 < 30)

    try testing.expectEqual(@as(f64, 5.0), ts.last_sample.?.value);
}

test "TimeSeries: duplicate policy MAX" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0,
        .MAX,
        100,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    try ts.addSample(1000, 10.0);
    try ts.addSample(1000, 20.0); // Should be kept (20 > 10)

    try testing.expectEqual(@as(f64, 20.0), ts.last_sample.?.value);

    try ts.addSample(1001, 30.0);
    try ts.addSample(1001, 15.0); // Should be ignored (15 < 30)

    try testing.expectEqual(@as(f64, 30.0), ts.last_sample.?.value);
}

test "TimeSeries: IGNORE parameter filters samples" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0,
        .LAST, // IGNORE only works with LAST policy
        100,
        .Uncompressed,
        1000, // ignore_max_time_diff = 1000ms
        5.0, // ignore_max_val_diff = 5.0
    );
    defer ts.deinit();

    try ts.addSample(1000, 100.0);

    // Time diff = 500ms, val diff = 2.0 - both within threshold, should be ignored
    try ts.addSample(1500, 102.0);
    try testing.expectEqual(@as(u64, 1), ts.total_samples);

    // Time diff = 2000ms - exceeds threshold, should be added
    try ts.addSample(3000, 102.0);
    try testing.expectEqual(@as(u64, 2), ts.total_samples);

    // Time diff = 500ms, val diff = 10.0 - val exceeds threshold, should be added
    try ts.addSample(3500, 112.0);
    try testing.expectEqual(@as(u64, 3), ts.total_samples);
}

test "TimeSeries: IGNORE does not apply to non-LAST policies" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0,
        .BLOCK, // Not LAST policy
        100,
        .Uncompressed,
        1000,
        5.0,
    );
    defer ts.deinit();

    try ts.addSample(1000, 100.0);

    // Even though within IGNORE thresholds, should be added (BLOCK policy)
    try ts.addSample(1500, 102.0);
    try testing.expectEqual(@as(u64, 2), ts.total_samples);
}

test "TimeSeries: retention policy evicts old chunks" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        5000, // 5 second retention
        .BLOCK,
        2, // 2 samples per chunk - forces multiple chunks
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    // Add samples that will create multiple chunks
    try ts.addSample(1000, 10.0);
    try ts.addSample(2000, 20.0);
    try testing.expectEqual(@as(u64, 2), ts.total_samples);

    // New chunk
    try ts.addSample(3000, 30.0);
    try ts.addSample(4000, 40.0);
    try testing.expectEqual(@as(u64, 4), ts.total_samples);

    // New chunk
    try ts.addSample(5000, 50.0);
    try ts.addSample(6000, 60.0);
    try testing.expectEqual(@as(u64, 6), ts.total_samples);

    // Add sample at 10000ms - retention window is [5000, 10000]
    // First chunk (1000-2000) should be evicted
    try ts.addSample(10000, 100.0);

    // Should still have samples from chunks 2 and 3
    try testing.expect(ts.head != null);
    try testing.expect(ts.head.?.first_ts >= 3000);
}

test "TimeSeries: retention policy with zero retention" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0, // No retention - keep all data
        .BLOCK,
        2,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    try ts.addSample(1000, 10.0);
    try ts.addSample(2000, 20.0);
    try ts.addSample(3000, 30.0);
    try ts.addSample(100000, 100.0); // Much later timestamp

    // All samples should be retained
    try testing.expectEqual(@as(u64, 4), ts.total_samples);
    try testing.expect(ts.head != null);
    try testing.expectEqual(@as(i64, 1000), ts.head.?.first_ts);
}

test "TimeSeries: chunk sealing creates new chunk when full" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0,
        .BLOCK,
        3, // Only 3 samples per chunk
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    try ts.addSample(1000, 10.0);
    try ts.addSample(1001, 11.0);
    try ts.addSample(1002, 12.0);

    // First chunk should be full
    try testing.expect(ts.tail != null);
    try testing.expectEqual(@as(u16, 3), ts.tail.?.sample_count);

    // Adding another sample should create a new chunk
    try ts.addSample(1003, 13.0);

    try testing.expect(ts.tail != null);
    try testing.expectEqual(@as(u16, 1), ts.tail.?.sample_count);
    try testing.expect(ts.tail.?.prev != null);
}

test "TimeSeries: compressed vs uncompressed encoding" {
    const allocator = testing.allocator;

    var ts_compressed = TimeSeries.init(
        allocator,
        0,
        .BLOCK,
        10,
        .DeltaXor,
        0,
        0.0,
    );
    defer ts_compressed.deinit();

    var ts_uncompressed = TimeSeries.init(
        allocator,
        0,
        .BLOCK,
        10,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts_uncompressed.deinit();

    // Add same samples to both
    var i: i64 = 0;
    while (i < 5) : (i += 1) {
        try ts_compressed.addSample(1000 + i, @as(f64, @floatFromInt(i)));
        try ts_uncompressed.addSample(1000 + i, @as(f64, @floatFromInt(i)));
    }

    try testing.expectEqual(@as(u64, 5), ts_compressed.total_samples);
    try testing.expectEqual(@as(u64, 5), ts_uncompressed.total_samples);
}

test "TimeSeries: multiple chunks with retention" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        10000, // 10 second retention
        .BLOCK,
        2,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    // Create 5 chunks over time
    try ts.addSample(1000, 1.0);
    try ts.addSample(2000, 2.0);

    try ts.addSample(3000, 3.0);
    try ts.addSample(4000, 4.0);

    try ts.addSample(5000, 5.0);
    try ts.addSample(6000, 6.0);

    try ts.addSample(7000, 7.0);
    try ts.addSample(8000, 8.0);

    try ts.addSample(9000, 9.0);
    try ts.addSample(10000, 10.0);

    // All should still exist (within 10s window from 10000)
    try testing.expectEqual(@as(u64, 10), ts.total_samples);

    // Add sample at 15000 - retention window [5000, 15000]
    // First two chunks should be evicted
    try ts.addSample(15000, 15.0);

    try testing.expect(ts.head != null);
    try testing.expect(ts.head.?.first_ts >= 5000);
}

test "TimeSeries: out of order samples with LAST policy" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0,
        .LAST,
        10,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    try ts.addSample(1000, 10.0);
    try ts.addSample(2000, 20.0);

    // Out of order sample (IGNORE only applies to in-order)
    try ts.addSample(1500, 15.0);

    try testing.expectEqual(@as(u64, 3), ts.total_samples);
}

test "TimeSeries: edge case - empty time series retention" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        5000,
        .BLOCK,
        10,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    // Empty time series - eviction should be a no-op
    ts.evictExpiredChunks(10000);

    try testing.expect(ts.head == null);
    try testing.expect(ts.tail == null);
}

test "TimeSeries: all chunks evicted clears head and tail" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        1000, // 1 second retention
        .BLOCK,
        2,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    try ts.addSample(1000, 10.0);
    try ts.addSample(1500, 15.0);

    // Add sample way in the future - all old chunks should be evicted
    try ts.addSample(10000, 100.0);

    // Old chunk should be gone, only new chunk remains
    try testing.expect(ts.head != null);
    try testing.expect(ts.head == ts.tail);
    try testing.expectEqual(@as(i64, 10000), ts.head.?.first_ts);
}

test "TimeSeries: getLastValue returns last value" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        0,
        .BLOCK,
        10,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    // Empty time series should return 0.0
    try testing.expectEqual(@as(f64, 0.0), ts.getLastValue());

    // Add samples
    try ts.addSample(1000, 10.0);
    try testing.expectEqual(@as(f64, 10.0), ts.getLastValue());

    try ts.addSample(2000, 20.5);
    try testing.expectEqual(@as(f64, 20.5), ts.getLastValue());
}

test "TimeSeries: alter updates properties" {
    const allocator = testing.allocator;

    var ts = TimeSeries.init(
        allocator,
        1000,
        .BLOCK,
        100,
        .Uncompressed,
        0,
        0.0,
    );
    defer ts.deinit();

    // Alter retention
    ts.alter(5000, null, null);
    try testing.expectEqual(@as(u64, 5000), ts.retention_ms);

    // Alter duplicate policy
    ts.alter(null, .LAST, null);
    try testing.expectEqual(Duplicate_Policy.LAST, ts.duplicate_policy);

    // Alter chunk size
    ts.alter(null, null, 200);
    try testing.expectEqual(@as(u16, 200), ts.max_chunk_samples);

    // Alter multiple at once
    ts.alter(10000, .MIN, 50);
    try testing.expectEqual(@as(u64, 10000), ts.retention_ms);
    try testing.expectEqual(Duplicate_Policy.MIN, ts.duplicate_policy);
    try testing.expectEqual(@as(u16, 50), ts.max_chunk_samples);
}

// Command-level tests
const Store = @import("../store.zig").Store;
const Value = @import("../parser.zig").Value;
const ts_commands = @import("../commands/time_series.zig");

test "TS.INCRBY increments from zero" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator, 4096);
    defer store.deinit();

    var buffer: [4096]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);

    // Create time series
    const create_args = [_]Value{
        .{ .data = "TS.CREATE" },
        .{ .data = "myts" },
        .{ .data = "RETENTION" },
        .{ .data = "0" },
        .{ .data = "ENCODING" },
        .{ .data = "UNCOMPRESSED" },
        .{ .data = "DUPLICATE_POLICY" },
        .{ .data = "LAST" },
    };
    try ts_commands.ts_create(&writer, &store, &create_args);

    // Increment by 5.0 (should start from 0.0)
    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const incrby_args = [_]Value{
        .{ .data = "TS.INCRBY" },
        .{ .data = "myts" },
        .{ .data = "1000" },
        .{ .data = "5.0" },
    };
    try ts_commands.ts_incrby(&writer, &store, &incrby_args);

    // Should return timestamp
    try testing.expectEqualStrings(":1000\r\n", writer.buffered());

    // Verify value is 5.0 (formatted as "5" in RESP)
    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const get_args = [_]Value{
        .{ .data = "TS.GET" },
        .{ .data = "myts" },
    };
    try ts_commands.ts_get(&writer, &store, &get_args);

    const output = writer.buffered();
    try testing.expect(std.mem.indexOf(u8, output, "$1\r\n5\r\n") != null or std.mem.indexOf(u8, output, "$3\r\n5.0\r\n") != null);
}

test "TS.INCRBY increments from existing value" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator, 4096);
    defer store.deinit();

    var buffer: [4096]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);

    // Create and add initial value
    const create_args = [_]Value{
        .{ .data = "TS.CREATE" },
        .{ .data = "myts" },
    };
    try ts_commands.ts_create(&writer, &store, &create_args);

    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const add_args = [_]Value{
        .{ .data = "TS.ADD" },
        .{ .data = "myts" },
        .{ .data = "1000" },
        .{ .data = "10.0" },
    };
    try ts_commands.ts_add(&writer, &store, &add_args);

    // Increment by 3.0
    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const incrby_args = [_]Value{
        .{ .data = "TS.INCRBY" },
        .{ .data = "myts" },
        .{ .data = "2000" },
        .{ .data = "3.0" },
    };
    try ts_commands.ts_incrby(&writer, &store, &incrby_args);

    // Verify value is 13.0 (formatted as "13" in RESP)
    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const get_args = [_]Value{
        .{ .data = "TS.GET" },
        .{ .data = "myts" },
    };
    try ts_commands.ts_get(&writer, &store, &get_args);

    const output = writer.buffered();
    try testing.expect(std.mem.indexOf(u8, output, "$2\r\n13\r\n") != null or std.mem.indexOf(u8, output, "$4\r\n13.0\r\n") != null);
}

test "TS.DECRBY decrements value" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator, 4096);
    defer store.deinit();

    var buffer: [4096]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);

    // Create and add initial value
    const create_args = [_]Value{
        .{ .data = "TS.CREATE" },
        .{ .data = "myts" },
    };
    try ts_commands.ts_create(&writer, &store, &create_args);

    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const add_args = [_]Value{
        .{ .data = "TS.ADD" },
        .{ .data = "myts" },
        .{ .data = "1000" },
        .{ .data = "20.0" },
    };
    try ts_commands.ts_add(&writer, &store, &add_args);

    // Decrement by 7.0
    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const decrby_args = [_]Value{
        .{ .data = "TS.DECRBY" },
        .{ .data = "myts" },
        .{ .data = "2000" },
        .{ .data = "7.0" },
    };
    try ts_commands.ts_decrby(&writer, &store, &decrby_args);

    // Verify value is 13.0 (formatted as "13" in RESP)
    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const get_args = [_]Value{
        .{ .data = "TS.GET" },
        .{ .data = "myts" },
    };
    try ts_commands.ts_get(&writer, &store, &get_args);

    const output = writer.buffered();
    try testing.expect(std.mem.indexOf(u8, output, "$2\r\n13\r\n") != null or std.mem.indexOf(u8, output, "$4\r\n13.0\r\n") != null);
}

test "TS.ALTER changes retention" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator, 4096);
    defer store.deinit();

    var buffer: [4096]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);

    // Create with retention 1000
    const create_args = [_]Value{
        .{ .data = "TS.CREATE" },
        .{ .data = "myts" },
        .{ .data = "RETENTION" },
        .{ .data = "1000" },
    };
    try ts_commands.ts_create(&writer, &store, &create_args);

    // Alter retention to 5000
    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const alter_args = [_]Value{
        .{ .data = "TS.ALTER" },
        .{ .data = "myts" },
        .{ .data = "RETENTION" },
        .{ .data = "5000" },
    };
    try ts_commands.ts_alter(&writer, &store, &alter_args);

    try testing.expectEqualStrings("+OK\r\n", writer.buffered());

    // Verify the retention was changed
    const ts = try store.getTimeSeries("myts");
    try testing.expectEqual(@as(u64, 5000), ts.?.retention_ms);
}

test "TS.ALTER changes duplicate policy" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator, 4096);
    defer store.deinit();

    var buffer: [4096]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);

    // Create with BLOCK policy
    const create_args = [_]Value{
        .{ .data = "TS.CREATE" },
        .{ .data = "myts" },
        .{ .data = "DUPLICATE_POLICY" },
        .{ .data = "BLOCK" },
    };
    try ts_commands.ts_create(&writer, &store, &create_args);

    // Alter to LAST policy
    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const alter_args = [_]Value{
        .{ .data = "TS.ALTER" },
        .{ .data = "myts" },
        .{ .data = "DUPLICATE_POLICY" },
        .{ .data = "LAST" },
    };
    try ts_commands.ts_alter(&writer, &store, &alter_args);

    try testing.expectEqualStrings("+OK\r\n", writer.buffered());

    // Verify the policy was changed
    const ts = try store.getTimeSeries("myts");
    try testing.expectEqual(Duplicate_Policy.LAST, ts.?.duplicate_policy);
}

test "TS.MGET returns multiple time series" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator, 4096);
    defer store.deinit();

    var buffer: [4096]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);

    // Create first time series with value
    const create_args1 = [_]Value{
        .{ .data = "TS.CREATE" },
        .{ .data = "ts1" },
    };
    try ts_commands.ts_create(&writer, &store, &create_args1);

    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const add_args1 = [_]Value{
        .{ .data = "TS.ADD" },
        .{ .data = "ts1" },
        .{ .data = "1000" },
        .{ .data = "100.0" },
    };
    try ts_commands.ts_add(&writer, &store, &add_args1);

    // Create second time series with value
    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const create_args2 = [_]Value{
        .{ .data = "TS.CREATE" },
        .{ .data = "ts2" },
    };
    try ts_commands.ts_create(&writer, &store, &create_args2);

    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const add_args2 = [_]Value{
        .{ .data = "TS.ADD" },
        .{ .data = "ts2" },
        .{ .data = "2000" },
        .{ .data = "200.0" },
    };
    try ts_commands.ts_add(&writer, &store, &add_args2);

    // MGET both time series
    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const mget_args = [_]Value{
        .{ .data = "TS.MGET" },
        .{ .data = "ts1" },
        .{ .data = "ts2" },
    };
    try ts_commands.ts_mget(&writer, &store, &mget_args);

    const output = writer.buffered();
    // Should return array of 2 elements
    try testing.expect(std.mem.startsWith(u8, output, "*2\r\n"));
    // Should contain both values (formatted as "100" and "200" in RESP)
    try testing.expect(std.mem.indexOf(u8, output, "$3\r\n100\r\n") != null or std.mem.indexOf(u8, output, "$5\r\n100.0\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, output, "$3\r\n200\r\n") != null or std.mem.indexOf(u8, output, "$5\r\n200.0\r\n") != null);
}

test "TS.MGET handles missing keys" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var store = Store.init(allocator, 4096);
    defer store.deinit();

    var buffer: [4096]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);

    // Create only one time series
    const create_args = [_]Value{
        .{ .data = "TS.CREATE" },
        .{ .data = "ts1" },
    };
    try ts_commands.ts_create(&writer, &store, &create_args);

    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const add_args = [_]Value{
        .{ .data = "TS.ADD" },
        .{ .data = "ts1" },
        .{ .data = "1000" },
        .{ .data = "100.0" },
    };
    try ts_commands.ts_add(&writer, &store, &add_args);

    // MGET with one existing and one missing key
    buffer = std.mem.zeroes([4096]u8);
    writer = std.Io.Writer.fixed(&buffer);
    const mget_args = [_]Value{
        .{ .data = "TS.MGET" },
        .{ .data = "ts1" },
        .{ .data = "nonexistent" },
    };
    try ts_commands.ts_mget(&writer, &store, &mget_args);

    const output = writer.buffered();
    // Should return array of 2 elements
    try testing.expect(std.mem.startsWith(u8, output, "*2\r\n"));
    // Should contain the existing value (formatted as "100" in RESP)
    try testing.expect(std.mem.indexOf(u8, output, "$3\r\n100\r\n") != null or std.mem.indexOf(u8, output, "$5\r\n100.0\r\n") != null);
    // Should contain a null for the missing key
    try testing.expect(std.mem.indexOf(u8, output, "$-1") != null);
}
