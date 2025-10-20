const std = @import("std");
const storeModule = @import("../store.zig");
const Store = storeModule.Store;
const Value = @import("../parser.zig").Value;
const resp = @import("./resp.zig");
const ts_mod = @import("../time_series.zig");
const TimeSeries = ts_mod.TimeSeries;
const Duplicate_Policy = ts_mod.Duplicate_Policy;
const EncodingType = ts_mod.EncodingType;

const eql = std.mem.eql;

/// Helper to write last sample in RESP format
fn writeLastSample(writer: *std.Io.Writer, time_series: *TimeSeries) !void {
    if (time_series.last_sample) |s| {
        // Return [timestamp, value] array
        try resp.writeListLen(writer, 2);
        try resp.writeInt(writer, s.timestamp);
        try resp.writeDoubleBulkString(writer, s.value);
    } else {
        // Empty time series - return empty array
        try resp.writeListLen(writer, 0);
    }
}

fn modifyAndAdd(writer: *std.Io.Writer, store: *Store, args: []const Value, operation: enum { increment, decrement }) !void {
    const key = args[1].asSlice();
    const timestamp = try args[2].asInt();
    const delta = try args[3].asF64();

    const ts = try store.getTimeSeries(key);

    if (ts) |time_series| {
        const last_value = time_series.getLastValue();
        const new_value = switch (operation) {
            .increment => last_value + delta,
            .decrement => last_value - delta,
        };
        try time_series.addSample(timestamp, new_value);
        try resp.writeInt(writer, timestamp);
    } else {
        return error.KeyNotFound;
    }
}

pub fn ts_create(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    const key = args[1].asSlice();
    var retention_ms: u64 = 0;
    var encoding: ?[]const u8 = null;
    var chunk_size: u16 = 4096;
    var duplicate_policy: ?[]const u8 = null;
    var ignore_max_time_diff: ?u64 = null;
    var ignore_max_val_diff: ?f64 = null;

    for (args, 0..) |value, i| {
        if (eql(u8, value.asSlice(), "RETENTION")) {
            if (i + 1 >= args.len) return error.SyntaxError;
            retention_ms = try args[i + 1].asU64();
        } else if (eql(u8, value.asSlice(), "ENCODING")) {
            if (i + 1 >= args.len) return error.SyntaxError;
            encoding = args[i + 1].asSlice();
        } else if (eql(u8, value.asSlice(), "CHUNK_SIZE")) {
            if (i + 1 >= args.len) return error.SyntaxError;
            chunk_size = try args[i + 1].asU16();
        } else if (eql(u8, value.asSlice(), "DUPLICATE_POLICY")) {
            if (i + 1 >= args.len) return error.SyntaxError;
            duplicate_policy = args[i + 1].asSlice();
        } else if (eql(u8, value.asSlice(), "IGNORE")) {
            if (i + 1 >= args.len or i + 2 >= args.len) return error.SyntaxError;
            ignore_max_time_diff = try args[i + 1].asU64();
            ignore_max_val_diff = try args[i + 2].asF64();
        }
    }

    const ts: TimeSeries = .init(
        store.base_allocator,
        retention_ms,
        if (duplicate_policy) |dp| .fromString(dp) else null,
        chunk_size,
        if (encoding) |enc| .fromString(enc) else null,
        ignore_max_time_diff,
        ignore_max_val_diff,
    );

    try store.createTimeSeries(key, ts);

    try resp.writeOK(writer);
}

pub fn ts_add(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    const key = args[1].asSlice();
    const timestamp = try args[2].asInt();
    const value = try args[3].asF64();

    const ts = try store.getTimeSeries(key);

    if (ts) |time_series| {
        try time_series.addSample(timestamp, value);
        try resp.writeInt(writer, timestamp);
    } else {
        return error.KeyNotFound;
    }
}

pub fn ts_get(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    const key = args[1].asSlice();

    const ts = try store.getTimeSeries(key);

    if (ts) |time_series| {
        try writeLastSample(writer, time_series);
    } else {
        // Key doesn't exist - return error per Redis spec
        return error.KeyNotFound;
    }
}

pub fn ts_incrby(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    try modifyAndAdd(writer, store, args, .increment);
}

pub fn ts_decrby(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    try modifyAndAdd(writer, store, args, .decrement);
}

pub fn ts_alter(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    const key = args[1].asSlice();

    const ts = try store.getTimeSeries(key);

    if (ts) |time_series| {
        var retention_ms: ?u64 = null;
        var chunk_size: ?u16 = null;
        var duplicate_policy: ?Duplicate_Policy = null;

        // Parse optional arguments
        var i: usize = 2;
        while (i < args.len) : (i += 1) {
            const arg = args[i].asSlice();
            if (eql(u8, arg, "RETENTION")) {
                if (i + 1 >= args.len) return error.SyntaxError;
                retention_ms = try args[i + 1].asU64();
                i += 1;
            } else if (eql(u8, arg, "CHUNK_SIZE")) {
                if (i + 1 >= args.len) return error.SyntaxError;
                chunk_size = try args[i + 1].asU16();
                i += 1;
            } else if (eql(u8, arg, "DUPLICATE_POLICY")) {
                if (i + 1 >= args.len) return error.SyntaxError;
                duplicate_policy = .fromString(args[i + 1].asSlice());
                i += 1;
            }
        }

        time_series.alter(retention_ms, duplicate_policy, chunk_size);
        try resp.writeOK(writer);
    } else {
        return error.KeyNotFound;
    }
}

pub fn ts_mget(writer: *std.Io.Writer, store: *Store, args: []const Value) !void {
    // args[1..] are the keys
    const keys = args[1..];

    // Write array length
    try resp.writeListLen(writer, keys.len);

    // For each key, get the last sample
    for (keys) |key_value| {
        const key = key_value.asSlice();
        const ts = try store.getTimeSeries(key);

        if (ts) |time_series| {
            try writeLastSample(writer, time_series);
        } else {
            // Key doesn't exist - return null
            try resp.writeNull(writer);
        }
    }
}
