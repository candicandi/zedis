const std = @import("std");
const storeModule = @import("../store.zig");
const Store = storeModule.Store;
const ZedisObject = storeModule.ZedisObject;
const Client = @import("../client.zig").Client;
const Value = @import("../parser.zig").Value;
const resp = @import("./resp.zig");

pub fn flush_all(client: *Client, _: []const Value, writer: *std.Io.Writer) !void {
    for (client.databases) |*db| {
        db.flush_db();
    }
    try resp.writeOK(writer);
}

pub fn flush_db(client: *Client, _: []const Value, writer: *std.Io.Writer) !void {
    client.getCurrentStore().flush_db();
    try resp.writeOK(writer);
}

pub fn db_size(writer: *std.Io.Writer, store: *Store, _: []const Value) !void {
    try resp.writeInt(writer, store.size());
}
