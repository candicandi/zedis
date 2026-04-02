const std = @import("std");

pub const PrimitiveValue = union(enum) {
    string: []const u8,
    int: i64,
};

pub const invalid_client_slot_index: u32 = std.math.maxInt(u32);

pub const ClientHandle = struct {
    slot_index: u32,
    generation: u32,

    pub fn eql(a: ClientHandle, b: ClientHandle) bool {
        return a.slot_index == b.slot_index and a.generation == b.generation;
    }
};
