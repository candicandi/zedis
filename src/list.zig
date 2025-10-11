const std = @import("std");
const PrimitiveValue = @import("types.zig").PrimitiveValue;

pub const ZedisListNode = struct {
    data: PrimitiveValue,
    node: std.DoublyLinkedList.Node = .{},
};

pub const ZedisList = struct {
    list: std.DoublyLinkedList = .{},
    allocator: std.mem.Allocator,
    cached_len: usize = 0,

    pub fn init(allocator: std.mem.Allocator) ZedisList {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ZedisList) void {
        var current = self.list.first;
        while (current) |node| {
            current = node.next;
            const list_node: *ZedisListNode = @fieldParentPtr("node", node);
            self.allocator.destroy(list_node);
        }
        self.list = .{};
    }

    pub inline fn len(self: *const ZedisList) usize {
        return self.cached_len;
    }

    pub fn prepend(self: *ZedisList, value: PrimitiveValue) !void {
        const list_node = try self.allocator.create(ZedisListNode);
        list_node.* = ZedisListNode{ .data = value };
        self.list.prepend(&list_node.node);
        self.cached_len += 1;
    }

    pub fn append(self: *ZedisList, value: PrimitiveValue) !void {
        const list_node = try self.allocator.create(ZedisListNode);
        list_node.* = ZedisListNode{ .data = value };
        self.list.append(&list_node.node);
        self.cached_len += 1;
    }

    pub fn popFirst(self: *ZedisList) ?PrimitiveValue {
        const node = self.list.popFirst() orelse return null;
        const list_node: *ZedisListNode = @fieldParentPtr("node", node);
        const value = list_node.data;
        self.allocator.destroy(list_node);
        self.cached_len -= 1;
        return value;
    }

    pub fn pop(self: *ZedisList) ?PrimitiveValue {
        const node = self.list.pop() orelse return null;
        const list_node: *ZedisListNode = @fieldParentPtr("node", node);
        const value = list_node.data;
        self.allocator.destroy(list_node);
        self.cached_len -= 1;
        return value;
    }

    pub fn getByIndex(self: *const ZedisList, index: i64) ?PrimitiveValue {
        const list_len = self.cached_len;
        if (list_len == 0) return null;

        // Convert negative index to positive
        const actual_index: usize = if (index < 0) blk: {
            const neg_offset = @as(usize, @intCast(-index));
            if (neg_offset > list_len) return null;
            break :blk list_len - neg_offset;
        } else blk: {
            const pos_index = @as(usize, @intCast(index));
            if (pos_index >= list_len) return null;
            break :blk pos_index;
        };

        // O(1) optimization for first index
        if (actual_index == 0) {
            const node = self.list.first orelse return null;
            const list_node: *ZedisListNode = @fieldParentPtr("node", node);
            return list_node.data;
        }

        // O(1) optimization for last index
        if (actual_index == list_len - 1) {
            const node = self.list.last orelse return null;
            const list_node: *ZedisListNode = @fieldParentPtr("node", node);
            return list_node.data;
        }

        // O(n) traversal for middle indices
        var current = self.list.first;
        var i: usize = 0;
        while (current) |node| {
            if (i == actual_index) {
                const list_node: *ZedisListNode = @fieldParentPtr("node", node);
                return list_node.data;
            }
            current = node.next;
            i += 1;
        }
        return null;
    }

    pub fn setByIndex(self: *ZedisList, index: i64, value: PrimitiveValue) !void {
        const list_len = self.cached_len;
        if (list_len == 0) return error.KeyNotFound;

        // Convert negative index to positive
        const actual_index: usize = if (index < 0) blk: {
            const neg_offset = @as(usize, @intCast(-index));
            if (neg_offset > list_len) return error.KeyNotFound;
            break :blk list_len - neg_offset;
        } else blk: {
            const pos_index = @as(usize, @intCast(index));
            if (pos_index >= list_len) return error.KeyNotFound;
            break :blk pos_index;
        };

        // O(1) optimization for first index
        if (actual_index == 0) {
            const node = self.list.first orelse return error.KeyNotFound;
            const list_node: *ZedisListNode = @fieldParentPtr("node", node);
            list_node.data = value;
            return;
        }

        // O(1) optimization for last index
        if (actual_index == list_len - 1) {
            const node = self.list.last orelse return error.KeyNotFound;
            const list_node: *ZedisListNode = @fieldParentPtr("node", node);
            list_node.data = value;
            return;
        }

        // O(n) traversal for middle indices
        var current = self.list.first;
        var i: usize = 0;
        while (current) |node| {
            if (i == actual_index) {
                const list_node: *ZedisListNode = @fieldParentPtr("node", node);
                list_node.data = value;
                return;
            }
            current = node.next;
            i += 1;
        }
        return error.KeyNotFound;
    }
};
