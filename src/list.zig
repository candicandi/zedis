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

    /// Normalize a signed index to an unsigned index.
    /// Returns null if the index is out of bounds.
    fn normalizeIndex(index: usize, list_len: usize) ?usize {
        if (list_len == 0) return null;

        if (index < 0) {
            const neg_offset = -index;
            if (neg_offset > list_len) return null;
            return list_len - neg_offset;
        } else {
            if (index >= list_len) return null;
            return index;
        }
    }

    /// Get the node at the specified index using bidirectional traversal.
    /// Optimizes by starting from the closest end (first or last).
    fn getNodeAt(self: *const ZedisList, actual_index: usize) ?*ZedisListNode {
        const list_len = self.cached_len;

        // O(1) optimization for first index
        if (actual_index == 0) {
            const node = self.list.first orelse return null;
            return @fieldParentPtr("node", node);
        }

        // O(1) optimization for last index
        if (actual_index == list_len - 1) {
            const node = self.list.last orelse return null;
            return @fieldParentPtr("node", node);
        }

        // O(n/2) traversal: start from the closest end
        if (actual_index >= list_len / 2) {
            // Start from the end if the index is in the second half
            var current = self.list.last;
            var i: usize = list_len - 1;
            while (current) |node| {
                if (i == actual_index) {
                    return @fieldParentPtr("node", node);
                }
                current = node.prev;
                i -= 1;
            }
        } else {
            // Start from the beginning if the index is in the first half
            var current = self.list.first;
            var i: usize = 0;
            while (current) |node| {
                if (i == actual_index) {
                    return @fieldParentPtr("node", node);
                }
                current = node.next;
                i += 1;
            }
        }

        return null;
    }

    pub fn getByIndex(self: *const ZedisList, index: usize) ?PrimitiveValue {
        const actual_index = normalizeIndex(index, self.cached_len) orelse return null;
        const list_node = self.getNodeAt(actual_index) orelse return null;
        return list_node.data;
    }

    pub fn setByIndex(self: *ZedisList, index: usize, value: PrimitiveValue) !void {
        const actual_index = normalizeIndex(index, self.cached_len) orelse return error.KeyNotFound;
        const list_node = self.getNodeAt(actual_index) orelse return error.KeyNotFound;
        list_node.data = value;
    }
};
