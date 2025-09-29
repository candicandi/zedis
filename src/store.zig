const std = @import("std");

pub const ValueType = enum(u8) {
    string = 0,
    int = 1,
    list = 2,

    pub fn toRdbOpcode(self: ValueType) u8 {
        return @intFromEnum(self);
    }

    pub fn fromOpCode(num: u8) ValueType {
        return @enumFromInt(num);
    }
};

pub const StoreError = error{
    KeyNotFound,
    WrongType,
    NotAnInteger,
};

pub const PrimitiveValue = union(enum) {
    string: []const u8,
    int: i64,
};

pub const ZedisListNode = struct {
    data: PrimitiveValue,
    node: std.DoublyLinkedList.Node = .{},
};

pub const ZedisList = struct {
    list: std.DoublyLinkedList = .{},
    allocator: std.mem.Allocator,

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

    pub fn len(self: *const ZedisList) usize {
        return self.list.len();
    }

    pub fn prepend(self: *ZedisList, value: PrimitiveValue) !void {
        const list_node = try self.allocator.create(ZedisListNode);
        list_node.* = ZedisListNode{ .data = value };
        self.list.prepend(&list_node.node);
    }

    pub fn append(self: *ZedisList, value: PrimitiveValue) !void {
        const list_node = try self.allocator.create(ZedisListNode);
        list_node.* = ZedisListNode{ .data = value };
        self.list.append(&list_node.node);
    }

    pub fn popFirst(self: *ZedisList) ?PrimitiveValue {
        const node = self.list.popFirst() orelse return null;
        const list_node: *ZedisListNode = @fieldParentPtr("node", node);
        const value = list_node.data;
        self.allocator.destroy(list_node);
        return value;
    }

    pub fn pop(self: *ZedisList) ?PrimitiveValue {
        const node = self.list.pop() orelse return null;
        const list_node: *ZedisListNode = @fieldParentPtr("node", node);
        const value = list_node.data;
        self.allocator.destroy(list_node);
        return value;
    }

    pub fn getByIndex(self: *const ZedisList, index: i64) ?PrimitiveValue {
        const list_len = self.list.len();
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
        const list_len = self.list.len();
        if (list_len == 0) return StoreError.KeyNotFound;

        // Convert negative index to positive
        const actual_index: usize = if (index < 0) blk: {
            const neg_offset = @as(usize, @intCast(-index));
            if (neg_offset > list_len) return StoreError.KeyNotFound;
            break :blk list_len - neg_offset;
        } else blk: {
            const pos_index = @as(usize, @intCast(index));
            if (pos_index >= list_len) return StoreError.KeyNotFound;
            break :blk pos_index;
        };

        // O(1) optimization for first index
        if (actual_index == 0) {
            const node = self.list.first orelse return StoreError.KeyNotFound;
            const list_node: *ZedisListNode = @fieldParentPtr("node", node);
            list_node.data = value;
            return;
        }

        // O(1) optimization for last index
        if (actual_index == list_len - 1) {
            const node = self.list.last orelse return StoreError.KeyNotFound;
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
        return StoreError.KeyNotFound;
    }
};

pub const ZedisValue = union(ValueType) {
    string: []const u8,
    int: i64,
    list: ZedisList,
};

pub const ZedisObject = struct { value: ZedisValue, expiration: ?i64 = null };

pub const Store = struct {
    allocator: std.mem.Allocator,
    // The HashMap stores string keys and string values.
    map: std.StringHashMap(ZedisObject),

    // Initializes the store.
    pub fn init(allocator: std.mem.Allocator) Store {
        return .{
            .allocator = allocator,
            .map = std.StringHashMap(ZedisObject).init(allocator),
        };
    }

    // Frees all memory associated with the store.
    pub fn deinit(self: *Store) void {
        var map_it = self.map.iterator();
        while (map_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            // Free values based on their type
            switch (entry.value_ptr.*.value) {
                .string => |str| self.allocator.free(str),
                .int => {},
                .list => |*list| @constCast(list).deinit(),
            }
        }
        self.map.deinit();
    }

    pub fn size(self: Store) u32 {
        return self.map.count();
    }

    pub fn setString(self: *Store, key: []const u8, value: []const u8) !void {
        const zedis_object = ZedisObject{ .value = .{ .string = value } };
        try self.setObject(key, zedis_object);
    }

    pub fn setInt(self: *Store, key: []const u8, value: i64) !void {
        const zedis_object = ZedisObject{ .value = .{ .int = value } };
        try self.setObject(key, zedis_object);
    }

    pub fn setObject(self: *Store, key: []const u8, object: ZedisObject) !void {
        // Check if key already exists and free old memory
        var key_exists = false;
        if (self.map.getPtr(key)) |existing_entry| {
            key_exists = true;
            // Free the old value based on its type
            switch (existing_entry.value) {
                .string => |str| self.allocator.free(str),
                .int => {},
                .list => |*list| @constCast(list).deinit(),
            }
        }

        // If key doesn't exist, we need to allocate memory for the key
        if (!key_exists) {
            const key_copy = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(key_copy);

            // Pre-allocate the entry in the map
            try self.map.put(key_copy, undefined);
        }

        // Now we know the key exists in the map, get a pointer to modify it
        const entry_ptr = self.map.getPtr(key).?;

        // Set up the new object
        var new_object = object;
        switch (object.value) {
            .string => {
                const value_copy = try self.allocator.dupe(u8, object.value.string);
                errdefer self.allocator.free(value_copy);
                new_object.value = .{ .string = value_copy };
            },
            .int => {
                // Integer values don't need allocation
                new_object.value = object.value;
            },
            .list => {
                // For lists, we just pass the list directly since createList handles it
                new_object.value = object.value;
            },
        }

        // Update the entry
        entry_ptr.* = new_object;
    }

    // Delete a key from the store
    pub fn delete(self: *Store, key: []const u8) bool {
        if (self.map.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
            // Free the value based on its type
            switch (kv.value.value) {
                .string => |str| self.allocator.free(str),
                .int => {},
                .list => |*list| @constCast(list).deinit(),
            }
            return true;
        }
        return false;
    }

    // Check if a key exists
    pub fn exists(self: Store, key: []const u8) bool {
        return self.map.contains(key);
    }

    pub fn getType(self: Store, key: []const u8) ?ValueType {
        if (self.map.get(key)) |obj| {
            switch (obj.value) {
                .int => return .int,
                .string => return .string,
                .list => return .list,
            }
        }
        return null;
    }

    // Gets a value by its key. It also acquires a lock.
    pub fn get(self: Store, key: []const u8) ?ZedisObject {
        if (self.map.get(key)) |obj| {
            return obj;
        } else {
            return null;
        }
    }

    // Gets a copy of the string value for thread safety
    pub fn getString(self: Store, allocator: std.mem.Allocator, key: []const u8) !?[]u8 {
        if (self.map.get(key)) |obj| {
            switch (obj.value) {
                .string => |str| return try allocator.dupe(u8, str),
                .int => |i| return try std.fmt.allocPrint(allocator, "{d}", .{i}),
                .list => return null, // Lists can't be converted to strings
            }
        }
        return null;
    }

    // Gets an integer value, converting from string if necessary
    pub fn getInt(self: Store, key: []const u8) !?i64 {
        if (self.map.get(key)) |obj| {
            switch (obj.value) {
                .int => |i| return i,
                .string => |str| {
                    return std.fmt.parseInt(i64, str, 10) catch StoreError.NotAnInteger;
                },
                .list => return StoreError.WrongType,
            }
        }
        return null;
    }

    pub fn getList(self: Store, key: []const u8) !?*ZedisList {
        if (self.map.getPtr(key)) |obj_ptr| {
            switch (obj_ptr.value) {
                .list => |*list| return list,
                else => return StoreError.WrongType,
            }
        }
        return null;
    }

    pub fn createList(self: *Store, key: []const u8) !*ZedisList {
        // Create list directly in place without going through setObject
        const key_copy = try self.allocator.dupe(u8, key);
        const list = ZedisList.init(self.allocator);

        const zedis_object = ZedisObject{ .value = .{ .list = list } };

        try self.setObject(key_copy, zedis_object);

        return &self.map.getPtr(key_copy).?.value.list;
    }

    pub fn getSetList(self: *Store, key: []const u8) !*ZedisList {
        const list = try self.getList(key);
        if (list == null) {
            return try self.createList(key);
        }
        return list.?;
    }

    pub fn expire(self: *Store, key: []const u8, time: i64) !bool {
        if (self.map.getPtr(key)) |entry| {
            entry.expiration = time;

            return true;
        }
        return false;
    }

    pub fn isExpired(self: Store, key: []const u8) bool {
        if (self.map.get(key)) |entry| {
            return entry.expiration != null;
        }
        return false;
    }
};
