const std = @import("std");
const Allocator = std.mem.Allocator;
const Client = @import("client.zig").Client;
const ClientMailbox = @import("client_mailbox.zig").ClientMailbox;
const MessageNode = @import("client_mailbox.zig").MessageNode;
const freeMessageList = @import("client_mailbox.zig").freeMessageList;
const CommandRegistry = @import("./commands/registry.zig").CommandRegistry;
const command_init = @import("./commands/init.zig");
const Reader = @import("./rdb/zdb.zig").Reader;
const Store = @import("store.zig").Store;
const pubsub = @import("./commands/pubsub.zig");
const PubSubContext = pubsub.PubSubContext;
const Config = @import("config.zig");
const KeyValueAllocator = @import("kv_allocator.zig");
const aof = @import("./aof/aof.zig");
const Clock = @import("clock.zig");
const ClientHandle = @import("types.zig").ClientHandle;
const invalid_client_slot_index = @import("types.zig").invalid_client_slot_index;
const Io = std.Io;
const Stream = Io.net.Stream;

const log = std.log.scoped(.server);

const Server = @This();
const mailbox_capacity = 256;

const ClientSlotState = enum(u8) {
    free,
    active,
    closing,
};

const ClientSlot = struct {
    generation: std.atomic.Value(u32) = .init(0),
    state: std.atomic.Value(ClientSlotState) = .init(.free),
    disconnect_requested: std.atomic.Value(bool) = .init(false),
    next_free: std.atomic.Value(u32) = .init(invalid_client_slot_index),
    mailbox: ClientMailbox = ClientMailbox.init(mailbox_capacity),
    client: Client = undefined,
};

// Configuration
config: Config,

// Base allocator (only for server initialization)
base_allocator: std.mem.Allocator,

// Network
address: Io.net.IpAddress,
listener: Io.net.Server,
io: Io,

// Fixed allocations (pre-allocated, never freed individually)
client_slots: []ClientSlot,
free_list_head: std.atomic.Value(u64),

// Map of channel_name -> subscriber handles
pubsub_map: std.StringHashMap([]ClientHandle),
pubsub_mutex: std.Io.RwLock,

// Arena for temporary/short-lived allocations
temp_arena: std.heap.ArenaAllocator,

// Custom allocator for key-value store with eviction
kv_allocator: KeyValueAllocator,
store: Store,
registry: CommandRegistry,
pubsub_context: PubSubContext,

// Metadata
redisVersion: ?[]u8 = undefined,
createdTime: i64,

// AOF logging
aof_writer: aof.Writer,

fn packFreeListHead(index: u32, tag: u32) u64 {
    return (@as(u64, tag) << 32) | index;
}

fn unpackFreeListHead(raw: u64) struct { index: u32, tag: u32 } {
    return .{
        .index = @intCast(raw & std.math.maxInt(u32)),
        .tag = @intCast(raw >> 32),
    };
}

const ClientAllocation = struct {
    handle: ClientHandle,
    slot: *ClientSlot,
};

pub fn initWithConfig(
    base_allocator: Allocator,
    host: []const u8,
    port: u16,
    config: Config,
    io: Io,
) !Server {
    const address = try Io.net.IpAddress.parse(host, port);

    const listener = try address.listen(io, .{ .kernel_backlog = 128 * 10 });

    // Initialize the KV allocator with eviction support
    var kv_allocator = try KeyValueAllocator.init(base_allocator, config.kv_memory_budget, config.eviction_policy);

    // Initialize shared clock for the store
    var clock = Clock.init(io, config.clock_update_ms);
    try clock.start();

    // Initialize the single shared store with the KV allocator
    var store = try Store.init(kv_allocator.allocator(), io, &clock, .{
        .initial_capacity = config.initial_capacity,
    });

    // Link KV allocator before eviction can be used.
    kv_allocator.attachStore(&store);

    // Initialize temp arena for temporary allocations
    const temp_arena = std.heap.ArenaAllocator.init(base_allocator);

    // Initialize command registry with base allocator (lives for server lifetime)
    const registry = try command_init.initRegistry(base_allocator);

    // Allocate fixed memory pools on heap
    const client_slots = try base_allocator.alloc(ClientSlot, config.max_clients);
    for (client_slots, 0..) |*slot, index| {
        slot.* = .{};
        const next_index: u32 = if (index + 1 < client_slots.len) @intCast(index + 1) else invalid_client_slot_index;
        slot.next_free.store(next_index, .release);
    }

    // Use shared clock for timestamp
    const ts = clock.now();
    const now = ts.toMilliseconds();

    var server = Server{
        .config = config,
        .base_allocator = base_allocator,
        .address = address,
        .listener = listener,
        .pubsub_map = .init(base_allocator),
        .pubsub_mutex = .init,
        .io = io,

        // Fixed allocations - heap allocated
        .client_slots = client_slots,
        .free_list_head = .init(packFreeListHead(if (client_slots.len == 0) invalid_client_slot_index else 0, 0)),

        // Arena for temporary allocations
        .temp_arena = temp_arena,

        // KV allocator and store
        .kv_allocator = kv_allocator,
        .store = store,
        .registry = registry,
        .pubsub_context = undefined, // Will be initialized after server creation

        // Metadata
        .redisVersion = undefined,
        .createdTime = now,

        // AOF
        .aof_writer = try aof.Writer.init(io, false),
    };

    if (config.requiresAuth()) {
        log.info("Authentication required", .{});
    } else {
        log.debug("No authentication required", .{});
    }

    server.pubsub_context = PubSubContext.init(&server);

    // Prefer AOF to RDB
    // Load AOF file if it exists
    // 'true' to be replaced with user option (use aof/rdb on boot)
    if (true) {
        if (aof.Reader.init(server.temp_arena.allocator(), &server.store, &server.registry, io)) |reader_value| {
            var reader = reader_value;
            log.info("Loading AOF into store", .{});
            reader.read() catch |err| {
                log.warn("Failed to read AOF: {s}", .{@errorName(err)});
            };
        } else |err| {
            log.debug("AOF not available: {s}", .{@errorName(err)});
        }
    } else {
        // Load RDB file if it exists
        if (Reader.rdbFileExists()) {
            if (Reader.init(server.temp_arena.allocator(), &server.store)) |reader_value| {
                var reader = reader_value;
                defer reader.deinit();

                if (reader.readFile()) |data| {
                    log.info("Loading RDB into store", .{});
                    server.createdTime = data.ctime;
                } else |err| {
                    log.warn("Failed to read RDB: {s}", .{@errorName(err)});
                }
            } else |err| {
                log.warn("Failed to initialize RDB reader: {s}", .{@errorName(err)});
            }
        }
    }

    log.info("Server initialized with hybrid allocation - Fixed: {}MB, KV: {}MB, Arena: {}MB", .{
        config.fixedMemorySize() / (1024 * 1024),
        config.kv_memory_budget / (1024 * 1024),
        config.temp_arena_size / (1024 * 1024),
    });

    return server;
}

pub fn deinit(self: *Server) void {
    // Network cleanup
    self.listener.deinit(self.io);

    // Store cleanup (uses KV allocator)
    self.store.deinit();

    // Registry cleanup (uses temp arena)
    self.registry.deinit();

    // Clean up pubsub map
    var iterator = self.pubsub_map.iterator();
    while (iterator.next()) |entry| {
        self.base_allocator.free(entry.key_ptr.*);
        self.base_allocator.free(entry.value_ptr.*);
    }
    self.pubsub_map.deinit();

    // Free heap allocated fixed memory pools
    for (self.client_slots) |*slot| {
        slot.mailbox.deinit(self.base_allocator, self.io);
    }
    self.base_allocator.free(self.client_slots);

    // Allocator cleanup
    self.kv_allocator.deinit();
    self.temp_arena.deinit();

    // AOF Deinit
    self.aof_writer.deinit(self.io);

    log.info("Server deinitialized - all memory freed", .{});
}

// The main server loop. It waits for incoming connections and
// handles each client (one thread per connection).
pub fn listen(self: *Server) !void {
    var connection_group: Io.Group = .init;
    defer connection_group.wait(self.io); // Wait for all clients to finish

    while (true) {
        const conn = self.listener.accept(self.io) catch |err| {
            log.err("Error accepting connection: {s}", .{@errorName(err)});
            continue;
        };

        // Handle this client on its own thread
        connection_group.async(self.io, handleConnectionAsync, .{ self, conn });
    }
}

fn handleConnectionAsync(self: *Server, conn: Stream) void {
    self.handleConnection(conn) catch |err| {
        log.err("Connection error: {s}", .{@errorName(err)});
    };
}

fn handleConnection(self: *Server, conn: Stream) !void {
    // Allocate client from fixed pool
    const allocation = self.allocateClientSlot() orelse {
        log.warn("Maximum client connections reached, rejecting connection", .{});
        conn.close(self.io);
        return;
    };
    const client_slot = allocation.slot;

    // Initialize client in the allocated slot
    client_slot.client = Client.init(
        self.base_allocator,
        conn,
        &self.pubsub_context,
        &self.registry,
        self,
        &self.store,
        allocation.handle,
        &client_slot.mailbox,
        &client_slot.disconnect_requested,
        self.io,
    );
    client_slot.state.store(.active, .release);

    defer {
        _ = self.beginClientShutdown(allocation.handle, client_slot.client.is_in_pubsub_mode);
        // Always clean up and deallocate when connection ends
        client_slot.client.deinit();
        self.deallocateClientSlot(allocation.handle);
        log.debug("Client {} deallocated from pool", .{client_slot.client.client_id});
    }

    try client_slot.client.handle();
    log.debug("Client {} handled", .{client_slot.client.client_id});
}

fn allocateClientSlot(self: *Server) ?ClientAllocation {
    while (true) {
        const head_raw = self.free_list_head.load(.acquire);
        const head = unpackFreeListHead(head_raw);
        if (head.index == invalid_client_slot_index) return null;

        const slot = &self.client_slots[head.index];
        const next_index = slot.next_free.load(.acquire);
        const next_raw = packFreeListHead(next_index, head.tag +% 1);

        if (self.free_list_head.cmpxchgWeak(head_raw, next_raw, .acq_rel, .acquire) == null) {
            slot.disconnect_requested.store(false, .release);
            slot.mailbox.open();
            return .{
                .handle = .{
                    .slot_index = head.index,
                    .generation = slot.generation.load(.acquire),
                },
                .slot = slot,
            };
        }
    }
}

fn deallocateClientSlot(self: *Server, handle: ClientHandle) void {
    if (handle.slot_index >= self.client_slots.len) return;

    const slot = &self.client_slots[handle.slot_index];
    if (slot.generation.load(.acquire) != handle.generation) return;

    slot.mailbox.deinit(self.base_allocator, self.io);
    slot.disconnect_requested.store(false, .release);
    _ = slot.generation.fetchAdd(1, .acq_rel);
    slot.state.store(.free, .release);
    self.pushFreeSlot(handle.slot_index);
}

fn pushFreeSlot(self: *Server, index: u32) void {
    const slot = &self.client_slots[index];
    while (true) {
        const head_raw = self.free_list_head.load(.acquire);
        const head = unpackFreeListHead(head_raw);
        slot.next_free.store(head.index, .release);

        if (self.free_list_head.cmpxchgWeak(head_raw, packFreeListHead(index, head.tag +% 1), .acq_rel, .acquire) == null) {
            return;
        }
    }
}

fn createMessageNode(self: *Server, payload: []const u8) !*MessageNode {
    const owned = try self.base_allocator.dupe(u8, payload);
    errdefer self.base_allocator.free(owned);

    const node = try self.base_allocator.create(MessageNode);
    errdefer self.base_allocator.destroy(node);

    node.* = .{
        .bytes = owned,
        .next = null,
    };
    return node;
}

fn enqueueToHandle(self: *Server, handle: ClientHandle, payload: []const u8) !void {
    if (handle.slot_index >= self.client_slots.len) return error.StaleHandle;

    const slot = &self.client_slots[handle.slot_index];
    const node = try self.createMessageNode(payload);
    errdefer {
        self.base_allocator.free(node.bytes);
        self.base_allocator.destroy(node);
    }

    slot.mailbox.mutex.lockUncancelable(self.io);
    defer slot.mailbox.mutex.unlock(self.io);

    const is_active = slot.state.load(.acquire) == .active and
        slot.generation.load(.acquire) == handle.generation and
        !slot.mailbox.closed.load(.acquire);
    if (!is_active) return error.StaleHandle;
    if (slot.mailbox.pending_count >= slot.mailbox.capacity) return error.OutboxFull;

    if (slot.mailbox.tail) |tail| {
        tail.next = node;
    } else {
        slot.mailbox.head = node;
    }
    slot.mailbox.tail = node;
    slot.mailbox.pending_count += 1;
}

fn beginClientShutdown(self: *Server, handle: ClientHandle, prune_subscriptions: bool) bool {
    if (handle.slot_index >= self.client_slots.len) return false;

    const slot = &self.client_slots[handle.slot_index];
    if (slot.generation.load(.acquire) != handle.generation) return false;

    if (slot.state.cmpxchgStrong(.active, .closing, .acq_rel, .acquire) == null) {
        slot.disconnect_requested.store(true, .release);
        slot.mailbox.close();
        if (prune_subscriptions) {
            self.cleanupDisconnectedPubSubClient(handle);
        }
        return true;
    }

    return false;
}

fn findOrCreateChannelLocked(self: *Server, channel_name: []const u8) !*[]ClientHandle {
    if (self.pubsub_map.getPtr(channel_name)) |ptr| return ptr;

    if (self.pubsub_map.count() >= self.config.max_channels) {
        return error.ChannelLimitReached;
    }

    const owned_key = try self.base_allocator.dupe(u8, channel_name);
    errdefer self.base_allocator.free(owned_key);

    const subscribers = try self.base_allocator.alloc(ClientHandle, 0);
    errdefer self.base_allocator.free(subscribers);

    try self.pubsub_map.put(owned_key, subscribers);
    return self.pubsub_map.getPtr(channel_name).?;
}

fn containsHandle(handles: []const ClientHandle, handle: ClientHandle) bool {
    for (handles) |candidate| {
        if (ClientHandle.eql(candidate, handle)) return true;
    }
    return false;
}

fn removeHandleFromSlice(self: *Server, current_subscribers: []const ClientHandle, handle: ClientHandle) !?[]ClientHandle {
    var remove_index: ?usize = null;
    for (current_subscribers, 0..) |existing_handle, i| {
        if (ClientHandle.eql(existing_handle, handle)) {
            remove_index = i;
            break;
        }
    }

    const index = remove_index orelse return null;
    if (current_subscribers.len == 1) return current_subscribers[0..0];

    const new_subscribers = try self.base_allocator.alloc(ClientHandle, current_subscribers.len - 1);
    @memcpy(new_subscribers[0..index], current_subscribers[0..index]);
    if (index < current_subscribers.len - 1) {
        @memcpy(new_subscribers[index..], current_subscribers[index + 1 ..]);
    }
    return new_subscribers;
}

fn pruneHandlesFromChannelLocked(self: *Server, channel_name: []const u8, handles: []const ClientHandle) !void {
    const current_subscribers = self.pubsub_map.get(channel_name) orelse return;

    var kept_count: usize = 0;
    for (current_subscribers) |existing_handle| {
        if (!containsHandle(handles, existing_handle)) kept_count += 1;
    }

    if (kept_count == current_subscribers.len) return;
    if (kept_count == 0) {
        if (self.pubsub_map.fetchRemove(channel_name)) |removed| {
            self.base_allocator.free(removed.key);
            self.base_allocator.free(removed.value);
        }
        return;
    }

    const next_subscribers = try self.base_allocator.alloc(ClientHandle, kept_count);
    var next_index: usize = 0;
    for (current_subscribers) |existing_handle| {
        if (containsHandle(handles, existing_handle)) continue;
        next_subscribers[next_index] = existing_handle;
        next_index += 1;
    }

    self.base_allocator.free(current_subscribers);
    self.pubsub_map.getPtr(channel_name).?.* = next_subscribers;
}

fn pruneHandlesFromChannel(self: *Server, channel_name: []const u8, handles: []const ClientHandle) !void {
    if (handles.len == 0) return;

    self.pubsub_mutex.lockUncancelable(self.io);
    defer self.pubsub_mutex.unlock(self.io);

    try self.pruneHandlesFromChannelLocked(channel_name, handles);
}

pub fn subscribeToChannel(self: *Server, channel_name: []const u8, handle: ClientHandle) !void {
    self.pubsub_mutex.lockUncancelable(self.io);
    defer self.pubsub_mutex.unlock(self.io);

    const subscribers_ptr = try self.findOrCreateChannelLocked(channel_name);
    const current_subscribers = subscribers_ptr.*;

    for (current_subscribers) |existing_handle| {
        if (ClientHandle.eql(existing_handle, handle)) return;
    }

    if (current_subscribers.len >= self.config.max_subscribers_per_channel) {
        return error.ChannelFull;
    }

    const new_subscribers = try self.base_allocator.realloc(current_subscribers, current_subscribers.len + 1);
    new_subscribers[new_subscribers.len - 1] = handle;
    subscribers_ptr.* = new_subscribers;
}

pub fn unsubscribeFromChannel(self: *Server, channel_name: []const u8, handle: ClientHandle) !void {
    self.pubsub_mutex.lockUncancelable(self.io);
    defer self.pubsub_mutex.unlock(self.io);

    const current_subscribers = self.pubsub_map.get(channel_name) orelse return;
    const new_subscribers = try self.removeHandleFromSlice(current_subscribers, handle) orelse return;

    if (new_subscribers.len == 0) {
        if (self.pubsub_map.fetchRemove(channel_name)) |removed| {
            self.base_allocator.free(removed.key);
            self.base_allocator.free(removed.value);
        }
        return;
    }

    self.base_allocator.free(current_subscribers);
    self.pubsub_map.getPtr(channel_name).?.* = new_subscribers;
}

pub fn publishToChannel(self: *Server, channel_name: []const u8, payload: []const u8) !usize {
    self.pubsub_mutex.lockSharedUncancelable(self.io);
    const current_subscribers = self.pubsub_map.get(channel_name) orelse {
        self.pubsub_mutex.unlockShared(self.io);
        return 0;
    };
    const snapshot = try self.base_allocator.dupe(ClientHandle, current_subscribers);
    self.pubsub_mutex.unlockShared(self.io);
    defer self.base_allocator.free(snapshot);

    var stale_handles: std.ArrayList(ClientHandle) = .empty;
    defer stale_handles.deinit(self.base_allocator);

    var messages_sent: usize = 0;
    for (snapshot) |handle| {
        if (self.enqueueToHandle(handle, payload)) |_| {
            messages_sent += 1;
        } else |err| switch (err) {
            error.StaleHandle => try stale_handles.append(self.base_allocator, handle),
            error.OutboxFull => {
                _ = self.beginClientShutdown(handle, true);
                try stale_handles.append(self.base_allocator, handle);
            },
            else => return err,
        }
    }

    if (stale_handles.items.len > 0) {
        try self.pruneHandlesFromChannel(channel_name, stale_handles.items);
    }

    return messages_sent;
}

pub fn cleanupDisconnectedPubSubClient(self: *Server, handle: ClientHandle) void {
    self.pubsub_mutex.lockUncancelable(self.io);
    defer self.pubsub_mutex.unlock(self.io);

    var empty_channels: std.ArrayList([]const u8) = .empty;
    defer empty_channels.deinit(self.base_allocator);

    var channel_iterator = self.pubsub_map.iterator();
    while (channel_iterator.next()) |entry| {
        const updated_subscribers = self.removeHandleFromSlice(entry.value_ptr.*, handle) catch |err| {
            log.warn("Failed to unsubscribe slot {} from channel {s}: {s}", .{
                handle.slot_index,
                entry.key_ptr.*,
                @errorName(err),
            });
            continue;
        } orelse continue;

        if (updated_subscribers.len == 0) {
            empty_channels.append(self.base_allocator, entry.key_ptr.*) catch |err| {
                log.warn("Failed to queue pubsub channel cleanup for slot {}: {s}", .{
                    handle.slot_index,
                    @errorName(err),
                });
            };
            continue;
        }

        self.base_allocator.free(entry.value_ptr.*);
        entry.value_ptr.* = updated_subscribers;
    }

    for (empty_channels.items) |channel_name| {
        if (self.pubsub_map.fetchRemove(channel_name)) |removed| {
            self.base_allocator.free(removed.key);
            self.base_allocator.free(removed.value);
        }
    }
}

// Memory statistics
pub fn getMemoryStats(self: *Server) Config.MemoryStats {
    const fixed_size = self.config.fixedMemorySize();
    const total_budget = self.config.totalMemoryBudget();
    return Config.MemoryStats{
        .fixed_memory_used = fixed_size,
        .kv_memory_used = self.kv_allocator.getMemoryUsage(),
        .temp_arena_used = self.temp_arena.queryCapacity() - self.temp_arena.state.buffer_list.first.?.data.len,
        .total_allocated = fixed_size + self.kv_allocator.getMemoryUsage() +
            (self.temp_arena.queryCapacity() - self.temp_arena.state.buffer_list.first.?.data.len),
        .total_budget = total_budget,
    };
}
pub fn getChannelCount(self: *Server) u32 {
    self.pubsub_mutex.lockSharedUncancelable(self.io);
    defer self.pubsub_mutex.unlockShared(self.io);
    return @intCast(self.pubsub_map.count());
}

const testing = std.testing;

fn initTestServer(allocator: Allocator, max_clients: u32) !Server {
    const client_slots = try allocator.alloc(ClientSlot, max_clients);
    for (client_slots, 0..) |*slot, index| {
        slot.* = .{};
        const next_index: u32 = if (index + 1 < client_slots.len) @intCast(index + 1) else invalid_client_slot_index;
        slot.next_free.store(next_index, .release);
    }

    var server = Server{
        .config = .{
            .max_clients = max_clients,
            .max_channels = 16,
            .max_subscribers_per_channel = 16,
        },
        .base_allocator = allocator,
        .address = undefined,
        .listener = undefined,
        .io = testing.io,
        .client_slots = client_slots,
        .free_list_head = .init(packFreeListHead(if (client_slots.len == 0) invalid_client_slot_index else 0, 0)),
        .pubsub_map = .init(allocator),
        .pubsub_mutex = .init,
        .temp_arena = undefined,
        .kv_allocator = undefined,
        .store = undefined,
        .registry = undefined,
        .pubsub_context = undefined,
        .redisVersion = null,
        .createdTime = 0,
        .aof_writer = undefined,
    };
    server.pubsub_context = PubSubContext.init(&server);
    return server;
}

fn deinitTestServer(server: *Server) void {
    var iterator = server.pubsub_map.iterator();
    while (iterator.next()) |entry| {
        server.base_allocator.free(entry.key_ptr.*);
        server.base_allocator.free(entry.value_ptr.*);
    }
    server.pubsub_map.deinit();

    for (server.client_slots) |*slot| {
        slot.mailbox.deinit(server.base_allocator, server.io);
    }
    server.base_allocator.free(server.client_slots);
}

test "Server reuses freed slots with a new generation" {
    var server = try initTestServer(testing.allocator, 2);
    defer deinitTestServer(&server);

    const first = server.allocateClientSlot().?;
    try testing.expectEqual(@as(u32, 0), first.handle.slot_index);
    try testing.expectEqual(@as(u32, 0), first.handle.generation);

    server.deallocateClientSlot(first.handle);

    const second = server.allocateClientSlot().?;
    try testing.expectEqual(@as(u32, 0), second.handle.slot_index);
    try testing.expectEqual(@as(u32, 1), second.handle.generation);
}

test "Server publishToChannel enqueues active subscribers and prunes stale handles" {
    var server = try initTestServer(testing.allocator, 2);
    defer deinitTestServer(&server);

    const active = server.allocateClientSlot().?;
    active.slot.state.store(.active, .release);

    try server.subscribeToChannel("news", active.handle);
    try server.subscribeToChannel("news", .{ .slot_index = 1, .generation = 99 });

    const delivered = try server.publishToChannel("news", "payload");
    try testing.expectEqual(@as(usize, 1), delivered);

    const queued = active.slot.mailbox.takeAll(server.io);
    defer freeMessageList(server.base_allocator, queued);

    try testing.expect(queued != null);
    try testing.expectEqualStrings("payload", queued.?.bytes);

    server.pubsub_mutex.lockSharedUncancelable(server.io);
    defer server.pubsub_mutex.unlockShared(server.io);

    const subscribers = server.pubsub_map.get("news").?;
    try testing.expectEqual(@as(usize, 1), subscribers.len);
    try testing.expect(ClientHandle.eql(active.handle, subscribers[0]));
}
