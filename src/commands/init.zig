const std = @import("std");
const Allocator = std.mem.Allocator;
const CommandRegistry = @import("registry.zig").CommandRegistry;
const connection_commands = @import("connection.zig");
const string = @import("string.zig");
const list = @import("list.zig");
const rdb = @import("../commands/rdb.zig");
const pubsub = @import("../commands/pubsub.zig");
const ts = @import("../commands/time_series.zig");
const key = @import("../commands/keys.zig");
const server_commands = @import("../commands/server.zig");

pub fn initRegistry(allocator: Allocator) !CommandRegistry {
    var registry = CommandRegistry.init(allocator);

    try registry.register(.{
        .name = "PING",
        .handler = .{ .default = connection_commands.ping },
        .min_args = 1,
        .max_args = 2,
        .description = "Ping the server",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "ECHO",
        .handler = .{ .default = connection_commands.echo },
        .min_args = 2,
        .max_args = 2,
        .description = "Echo the given string",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "QUIT",
        .handler = .{ .client_handler = connection_commands.quit },
        .min_args = 1,
        .max_args = 1,
        .description = "Close the connection",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "SET",
        .handler = .{ .store_handler = string.set },
        .min_args = 3,
        .max_args = 3,
        .description = "Set string value of a key",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "GET",
        .handler = .{ .store_handler = string.get },
        .min_args = 2,
        .max_args = 2,
        .description = "Get string value of a key",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "INCR",
        .handler = .{ .store_handler = string.incr },
        .min_args = 2,
        .max_args = 2,
        .description = "Increment the value of a key",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "DECR",
        .handler = .{ .store_handler = string.decr },
        .min_args = 2,
        .max_args = 2,
        .description = "Decrement the value of a key",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "HELP",
        .handler = .{ .default = connection_commands.help },
        .min_args = 1,
        .max_args = 1,
        .description = "Show help message",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "DEL",
        .handler = .{ .store_handler = string.del },
        .min_args = 2,
        .max_args = null,
        .description = "Delete key",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "SAVE",
        .handler = .{ .client_handler = rdb.save },
        .min_args = 1,
        .max_args = 1,
        .description = "The SAVE commands performs a synchronous save of the dataset producing a point in time snapshot of all the data inside the Redis instance, in the form of an RDB file.",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "PUBLISH",
        .handler = .{ .client_handler = pubsub.publish },
        .min_args = 3,
        .max_args = 3,
        .description = "Publish message",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "SUBSCRIBE",
        .handler = .{ .client_handler = pubsub.subscribe },
        .min_args = 2,
        .max_args = null,
        .description = "Subscribe to channels",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "EXPIRE",
        .handler = .{ .store_handler = string.expire },
        .min_args = 3,
        .max_args = null,
        .description = "Expire key",
        // TODO: convert to expireat
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "EXPIREAT",
        .handler = .{ .store_handler = string.expireAt },
        .min_args = 3,
        .max_args = null,
        .description = "Expire key",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "AUTH",
        .handler = .{ .client_handler = connection_commands.auth },
        .min_args = 2,
        .max_args = 2,
        .description = "Authenticate to the server",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "SELECT",
        .handler = .{ .client_handler = connection_commands.select },
        .min_args = 2,
        .max_args = 2,
        .description = "Select a database (0-15)",
        .write_to_aof = false,
    });

    // List commands: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE

    try registry.register(.{
        .name = "LPUSH",
        .handler = .{ .store_handler = list.lpush },
        .min_args = 3,
        .max_args = null,
        .description = "Prepend one or multiple values to a list",
        // TODO: test
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "RPUSH",
        .handler = .{ .store_handler = list.rpush },
        .min_args = 3,
        .max_args = null,
        .description = "Append one or multiple values to a list",
        // TODO: test
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "LPOP",
        .handler = .{ .store_handler = list.lpop },
        .min_args = 2,
        .max_args = 3,
        .description = "Remove and return the first element of a list",
        // TODO: test
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "RPOP",
        .handler = .{ .store_handler = list.rpop },
        .min_args = 2,
        .max_args = 3,
        .description = "Remove and return the last element of a list",
        // TODO: test
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "LLEN",
        .handler = .{ .store_handler = list.llen },
        .min_args = 2,
        .max_args = 2,
        .description = "Get the length of a list",
        // TODO: test
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "LINDEX",
        .handler = .{ .store_handler = list.lindex },
        .min_args = 3,
        .max_args = 3,
        .description = "Get an element from a list by its index",
        // TODO: test
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "LSET",
        .handler = .{ .store_handler = list.lset },
        .min_args = 4,
        .max_args = 4,
        .description = "Set the value of an element in a list by its index",
        // TODO: test
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "LRANGE",
        .handler = .{ .store_handler = list.lrange },
        .min_args = 4,
        .max_args = 4,
        .description = "Get a range of elements from a list",
        // TODO: test
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "APPEND",
        .handler = .{ .store_handler = string.append },
        .min_args = 3,
        .max_args = 3,
        .description = "Append a value to a key",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "STRLEN",
        .handler = .{ .store_handler = string.strlen },
        .min_args = 2,
        .max_args = 2,
        .description = "Get the length of the value stored in a key",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "GETSET",
        .handler = .{ .store_handler = string.getset },
        .min_args = 3,
        .max_args = 3,
        .description = "Set a key and return its old value",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "MGET",
        .handler = .{ .store_handler = string.mget },
        .min_args = 2,
        .max_args = null,
        .description = "Get the values of multiple keys",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "MSET",
        .handler = .{ .store_handler = string.mset },
        .min_args = 3,
        .max_args = null,
        .description = "Set multiple key-value pairs",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "SETEX",
        .handler = .{ .store_handler = string.setex },
        .min_args = 4,
        .max_args = 4,
        .description = "Set a key with expiration time",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "SETNX",
        .handler = .{ .store_handler = string.setnx },
        .min_args = 3,
        .max_args = 3,
        .description = "Set a key only if it doesn't exist",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "INCRBY",
        .handler = .{ .store_handler = string.incrby },
        .min_args = 3,
        .max_args = 3,
        .description = "Increment a key by a specific amount",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "DECRBY",
        .handler = .{ .store_handler = string.decrby },
        .min_args = 3,
        .max_args = 3,
        .description = "Decrement a key by a specific amount",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "INCRBYFLOAT",
        .handler = .{ .store_handler = string.incrbyfloat },
        .min_args = 3,
        .max_args = 3,
        .description = "Increment a key by a floating point number",
        .write_to_aof = true,
    });

    // Key commands

    try registry.register(.{
        .name = "KEYS",
        .handler = .{ .store_handler = key.keys },
        .min_args = 2,
        .max_args = 2,
        .description = "Find all keys matching a pattern",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "EXISTS",
        .handler = .{ .store_handler = key.exists },
        .min_args = 2,
        .max_args = null,
        .description = "Check if key exists",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "TTL",
        .handler = .{ .store_handler = key.ttl },
        .min_args = 2,
        .max_args = 2,
        .description = "Get remaining time to live of a key",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "PERSIST",
        .handler = .{ .store_handler = key.persist },
        .min_args = 2,
        .max_args = 2,
        .description = "Remove expiration from a key",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "TYPE",
        .handler = .{ .store_handler = key.typeCmd },
        .min_args = 2,
        .max_args = 2,
        .description = "Get the data type of a key",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "RENAME",
        .handler = .{ .store_handler = key.rename },
        .min_args = 3,
        .max_args = 3,
        .description = "Rename a key",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "RANDOMKEY",
        .handler = .{ .store_handler = key.randomkey },
        .min_args = 1,
        .max_args = 1,
        .description = "Return a random key",
        .write_to_aof = false,
    });

    // Time series commands
    try registry.register(.{
        .name = "TS.CREATE",
        .handler = .{ .store_handler = ts.ts_create },
        .min_args = 2,
        .max_args = null,
        .description = "Create a new time series",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "TS.ADD",
        .handler = .{ .store_handler = ts.ts_add },
        .min_args = 4,
        .max_args = null,
        .description = "Add a new sample to a time series",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "TS.GET",
        .handler = .{ .store_handler = ts.ts_get },
        .min_args = 2,
        .max_args = 2,
        .description = "Get the last sample from a time series",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "TS.INCRBY",
        .handler = .{ .store_handler = ts.ts_incrby },
        .min_args = 4,
        .max_args = 4,
        .description = "Increment the last value and add as a new sample",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "TS.DECRBY",
        .handler = .{ .store_handler = ts.ts_decrby },
        .min_args = 4,
        .max_args = 4,
        .description = "Decrement the last value and add as a new sample",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "TS.ALTER",
        .handler = .{ .store_handler = ts.ts_alter },
        .min_args = 2,
        .max_args = null,
        .description = "Alter time series properties",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "TS.ALTER",
        .handler = .{ .store_handler = ts.ts_alter },
        .min_args = 2,
        .max_args = null,
        .description = "Alter time series properties",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "TS.RANGE",
        .handler = .{ .store_handler = ts.ts_range },
        .min_args = 4,
        .max_args = null,
        .description = "Query a range of samples from a time series",
        .write_to_aof = false,
    });

    // Server commands

    try registry.register(.{
        .name = "DBSIZE",
        .handler = .{ .store_handler = server_commands.db_size },
        .min_args = 1,
        .max_args = 1,
        .description = "Get database size",
        .write_to_aof = false,
    });

    try registry.register(.{
        .name = "FLUSHDB",
        .handler = .{ .client_handler = server_commands.flush_db },
        .min_args = 1,
        .max_args = 1,
        .description = "Flush the current database",
        .write_to_aof = true,
    });

    try registry.register(.{
        .name = "FLUSHALL",
        .handler = .{ .client_handler = server_commands.flush_all },
        .min_args = 1,
        .max_args = 1,
        .description = "Flush all databases",
        .write_to_aof = true,
    });

    return registry;
}
