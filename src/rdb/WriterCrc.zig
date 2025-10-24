const std = @import("std");

underlying_writer: *std.Io.Writer,
checksum: std.hash.crc.Crc64Redis,
interface: std.Io.Writer,

const WriterCrc = @This();

pub fn init(underlying: *std.Io.Writer, buffer: []u8) WriterCrc {
    return .{
        .underlying_writer = underlying,
        .checksum = .init(),
        .interface = .{
            .vtable = &.{
                .drain = drain,
                .flush = flush,
                .sendFile = std.Io.Writer.unimplementedSendFile,
                .rebase = std.Io.Writer.defaultRebase,
            },
            .buffer = buffer,
        },
    };
}

fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
    _ = splat;
    const self: *WriterCrc = @fieldParentPtr("interface", w);

    // Capture the bytes and update checksum
    const bytes_to_write = data[0];
    self.checksum.update(bytes_to_write);

    // Write to underlying writer (don't flush here - that's done by the flush vtable function)
    try self.underlying_writer.writeAll(bytes_to_write);

    return bytes_to_write.len;
}

fn flush(w: *std.Io.Writer) std.Io.Writer.Error!void {
    const self: *WriterCrc = @fieldParentPtr("interface", w);

    // First, drain any buffered data in WriterCrc's own buffer
    while (w.end > 0) {
        _ = try drain(w, &.{w.buffer[0..w.end]}, 0);
        w.end = 0;
    }

    // Then flush the underlying writer
    try self.underlying_writer.flush();
}

pub fn writer(self: *WriterCrc) *std.Io.Writer {
    return &self.interface;
}

pub fn getChecksum(self: *WriterCrc) u64 {
    return self.checksum.final();
}
