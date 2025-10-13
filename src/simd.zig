const std = @import("std");

// SIMD-optimized string equality with threshold-based optimization
pub fn simdStringEql(a: []const u8, b: []const u8) bool {
    // Fast path: length check
    if (a.len != b.len) return false;
    if (a.len == 0) return true;

    // Fast path: pointer equality check (for interned strings)
    // This is extremely common in Redis since keys are interned
    if (a.ptr == b.ptr) return true;

    // For strings < 45 bytes, std.mem.eql is faster due to:
    // - Highly optimized LLVM intrinsics
    // - Lower overhead for small comparisons
    // - Better branch prediction
    // Most Redis keys fall into this category (user:123, cache:key, session:abc, etc.)
    if (a.len < 45) {
        return std.mem.eql(u8, a, b);
    }

    // For longer strings (≥45 bytes), use explicit SIMD vectorization
    // Fixed 16-byte vectors for consistent performance across platforms
    const vec_len = 16;
    const Vec = @Vector(vec_len, u8);
    var i: usize = 0;

    // Process 16-byte chunks with SIMD using XOR technique
    // XOR is slightly more efficient than equality comparison + reduce
    while (i + vec_len <= a.len) : (i += vec_len) {
        const va: Vec = a[i..][0..vec_len].*;
        const vb: Vec = b[i..][0..vec_len].*;
        const xor_result = va ^ vb;

        // If XOR result is all zeros, the vectors are equal
        // Check if any byte is non-zero (which means difference)
        if (@reduce(.Or, xor_result != @as(Vec, @splat(0)))) {
            return false;
        }
    }

    // Handle remaining bytes efficiently with std.mem.eql
    // This is faster than a scalar loop for the tail
    return std.mem.eql(u8, a[i..], b[i..]);
}
