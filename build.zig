// build.zig
// This is the build script for our Zig Redis project.
// To build the project, run `zig build` in your terminal.
// To run the server, execute `zig build run`.

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "zedis",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // This makes the standard library available to our project.
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the application");
    run_step.dependOn(&run_cmd.step);

    // For ZLS - builds but doesn't install anything
    const check_exe = b.addExecutable(.{ .name = "check", .root_module = exe.root_module });
    const check_step = b.step("check", "check for build errors");
    check_step.dependOn(&check_exe.step);

    // Test steps - enhanced test runner system
    const unit_tests = b.addTest(.{
        .name = "test-unit",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/unit_tests.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = b.args orelse &.{},
    });

    const run_unit_tests = b.addRunArtifact(unit_tests);
    run_unit_tests.setEnvironmentVariable("ZIG_EXE", b.graph.zig_exe);

    // Don't cache test results if running with specific args (filters, etc.)
    if (b.args != null) {
        run_unit_tests.has_side_effects = true;
    }

    // Main test commands
    const test_step = b.step("test", "Run all unit tests");
    test_step.dependOn(&run_unit_tests.step);

    const test_unit_step = b.step("test:unit", "Run unit tests only");
    test_unit_step.dependOn(&run_unit_tests.step);

    const test_build_step = b.step("test:build", "Build tests without running");
    test_build_step.dependOn(&b.addInstallArtifact(unit_tests, .{}).step);

    // Format checking
    const fmt_step = b.step("test:fmt", "Check code formatting");
    const run_fmt = b.addFmt(.{ .paths = &.{"src"}, .check = true });
    fmt_step.dependOn(&run_fmt.step);

    // Integration testing (for future expansion)
    const test_integration_step = b.step("test:integration", "Run integration tests");
    // For now, just depend on unit tests - can be expanded later
    test_integration_step.dependOn(&run_unit_tests.step);

    // All tests (unit + format + integration)
    const test_all_step = b.step("test:all", "Run all tests including formatting checks");
    test_all_step.dependOn(&run_unit_tests.step);
    test_all_step.dependOn(fmt_step);
}
