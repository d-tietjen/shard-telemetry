//! Select only the reviewed Pco source/layout domain; ordinary decoding is unaffected.
use std::env;
use std::process::Command;

fn selected() -> bool {
    // These affect the compiler identity/effective layout. Unknown configurations
    // retain their parent and refuse only the new bounded timestamp operation.
    for name in [
        "RUSTC_BOOTSTRAP",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
    ] {
        if env::var_os(name).is_some_and(|value| !value.is_empty()) {
            return false;
        }
    }
    if env::vars_os().any(|(name, value)| {
        name.to_string_lossy().starts_with("CARGO_PROFILE_") && !value.is_empty()
    }) {
        return false;
    }
    for (name, expected) in [
        ("TARGET", "x86_64-unknown-linux-gnu"),
        ("CARGO_CFG_TARGET_ENDIAN", "little"),
        ("CARGO_CFG_TARGET_POINTER_WIDTH", "64"),
        ("CARGO_CFG_TARGET_FEATURE", "fxsr,sse,sse2"),
    ] {
        if env::var(name).as_deref() != Ok(expected) {
            return false;
        }
    }
    if !matches!(
        env::var("CARGO_CFG_PANIC").as_deref(),
        Ok("unwind" | "abort")
    ) || !matches!(env::var("OPT_LEVEL").as_deref(), Ok("0" | "3"))
    {
        return false;
    }
    let Some(compiler) = env::var_os("RUSTC") else {
        return false;
    };
    let Ok(actual) = Command::new(compiler).arg("-vV").output() else {
        return false;
    };
    if !actual.status.success() {
        return false;
    }
    let Ok(identity) = std::str::from_utf8(&actual.stdout) else {
        return false;
    };
    identity.lines().any(|line| line == "release: 1.93.0")
        && identity
            .lines()
            .any(|line| line == "commit-hash: 254b59607d4417e9dffbc307138ae5c86280fe4c")
        && identity
            .lines()
            .any(|line| line == "host: x86_64-unknown-linux-gnu")
}

fn main() {
    println!("cargo:rustc-check-cfg=cfg(telemetry_bounded_pco_layout)");
    println!("cargo:rerun-if-changed=build.rs");
    for name in [
        "RUSTC",
        "RUSTC_BOOTSTRAP",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "TARGET",
        "CARGO_CFG_TARGET_ENDIAN",
        "CARGO_CFG_TARGET_POINTER_WIDTH",
        "CARGO_CFG_TARGET_FEATURE",
        "CARGO_CFG_PANIC",
        "OPT_LEVEL",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
    }
    for profile in ["DEV", "RELEASE"] {
        for field in [
            "OPT_LEVEL",
            "DEBUG",
            "STRIP",
            "DEBUG_ASSERTIONS",
            "OVERFLOW_CHECKS",
            "LTO",
            "PANIC",
            "INCREMENTAL",
            "CODEGEN_UNITS",
            "RPATH",
            "SPLIT_DEBUGINFO",
        ] {
            println!("cargo:rerun-if-env-changed=CARGO_PROFILE_{profile}_{field}");
        }
    }
    for (name, _) in
        env::vars_os().filter(|(name, _)| name.to_string_lossy().starts_with("CARGO_PROFILE_"))
    {
        println!("cargo:rerun-if-env-changed={}", name.to_string_lossy());
    }
    // Both normal standalone dev/release (unwind) and canonical host release
    // (abort) select the same source geometry. Actual release artifact/layout
    // qualification is a separate acceptance gate, not proved by PROFILE.
    if selected() {
        println!("cargo:rustc-cfg=telemetry_bounded_pco_layout");
    }
}
