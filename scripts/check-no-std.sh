#!/bin/sh
# Check a supported OS target with Rust std deliberately absent from its sysroot.
set -eu
root=$(pwd)
target=${NO_STD_TARGET:-$(rustc -vV | awk '/^host:/ {print $2}')}
libdir=$(rustc --print target-libdir --target "$target")
scratch="$root/target/no-std-sysroot/$target"
mkdir -p "$scratch/lib/rustlib/$target/lib"
for library in "$libdir"/*; do
    name=$(basename "$library")
    case "$name" in
        libstd-*|libstd_detect-*|libtest-*|libproc_macro-*|librustc_std_workspace_std-*|std-*|std_detect-*|test-*|proc_macro-*)
            rm -f "$scratch/lib/rustlib/$target/lib/$name"
            continue ;;
    esac
    ln -sf "$library" "$scratch/lib/rustlib/$target/lib/$name"
done
printf '%s\n' 'pub fn forbidden() { std::mem::drop(1u8); }' > "$scratch/negative.rs"
if rustc --crate-type lib --emit metadata --target "$target" --sysroot "$scratch" "$scratch/negative.rs" -o "$scratch/negative.rmeta" > "$scratch/negative.log" 2>&1; then
    echo 'std unexpectedly available in negative control' >&2
    exit 1
fi
if ! grep -q "can't find crate for .*std" "$scratch/negative.log"; then
    cat "$scratch/negative.log" >&2
    exit 1
fi
printf '%s\n' '#![no_std]' 'extern crate alloc;' 'pub fn allowed(v: alloc::vec::Vec<u8>) -> usize { v.len() }' > "$scratch/positive.rs"
rustc --crate-type lib --emit metadata --target "$target" --sysroot "$scratch" "$scratch/positive.rs" -o "$scratch/positive.rmeta"
# Explicit --target keeps proc macros and build scripts on the ordinary host sysroot.
CARGO_ENCODED_RUSTFLAGS=$(printf '%s\037%s' --sysroot "$scratch")
export CARGO_ENCODED_RUSTFLAGS
cargo check --target "$target" "$@"
