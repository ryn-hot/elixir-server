#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE_ROOT="$(cd "$ROOT/.." && pwd)"
OUTPUT_ROOT="$ROOT/extensions/probe/linux"
X86_64_IMAGE="${X86_64_IMAGE:-messense/rust-musl-cross:x86_64-musl}"
AARCH64_IMAGE="${AARCH64_IMAGE:-messense/rust-musl-cross:aarch64-musl}"

build_probe() {
  local arch="$1"
  local image="$2"
  local output_dir="$OUTPUT_ROOT/$arch"

  mkdir -p "$output_dir"

  docker run --rm \
    --entrypoint /bin/sh \
    -v "$WORKSPACE_ROOT:/workspace:ro" \
    -v "$output_dir:/out" \
    -e CARGO_TARGET_DIR=/tmp/target \
    -w /workspace \
    "$image" \
    -lc "set -e; \
      cargo build --release --quiet --manifest-path /workspace/crates/elixir_probe/Cargo.toml; \
      cp /tmp/target/\$CARGO_BUILD_TARGET/release/elixir-probe /out/elixir-probe; \
      chmod 755 /out/elixir-probe"
}

build_probe "x86_64" "$X86_64_IMAGE"
build_probe "aarch64" "$AARCH64_IMAGE"
