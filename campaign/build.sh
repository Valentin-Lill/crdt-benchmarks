#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p bin results
cargo +1.96.0 build --release --locked > results/build-time.log 2>&1
cp target/release/crdt-benchmark bin/night-native
cargo +1.96.0 build --release --locked --features allocation-metrics > results/build-memory.log 2>&1
cp target/release/crdt-benchmark bin/night-native-memory
