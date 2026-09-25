#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NODE_ROOT="${NODE_MUTEX_ROOT:-$ROOT/live-mutex}"
RUST_ROOT="${RUST_MUTEX_ROOT:-$ROOT/live-mutex-rs}"
EVIDENCE_DIR="${EVIDENCE_DIR:-$ROOT/evidence/live-mutex-polyglot}"
NODE_PORT="${NODE_MUTEX_PORT:-7970}"
RUST_PORT="${RUST_MUTEX_PORT:-6970}"

mkdir -p "$EVIDENCE_DIR"

NODE_BROKER_PID=''
RUST_BROKER_PID=''

cleanup() {
  if [ -n "$NODE_BROKER_PID" ]; then
    kill "$NODE_BROKER_PID" 2>/dev/null || true
    wait "$NODE_BROKER_PID" 2>/dev/null || true
  fi

  if [ -n "$RUST_BROKER_PID" ]; then
    kill "$RUST_BROKER_PID" 2>/dev/null || true
    wait "$RUST_BROKER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "FATAL: required command not found: $command_name" >&2
    return 1
  fi
}

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local attempts=120

  for ((attempt = 1; attempt <= attempts; attempt += 1)); do
    if (echo >"/dev/tcp/$host/$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "FATAL: broker did not become ready at $host:$port" >&2
  return 1
}

run_case() {
  local broker="$1"
  local language="$2"
  shift 2

  echo "::group::${broker} broker / ${language} client"
  "$@"
  echo "::endgroup::"
}

write_receipt() {
  local broker="$1"
  local source_sha="$2"
  local clients_json="$3"
  local receipt_path="$EVIDENCE_DIR/${broker}.json"

  cat >"$receipt_path" <<EOF
{
  "schema": "fiducia-cloud-test.live-mutex-polyglot-e2e-receipt/v1",
  "broker": "$broker",
  "source_sha": "$source_sha",
  "client_count": 9,
  "clients": $clients_json,
  "result": "passed"
}
EOF
}

for command_name in bash cargo c++ dart gleam go java make mvn node npm pwsh python3; do
  require_command "$command_name"
done

NODE_SHA="$(git -C "$NODE_ROOT" rev-parse HEAD)"
RUST_SHA="$(git -C "$RUST_ROOT" rev-parse HEAD)"

# -----------------------------------------------------------------------------
# Node/TypeScript Broker1 with nine real client processes.
# -----------------------------------------------------------------------------
(
  cd "$NODE_ROOT"
  npm ci --ignore-scripts --no-audit --no-fund
  npm run compile
)

NODE_FENCING_STATE="$EVIDENCE_DIR/node-fencing-watermark.json"
LMX_FENCING_TOKEN_STATE_PATH="$NODE_FENCING_STATE" \
  node -e "const {Broker1}=require('${NODE_ROOT}/dist/main.js'); new Broker1({port:${NODE_PORT},host:'127.0.0.1'}).ensure().then(()=>console.log('node-broker-ready'));" \
  >"$EVIDENCE_DIR/node-broker.log" 2>&1 &
NODE_BROKER_PID="$!"
wait_for_tcp 127.0.0.1 "$NODE_PORT"

export LMX_HOST=127.0.0.1
export LMX_PORT="$NODE_PORT"
export LIVE_MUTEX_HOST=127.0.0.1
export LIVE_MUTEX_PORT="$NODE_PORT"

run_case node rust bash -lc "cd '$NODE_ROOT/clients/rust' && cargo run --quiet --example smoke"

python3 -m venv "$EVIDENCE_DIR/node-python-venv"
"$EVIDENCE_DIR/node-python-venv/bin/pip" install --quiet -e "$NODE_ROOT/clients/python"
run_case node python "$EVIDENCE_DIR/node-python-venv/bin/python" -m live_mutex_client.smoke

run_case node shell "$NODE_ROOT/clients/shell/smoke.sh"
run_case node powershell pwsh "$NODE_ROOT/clients/powershell/smoke.ps1"
run_case node go bash -lc "cd '$NODE_ROOT/clients/go' && go run ./cmd/smoke"
run_case node dart bash -lc "cd '$NODE_ROOT/clients/dart' && dart pub get >/dev/null && dart run example/smoke.dart"
run_case node java mvn -q -f "$NODE_ROOT/clients/java/pom.xml" exec:java
run_case node cpp make -C "$NODE_ROOT/clients/cpp" run
run_case node gleam bash -lc "cd '$NODE_ROOT/clients/gleam' && LIVE_MUTEX_SMOKE=1 gleam test"

write_receipt \
  node \
  "$NODE_SHA" \
  '["rust","python","shell","powershell","go","dart","java","cpp","gleam"]'

kill "$NODE_BROKER_PID"
wait "$NODE_BROKER_PID" 2>/dev/null || true
NODE_BROKER_PID=''

# -----------------------------------------------------------------------------
# Rust broker with nine real client processes.
# -----------------------------------------------------------------------------
(
  cd "$RUST_ROOT"
  cargo build --release --locked --no-default-features --bin dd-rust-network-mutex
)

"$RUST_ROOT/target/release/dd-rust-network-mutex" \
  >"$EVIDENCE_DIR/rust-broker.log" 2>&1 &
RUST_BROKER_PID="$!"
wait_for_tcp 127.0.0.1 "$RUST_PORT"

export LMX_HOST=127.0.0.1
export LMX_PORT="$RUST_PORT"
export LIVE_MUTEX_HOST=127.0.0.1
export LIVE_MUTEX_PORT="$RUST_PORT"

(
  cd "$RUST_ROOT/clients/ts"
  npm ci --ignore-scripts --no-audit --no-fund
)
run_case rust typescript bash -lc "cd '$RUST_ROOT/clients/ts' && npm run smoke"
run_case rust go bash -lc "cd '$RUST_ROOT/clients/go' && go run ./cmd/smoke"
run_case rust dart bash -lc "cd '$RUST_ROOT/clients/dart' && dart pub get >/dev/null && dart run bin/smoke.dart"
run_case rust gleam bash -lc "cd '$RUST_ROOT/clients/gleam' && LIVE_MUTEX_SMOKE=1 gleam test"
run_case rust python python3 "$RUST_ROOT/clients/python/smoke.py"
run_case rust cpp make -C "$RUST_ROOT/clients/cpp" run
run_case rust java bash -lc "cd '$RUST_ROOT' && clients/java/build.sh >/dev/null && java -cp clients/java/out com.oresoftware.networkmutex.Smoke"
run_case rust shell "$RUST_ROOT/clients/shell/smoke.sh"
run_case rust powershell pwsh "$RUST_ROOT/clients/powershell/smoke.ps1"

write_receipt \
  rust \
  "$RUST_SHA" \
  '["typescript","go","dart","gleam","python","cpp","java","shell","powershell"]'

kill "$RUST_BROKER_PID"
wait "$RUST_BROKER_PID" 2>/dev/null || true
RUST_BROKER_PID=''

echo "live-mutex polyglot E2E passed: 9 clients against node + 9 clients against rust"
