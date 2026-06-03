#!/usr/bin/env bash
# Cross-language interop proof: a Go writer must exclude a Node writer on the same
# resource (mutual exclusion via the shared FUNCTION library), and the fencing counter
# must be shared/monotonic across languages. Requires go, node, redis-server on PATH,
# and the Node client built (clients/node/dist).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RES="interop:demo"
PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("",0));print(s.getsockname()[1]);s.close()')"
ADDR="127.0.0.1:${PORT}"

echo ">> starting redis on ${ADDR}"
redis-server --port "${PORT}" --save "" --appendonly no --daemonize yes >/dev/null
trap 'redis-cli -p "${PORT}" shutdown nosave >/dev/null 2>&1 || true' EXIT
for _ in $(seq 1 50); do redis-cli -p "${PORT}" ping >/dev/null 2>&1 && break; sleep 0.1; done

echo ">> building node client"
( cd "${ROOT}/clients/node" && npm run build >/dev/null 2>&1 )

# 1) Go acquires the WRITE lock and holds it ~2s.
GOOUT="$(mktemp)"
( cd "${ROOT}/clients/go" && go run ./cmd/interop -addr="${ADDR}" -mode=write -resource="${RES}" -hold=2s ) >"${GOOUT}" 2>&1 &
GOPID=$!
for _ in $(seq 1 100); do grep -q "RESULT FENCING" "${GOOUT}" && break; sleep 0.05; done
F1="$(awk '/RESULT FENCING/{print $3}' "${GOOUT}")"
[ -n "${F1}" ] || { echo "FAIL: Go writer did not acquire"; cat "${GOOUT}"; exit 1; }
echo ">> Go holds write lock (fencing=${F1})"

# 2) While Go holds, a Node writer must TIME OUT (cross-language mutual exclusion).
sleep 0.3
N1="$(cd "${ROOT}/clients/node" && node interop.mjs "${ADDR}" write "${RES}" 800)"
echo ">> node(contend)=${N1}"
echo "${N1}" | grep -q "RESULT TIMEOUT" || { echo "FAIL: Node writer should have timed out while Go held"; exit 1; }

# 3) After Go releases, Node acquires — fencing must be > Go's (shared counter).
wait "${GOPID}"
N2="$(cd "${ROOT}/clients/node" && node interop.mjs "${ADDR}" write "${RES}" 5000)"
echo ">> node(acquire)=${N2}"
F2="$(echo "${N2}" | awk '/RESULT FENCING/{print $3}')"
[ -n "${F2}" ] || { echo "FAIL: Node did not acquire after release"; exit 1; }
[ "${F2}" -gt "${F1}" ] || { echo "FAIL: fencing not shared/monotonic across languages (${F2} !> ${F1})"; exit 1; }

echo "PASS: Go↔Node interop — mutual exclusion held, fencing shared & monotonic (${F1} -> ${F2})"
