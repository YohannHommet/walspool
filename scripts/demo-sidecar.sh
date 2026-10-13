#!/usr/bin/env bash
#
# Terminal 1 — Lance le sidecar Walspool et génère un flux continu de télémétrie.
# Usage: ./scripts/demo-sidecar.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

PORT="${WALSPOOL_PORT:-8080}"
ADDR="http://127.0.0.1:${PORT}"
DATA_DIR=$(mktemp -d /tmp/walspool-demo-XXXXXX)

cleanup() {
    echo ""
    echo "🛑 Arrêt du sidecar et nettoyage..."
    if [ -n "${SIDECAR_PID:-}" ] && kill -0 "${SIDECAR_PID}" 2>/dev/null; then
        kill "${SIDECAR_PID}" 2>/dev/null || true
        wait "${SIDECAR_PID}" 2>/dev/null || true
    fi
    rm -rf "${DATA_DIR}"
    echo "✅ Nettoyage terminé."
}
trap cleanup EXIT INT TERM

# 1. Vérification / Compilation du binaire sidecar
if [ ! -f "bin/walspool-sidecar" ]; then
    echo "📦 Compilation de bin/walspool-sidecar..."
    mkdir -p bin
    CGO_ENABLED=0 go build -o bin/walspool-sidecar ./cmd/sidecar
fi

# 2. Démarrage du sidecar
echo "🚀 Démarrage de walspool-sidecar sur ${ADDR}..."
./bin/walspool-sidecar -addr ":${PORT}" -data-dir "${DATA_DIR}" &
SIDECAR_PID=$!

# Attente de disponibilité de l'API
READY=0
for i in {1..20}; do
    if curl -s --connect-timeout 0.5 --max-time 0.5 "${ADDR}/healthz" >/dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 0.1
done

if [ "${READY}" -ne 1 ]; then
    echo "❌ Échec du démarrage du sidecar."
    exit 1
fi

echo "================================================================="
echo "✅ Walspool Sidecar est ACTIF sur ${ADDR}"
echo "📁 Répertoire temporaire : ${DATA_DIR}"
echo "👉 DANS UN SECOND TERMINAL, LANCEZ :"
echo "   ./scripts/demo-cli.sh"
echo "================================================================="
echo "⚡ Injection de logs en direct en cours (Ctrl+C pour arrêter)..."
echo ""

SERVICES=("auth-svc" "payment-svc" "order-api" "database-svc" "inventory-svc")
LEVELS=("INFO" "INFO" "WARN" "ERROR" "DEBUG")
TOPICS=("telemetry" "orders" "audit" "payments")

COUNT=0
while true; do
    IDX=$((COUNT % 5))
    SERVICE="${SERVICES[$IDX]}"
    LEVEL="${LEVELS[$IDX]}"
    TOPIC="${TOPICS[$((COUNT % 4))]}"
    TRACE_ID="tr-demo-$((1000 + (COUNT % 15)))"

    case "${LEVEL}" in
        "INFO")
            MSG="Operation completed successfully"
            ;;
        "WARN")
            MSG="Downstream latency exceeded 120ms"
            ;;
        "ERROR")
            MSG="Connection timeout to upstream cluster"
            ;;
        "DEBUG")
            MSG="Cache lookup miss; reading persistent storage"
            ;;
    esac

    PAYLOAD=$(printf '{"event_id":%d,"service":"%s","level":"%s","msg":"%s","latency_ms":%d}' \
        "${COUNT}" "${SERVICE}" "${LEVEL}" "${MSG}" "$((RANDOM % 150 + 10))")

    JSON_REQ=$(printf '{"topic":"%s","service":"%s","level":"%s","trace_id":"%s","payload":%s}' \
        "${TOPIC}" "${SERVICE}" "${LEVEL}" "${TRACE_ID}" "${PAYLOAD}")

    curl -s -X POST "${ADDR}/v1/enqueue" \
        -H "Content-Type: application/json" \
        -d "${JSON_REQ}" >/dev/null || true

    COUNT=$((COUNT + 1))
    if [ $((COUNT % 5)) -eq 0 ]; then
        printf "\r📡 Événements injectés : %-6d (Dernier: [%s] %s)" "${COUNT}" "${LEVEL}" "${SERVICE}"
    fi

    # Pause aléatoire entre 100ms et 300ms pour simuler un trafic régulier
    sleep 0.2
done
