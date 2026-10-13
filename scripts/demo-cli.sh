#!/usr/bin/env bash
#
# Terminal 2 — Lance la CLI Walspool (monitor ou stream avec filtres).
# Usage:
#   ./scripts/demo-cli.sh                 # Menu interactif
#   ./scripts/demo-cli.sh monitor         # Lance directement le dashboard
#   ./scripts/demo-cli.sh stream [flags]  # Lance directement le stream
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

PORT="${WALSPOOL_PORT:-8080}"
ADDR="http://127.0.0.1:${PORT}"

# 1. Vérification / Compilation du binaire CLI
if [ ! -f "bin/walspool" ]; then
    echo "📦 Compilation de bin/walspool..."
    mkdir -p bin
    CGO_ENABLED=0 go build -o bin/walspool ./cmd/walspool
fi

# 2. Vérification que le sidecar tourne bien
if ! curl -s --connect-timeout 1 --max-time 1 "${ADDR}/healthz" >/dev/null 2>&1; then
    echo "⚠️  Le sidecar walspool n'est pas joignable sur ${ADDR}."
    echo ""
    echo "👉 Veuillez d'abord lancer le script dans le TERMINAL 1 :"
    echo "   ./scripts/demo-sidecar.sh"
    echo ""
    read -r -p "Voulez-vous quand même continuer ? [y/N] " confirm
    if [[ ! "${confirm:-}" =~ ^[yYoO]$ ]]; then
        exit 1
    fi
fi

export WALSPOOL_ADDR="${ADDR}"

# 3. Si des arguments sont passés, exécuter directement la CLI
if [ "$#" -gt 0 ]; then
    exec ./bin/walspool "$@"
fi

# 4. Sinon, afficher un menu interactif ergonomique
while true; do
    echo ""
    echo "================================================================="
    echo "🔍 Walspool CLI Demo Explorer (${ADDR})"
    echo "================================================================="
    echo "1) 📊 walspool monitor           - Tableau de bord live interactif"
    echo "2) 🌊 walspool stream            - Stream temps réel colorisé"
    echo "3) 🚨 stream --level ERROR       - Filtrer les erreurs uniquement"
    echo "4) 🏷️  stream --service payment   - Filtrer par microservice"
    echo "5) 🔎 stream --grep 'timeout'    - Filtrer par motif regex"
    echo "6) ⏱️  stream --limit 5           - Écouter 5 logs puis s'arrêter"
    echo "7) 🔄 stream --format json | jq  - Test de composition Unix (JSON)"
    echo "8) 🚀 Stress Test                - Injecter une charge massive (10 000+ ev/s)"
    echo "q) Quitter"
    echo "================================================================="
    read -r -p "Choisissez une option [1-8, q] : " choice

    case "${choice}" in
        1)
            echo "Lancement de walspool monitor (appuyez sur 'q' pour revenir au menu)..."
            ./bin/walspool monitor
            ;;
        2)
            echo "Lancement de walspool stream (Ctrl+C pour revenir au menu)..."
            ./bin/walspool stream || true
            ;;
        3)
            echo "Lancement de walspool stream --level ERROR (Ctrl+C pour revenir au menu)..."
            ./bin/walspool stream --level ERROR || true
            ;;
        4)
            echo "Lancement de walspool stream --service payment-svc (Ctrl+C pour revenir au menu)..."
            ./bin/walspool stream --service payment-svc || true
            ;;
        5)
            echo "Lancement de walspool stream --grep 'timeout' (Ctrl+C pour revenir au menu)..."
            ./bin/walspool stream --grep "timeout" || true
            ;;
        6)
            echo "Lancement de walspool stream --limit 5..."
            ./bin/walspool stream --limit 5 || true
            ;;
        7)
            if command -v jq >/dev/null 2>&1; then
                echo "Lancement de walspool stream --format json | jq (Ctrl+C pour arrêter)..."
                ./bin/walspool stream --format json | jq . || true
            else
                echo "Lancement de walspool stream --format json (Ctrl+C pour arrêter)..."
                ./bin/walspool stream --format json || true
            fi
            ;;
        8)
            if [ ! -f "bin/loadgen" ]; then
                echo "📦 Compilation de bin/loadgen..."
                mkdir -p bin
                CGO_ENABLED=0 go build -o bin/loadgen ./scripts/loadgen
            fi
            echo "🚀 Injection de charge massive pendant 10s vers ${ADDR}..."
            ./bin/loadgen -url "${ADDR}/v1/enqueue" -workers 16 -duration 10s
            ;;
        q|Q)
            echo "Au revoir !"
            exit 0
            ;;
        *)
            echo "Option invalide : ${choice}"
            ;;
    esac
done
