#!/usr/bin/env bash
#
# Walspool Industrial Benchmark & Non-Regression Harness
#
# Usage:
#   ./scripts/bench.sh run [regex]              # Run benchmarks with memory allocations
#   ./scripts/bench.sh record [baseline_name]   # Record 5-sample statistical baseline
#   ./scripts/bench.sh compare <baseline_file>  # Compare against baseline using benchstat
#   ./scripts/bench.sh profile <bench_name>     # Generate CPU & memory pprof profiles
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

ensure_benchstat() {
    if ! command -v benchstat &>/dev/null; then
        local gopath_bin
        gopath_bin="$(go env GOPATH)/bin"
        if [ -x "${gopath_bin}/benchstat" ]; then
            export PATH="${gopath_bin}:${PATH}"
        else
            echo "Installing benchstat (golang.org/x/perf/cmd/benchstat@latest)..."
            go install golang.org/x/perf/cmd/benchstat@latest
            export PATH="${gopath_bin}:${PATH}"
        fi
    fi
}

cmd="${1:-run}"

case "${cmd}" in
    run)
        regex="${2:-.}"
        echo "================================================================="
        echo "Running Walspool Benchmarks (filter: ${regex})"
        echo "================================================================="
        go test -run=^$ -bench="${regex}" -benchmem ./...
        ;;

    record)
        name="${2:-v1.0}"
        output_file="${ROOT_DIR}/bench_baseline_${name}.txt"
        echo "================================================================="
        echo "Recording 5-sample statistical baseline -> ${output_file}"
        echo "================================================================="
        # Run 5 samples for rigorous statistical significance (benchstat compatible)
        go test -run=^$ -bench=. -benchmem -count=5 ./... | tee "${output_file}"
        echo ""
        echo "Baseline saved to: ${output_file}"
        ;;

    compare)
        baseline="${2:-}"
        if [ -z "${baseline}" ] || [ ! -f "${baseline}" ]; then
            echo "Error: please provide a valid baseline file."
            echo "Usage: ./scripts/bench.sh compare <baseline_file>"
            exit 1
        fi

        ensure_benchstat

        current_file="$(mktemp /tmp/bench_current_XXXXXX.txt)"
        echo "================================================================="
        echo "Benchmarking current branch (5 samples)..."
        echo "================================================================="
        go test -run=^$ -bench=. -benchmem -count=5 ./... | tee "${current_file}"

        echo ""
        echo "================================================================="
        echo "Statistical Comparison (benchstat): Baseline vs Current"
        echo "================================================================="
        benchstat "${baseline}" "${current_file}"
        rm -f "${current_file}"
        ;;

    profile)
        bench_target="${2:-BenchmarkFileStorage_Append_Parallel}"
        echo "================================================================="
        echo "Profiling benchmark: ${bench_target}"
        echo "================================================================="
        profile_dir="${ROOT_DIR}/tmp/profiles"
        mkdir -p "${profile_dir}"

        cpu_prof="${profile_dir}/cpu.pprof"
        mem_prof="${profile_dir}/mem.pprof"

        go test -run=^$ -bench="^${bench_target}$" -benchmem \
            -cpuprofile="${cpu_prof}" \
            -memprofile="${mem_prof}" \
            ./...

        echo ""
        echo "Profiles generated successfully in ${profile_dir}:"
        echo "  - CPU Profile : ${cpu_prof}"
        echo "  - MEM Profile : ${mem_prof}"
        echo ""
        echo "To inspect flame graphs interactively in your browser:"
        echo "  go tool pprof -http=:8080 ${cpu_prof}"
        echo "  go tool pprof -http=:8081 ${mem_prof}"
        ;;

    *)
        echo "Usage: $0 {run|record|compare|profile}"
        exit 1
        ;;
esac
