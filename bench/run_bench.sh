#!/usr/bin/env bash
# run_bench.sh — tinyKV profiling & benchmarking pipeline
#
# Usage:
#   ./bench/run_bench.sh                        # all benchmarks, no profiles
#   ./bench/run_bench.sh BenchmarkPutSeq        # filter to one benchmark
#   PROFILE=1 ./bench/run_bench.sh              # benchmarks + all pprof profiles
#   PROFILE=1 PROFILE_TARGET=BenchmarkGetCold \ # profile a specific benchmark
#     ./bench/run_bench.sh
#
# Environment variables:
#   BENCH_TIME      benchtime per benchmark (default: 5s)
#   BENCH_CPU       comma-separated GOMAXPROCS list (default: current)
#   PROFILE         set to 1 to capture pprof profiles (default: 0)
#   PROFILE_TARGET  benchmark filter used for profiling (default: BenchmarkPutSeq)
#
# Results are written to bench/results/<timestamp>/.
# Profiles can be inspected with:
#   go tool pprof -http=:6060 <profile.pprof>

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${SCRIPT_DIR}/results/${TIMESTAMP}"
mkdir -p "${OUT_DIR}"

FILTER="${1:-.}"
BENCH_TIME="${BENCH_TIME:-5s}"
BENCH_CPU="${BENCH_CPU:-}"
PROFILE="${PROFILE:-0}"
PROFILE_TARGET="${PROFILE_TARGET:-BenchmarkPutSeq}"

CPU_FLAG=""
if [[ -n "${BENCH_CPU}" ]]; then
  CPU_FLAG="-cpu=${BENCH_CPU}"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  tinyKV benchmark pipeline"
echo "  filter         : ${FILTER}"
echo "  benchtime      : ${BENCH_TIME}"
echo "  cpu            : ${BENCH_CPU:-<default>}"
echo "  profile        : ${PROFILE}"
echo "  profile target : ${PROFILE_TARGET}"
echo "  output dir     : ${OUT_DIR}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

cd "${ROOT}"

# ── [1] Full benchmark suite ──────────────────────────────────────────────────
echo ""
echo "▶  [1/5] Running benchmark suite…"
# shellcheck disable=SC2086
go test \
  -run='^$' \
  -bench="${FILTER}" \
  -benchmem \
  -benchtime="${BENCH_TIME}" \
  ${CPU_FLAG} \
  ./bench/... \
  2>&1 | tee "${OUT_DIR}/bench.txt"

echo ""
echo "   ✓ Results saved → ${OUT_DIR}/bench.txt"

if [[ "${PROFILE}" != "1" ]]; then
  echo ""
  echo "━━ Done (profiling disabled — set PROFILE=1 to enable) ━━━━━━━━━━━━"
  echo ""
  if command -v benchstat &>/dev/null; then
    echo "Tip: compare two runs with benchstat:"
    echo "  benchstat <old>/bench.txt ${OUT_DIR}/bench.txt"
  fi
  exit 0
fi

# ── [2] CPU profile ───────────────────────────────────────────────────────────
echo ""
echo "▶  [2/5] Capturing CPU profile (${PROFILE_TARGET})…"
# shellcheck disable=SC2086
go test \
  -run='^$' \
  -bench="${PROFILE_TARGET}" \
  -benchtime="${BENCH_TIME}" \
  ${CPU_FLAG} \
  -cpuprofile="${OUT_DIR}/cpu.pprof" \
  ./bench/... >/dev/null
echo "   ✓ Saved → ${OUT_DIR}/cpu.pprof"

# ── [3] Memory profile ────────────────────────────────────────────────────────
echo ""
echo "▶  [3/5] Capturing memory profile (${PROFILE_TARGET})…"
# shellcheck disable=SC2086
go test \
  -run='^$' \
  -bench="${PROFILE_TARGET}" \
  -benchtime="${BENCH_TIME}" \
  ${CPU_FLAG} \
  -memprofile="${OUT_DIR}/mem.pprof" \
  -memprofilerate=1 \
  ./bench/... >/dev/null
echo "   ✓ Saved → ${OUT_DIR}/mem.pprof"

# ── [4] Mutex contention profile ──────────────────────────────────────────────
echo ""
echo "▶  [4/5] Capturing mutex contention profile (${PROFILE_TARGET})…"
# shellcheck disable=SC2086
go test \
  -run='^$' \
  -bench="${PROFILE_TARGET}" \
  -benchtime="${BENCH_TIME}" \
  ${CPU_FLAG} \
  -mutexprofile="${OUT_DIR}/mutex.pprof" \
  -mutexprofilefraction=1 \
  ./bench/... >/dev/null
echo "   ✓ Saved → ${OUT_DIR}/mutex.pprof"

# ── [5] Block (scheduler) profile ─────────────────────────────────────────────
echo ""
echo "▶  [5/5] Capturing block profile (${PROFILE_TARGET})…"
# shellcheck disable=SC2086
go test \
  -run='^$' \
  -bench="${PROFILE_TARGET}" \
  -benchtime="${BENCH_TIME}" \
  ${CPU_FLAG} \
  -blockprofile="${OUT_DIR}/block.pprof" \
  -blockprofilerate=1 \
  ./bench/... >/dev/null
echo "   ✓ Saved → ${OUT_DIR}/block.pprof"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  All profiles saved in: ${OUT_DIR}/"
echo ""
echo "  Inspect interactively:"
echo "    go tool pprof -http=:6060 ${OUT_DIR}/cpu.pprof"
echo "    go tool pprof -http=:6060 ${OUT_DIR}/mem.pprof"
echo "    go tool pprof -http=:6060 ${OUT_DIR}/mutex.pprof"
echo "    go tool pprof -http=:6060 ${OUT_DIR}/block.pprof"
echo ""
echo "  Quick top-N from the terminal:"
echo "    go tool pprof -top -nodecount=20 ${OUT_DIR}/cpu.pprof"
echo "    go tool pprof -top -nodecount=20 -alloc_space ${OUT_DIR}/mem.pprof"

if command -v benchstat &>/dev/null; then
  echo ""
  echo "  Compare two runs:"
  echo "    benchstat <old>/bench.txt ${OUT_DIR}/bench.txt"
else
  echo ""
  echo "  Tip: install benchstat for statistical result comparison:"
  echo "    go install golang.org/x/perf/cmd/benchstat@latest"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
