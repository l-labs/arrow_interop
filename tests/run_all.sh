#!/bin/sh
# run_all.sh — the full arrow_interop suite, one command.
#
#   L_BIN=/path/to/l sh tests/run_all.sh
#
# Requires: build/arrow_io.so (cmake --build build) and uv (pyarrow steps).
# Suites: core (the original 18) + edges + stream + matrix (L side + pyarrow
# oracle) + adversarial (subprocess, crash-detecting) + leaks.
# L_STRESS=1 widens the matrix (40 -> 120 cases) and the leak loop.
set -u
cd "$(dirname "$0")/.."
: "${L_BIN:?set L_BIN to the l binary}"
UV=${UV:-uv}
fail=0

run_q() { # $1=name $2=script
  out=$("$L_BIN" "$2" </dev/null 2>&1); rc=$?
  line=$(printf '%s\n' "$out" | grep "Results:")
  printf '%-12s %s\n' "$1" "${line:-NO RESULT LINE}"
  if [ $rc -ne 0 ]; then echo "$1: CRASH rc=$rc"; fail=1; fi
  printf '%s\n' "$out" | grep -q " 0 failed" || {
    fail=1; printf '%s\n' "$out" | grep "FAIL"; }
}

echo "== fixtures =="
$UV run --with pyarrow tests/make_fixtures.py >/dev/null || fail=1
$UV run --with pyarrow tests/gen_edges.py >/dev/null || fail=1
$UV run --with pyarrow tests/gen_matrix.py gen || fail=1

echo "== q suites =="
run_q core   tests/test_arrow.q
run_q edges  tests/test_edges.q
run_q stream tests/test_stream.q
run_q matrix tests/test_matrix.q

echo "== pyarrow oracle =="
$UV run --with pyarrow tests/gen_matrix.py check || fail=1

echo "== adversarial (subprocess) =="
L_BIN="$L_BIN" $UV run --with pyarrow tests/test_adversarial.py || fail=1

echo "== leak loop =="
run_q leaks tests/test_leaks.q

if [ $fail -eq 0 ]; then echo "ALL SUITES PASSED"; else
  echo "SUITE FAILURES"; exit 1; fi
