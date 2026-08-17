#!/usr/bin/env bash
# A/B the control plane's xDS fleet memory footprint between the current worktree
# and a base ref, using test/perf/xdsscale. Reps alternate A/B then B/A so machine
# drift affects both sides equally, and one snapshot of the harness is copied into
# both worktrees so each side runs byte-identical measurement code.
#
# Usage:
#   hack/perf/xds-scale-compare.sh [--base main] [--reps 3] [--backends 500] ...
#   hack/perf/xds-scale-compare.sh --control   # base vs base: the noise floor
set -euo pipefail

BASE_REF="main"
CANDIDATE_REF=""
CANDIDATE_TREE_IN=""
REPS=3
GATEWAYS=20
BACKENDS=500
ENDPOINTS=4
ZONES=3
STEPS=4
ROUTES_PER_GATEWAY=2
CHURN_ROUNDS=3
CHURN_BACKENDS=50
OUT=""
CONTROL=0
GOMAXPROCS_VAL="${GOMAXPROCS:-4}"
GOGC_VAL="${GOGC:-100}"
QUIET="5s"
TIMEOUT="15m"

usage() {
    sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
    cat <<'EOF'

Options:
  --base <ref>          baseline git ref (default: main)
  --candidate-ref <ref> measure this ref instead of the current worktree
  --candidate-tree <d>  measure this existing directory as-is (uncommitted work
                        included); the harness is copied into it
  --reps <n>            measurement reps per side (default: 3)
  --gateways <n>        Gateways, and connected xDS clients (default: 20)
  --backends <n>        Services, i.e. clusters per client (default: 500)
  --endpoints <n>       endpoints per backend (default: 4)
  --zones <n>           distinct node localities (default: 3; 1 = best case for
                        sharing endpoints across clients)
  --steps <n>           client counts to sample heap at (default: 4)
  --routes-per-gw <n>   HTTPRoutes per Gateway (default: 2)
  --churn-rounds <n>    endpoint churn rounds (default: 3)
  --churn-backends <n>  backends rewritten per churn round (default: 50)
  --quiet <dur>         idle period that counts as converged (default: 5s)
  --timeout <dur>       per-phase timeout (default: 15m)
  --out <dir>           output dir (default: _output/xdsperf)
  --control             run base vs base to measure the noise floor
  -h, --help            show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --base) BASE_REF="$2"; shift 2 ;;
        --candidate-ref) CANDIDATE_REF="$2"; shift 2 ;;
        --candidate-tree) CANDIDATE_TREE_IN="$2"; shift 2 ;;
        --reps) REPS="$2"; shift 2 ;;
        --gateways) GATEWAYS="$2"; shift 2 ;;
        --backends) BACKENDS="$2"; shift 2 ;;
        --endpoints) ENDPOINTS="$2"; shift 2 ;;
        --zones) ZONES="$2"; shift 2 ;;
        --steps) STEPS="$2"; shift 2 ;;
        --routes-per-gw) ROUTES_PER_GATEWAY="$2"; shift 2 ;;
        --churn-rounds) CHURN_ROUNDS="$2"; shift 2 ;;
        --churn-backends) CHURN_BACKENDS="$2"; shift 2 ;;
        --quiet) QUIET="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --control) CONTROL=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"
OUT="${OUT:-$ROOT/_output/xdsperf}"
mkdir -p "$OUT"
# Measured runs cd into a worktree, so a relative --out would resolve there.
OUT="$(cd "$OUT" && pwd)"
TREES="$ROOT/_output/xdsperf-trees"
mkdir -p "$TREES"

CANDIDATE_SHA="$(git rev-parse --short HEAD)"
CANDIDATE_NAME="$(git rev-parse --abbrev-ref HEAD)"
BASE_SHA="$(git rev-parse --short "$BASE_REF")"

if [[ -n "$(git status --porcelain -- ':!_output' 2>/dev/null)" ]]; then
    echo "note: working tree is dirty; the candidate side measures the working tree as-is" >&2
fi

# Snapshot the harness once, up front, and copy from the snapshot into every tree.
# Copying from $ROOT instead would let an edit made during the run land between the
# two copies, which silently puts the sides on different measurement code.
SNAPSHOT="$OUT/harness-snapshot"
rm -rf "$SNAPSHOT"
mkdir -p "$SNAPSHOT"
cp "$ROOT/test/perf/xdsscale/"*.go "$SNAPSHOT/"
HARNESS_SUM="$(cat "$SNAPSHOT"/*.go | shasum | awk '{print $1}')"
echo "==> harness snapshot: $SNAPSHOT (sha1 $HARNESS_SUM)"

install_harness() {
    local tree="$1"
    mkdir -p "$tree/test/perf/xdsscale"
    cp "$SNAPSHOT/"*.go "$tree/test/perf/xdsscale/"
}

verify_harness() {
    local tree="$1" when="$2"
    local sum
    sum="$(cat "$tree/test/perf/xdsscale/"*.go | shasum | awk '{print $1}')"
    if [[ "$sum" != "$HARNESS_SUM" ]]; then
        echo "harness mismatch in $tree $when ($sum != $HARNESS_SUM); results are not comparable" >&2
        exit 1
    fi
}

# Base runs in a detached worktree so the current checkout is untouched. Per-run
# private trees: a shared tree can be overwritten by another comparison's harness.
BASE_TREE="$TREES/base-$BASE_SHA-$$"
echo "==> creating base worktree at $BASE_TREE ($BASE_REF @ $BASE_SHA)"
git worktree add --detach "$BASE_TREE" "$BASE_REF" >/dev/null
install_harness "$BASE_TREE"

if [[ -n "$CANDIDATE_TREE_IN" ]]; then
    CANDIDATE_TREE="$(cd "$CANDIDATE_TREE_IN" && pwd)"
    CANDIDATE_LABEL="candidate"
    install_harness "$CANDIDATE_TREE"
    CANDIDATE_DESC="$(cd "$CANDIDATE_TREE" && git rev-parse --short HEAD) tree $CANDIDATE_TREE"
elif [[ "$CONTROL" == "1" || -n "$CANDIDATE_REF" ]]; then
    if [[ "$CONTROL" == "1" ]]; then
        CANDIDATE_REF="$BASE_REF"
        CANDIDATE_LABEL="control"
    else
        CANDIDATE_LABEL="candidate"
    fi
    CAND_SHA="$(git rev-parse --short "$CANDIDATE_REF")"
    CANDIDATE_TREE="$TREES/cand-$CANDIDATE_LABEL-$CAND_SHA-$$"
    echo "==> creating candidate worktree at $CANDIDATE_TREE ($CANDIDATE_REF @ $CAND_SHA)"
    git worktree add --detach "$CANDIDATE_TREE" "$CANDIDATE_REF" >/dev/null
    install_harness "$CANDIDATE_TREE"
    CANDIDATE_DESC="$CANDIDATE_REF @ $CAND_SHA"
    [[ "$CONTROL" == "1" ]] && CANDIDATE_DESC="$CANDIDATE_DESC (control)"
else
    CANDIDATE_TREE="$ROOT"
    CANDIDATE_LABEL="candidate"
    CANDIDATE_DESC="$CANDIDATE_NAME @ $CANDIDATE_SHA"
fi

echo "==> candidate: $CANDIDATE_DESC"
echo "==> base:      $BASE_REF @ $BASE_SHA"
echo "==> scale:     ${GATEWAYS} clients, ${BACKENDS} backends x ${ENDPOINTS} endpoints, ${ZONES} zones"
echo "==> churn:     ${CHURN_ROUNDS} rounds x ${CHURN_BACKENDS} backends"
echo "==> reps:      $REPS per side, alternating A/B order; GOMAXPROCS=$GOMAXPROCS_VAL GOGC=$GOGC_VAL"
echo "==> out:       $OUT"

for tree in "$CANDIDATE_TREE" "$BASE_TREE"; do
    verify_harness "$tree" "before the run"
done

# Warm both build caches first so compile time never lands inside a measured run.
for tree in "$CANDIDATE_TREE" "$BASE_TREE"; do
    (cd "$tree" && go test -tags e2e -count=1 -run XXX_NONE ./test/perf/xdsscale/ >/dev/null)
done

run_one() {
    local tree="$1" side="$2" rep="$3"
    local rep_out="$OUT/$side/rep$rep"
    mkdir -p "$rep_out"
    echo "--> $side rep $rep"
    # The previous rep tears its apiserver and etcd down asynchronously; starting the
    # next one on top of them costs startup time and sometimes the startup itself.
    sleep 5
    (
        cd "$tree"
        env \
            GOMAXPROCS="$GOMAXPROCS_VAL" \
            GOGC="$GOGC_VAL" \
            XDSPERF=1 \
            XDSPERF_LABEL="$side" \
            XDSPERF_GATEWAYS="$GATEWAYS" \
            XDSPERF_BACKENDS="$BACKENDS" \
            XDSPERF_ENDPOINTS_PER_BACKEND="$ENDPOINTS" \
            XDSPERF_ZONES="$ZONES" \
            XDSPERF_STEPS="$STEPS" \
            XDSPERF_ROUTES_PER_GATEWAY="$ROUTES_PER_GATEWAY" \
            XDSPERF_CHURN_ROUNDS="$CHURN_ROUNDS" \
            XDSPERF_CHURN_BACKENDS="$CHURN_BACKENDS" \
            XDSPERF_QUIET="$QUIET" \
            XDSPERF_TIMEOUT="$TIMEOUT" \
            XDSPERF_OUT="$rep_out" \
            XDSPERF_MEMPROFILERATE="${XDSPERF_MEMPROFILERATE:-0}" \
            go test -tags e2e -count=1 -timeout 90m -run TestXdsScaleFootprint ./test/perf/xdsscale/ \
            >"$rep_out/test.log" 2>&1
    ) || { echo "    FAILED - see $rep_out/test.log" >&2; tail -20 "$rep_out/test.log" >&2; exit 1; }
}

for rep in $(seq 1 "$REPS"); do
    # Alternate A/B then B/A so a consistent thermal or background-load drift cannot
    # systematically favor the side that always runs first.
    if (( rep % 2 == 1 )); then
        run_one "$CANDIDATE_TREE" "$CANDIDATE_LABEL" "$rep"
        run_one "$BASE_TREE" "base" "$rep"
    else
        run_one "$BASE_TREE" "base" "$rep"
        run_one "$CANDIDATE_TREE" "$CANDIDATE_LABEL" "$rep"
    fi
done

for tree in "$CANDIDATE_TREE" "$BASE_TREE"; do
    verify_harness "$tree" "after the run"
done

echo
python3 "$ROOT/hack/perf/xds_scale_report.py" \
    --out "$OUT" \
    --candidate "$CANDIDATE_LABEL" \
    --candidate-desc "$CANDIDATE_DESC" \
    --base-desc "$BASE_REF @ $BASE_SHA"
