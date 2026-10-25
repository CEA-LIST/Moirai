#!/usr/bin/env bash
# NW.19, P4 (vault spec/items/NW.19.md, sections 1 and 6): the container suite on an image built
# from this tree's clean HEAD under a test-owned tag. A load step: run it alone.
#
#   tests/nw19/containers.sh <out-dir>
#
# 1. Preconditions: on akira/moirai-modelstore, the tree clean; no cargo, rustc or Docker build
#    running; at least 30 GB free on /volatile (else exit 3, BLOCKED, nothing pruned); the
#    protected tags' image ids recorded; `docker volume ls` and `docker network ls` recorded.
# 2. The image NW19_IMAGE_TAG (default moirai-json-crdt-model-plane:nw19) built with the tree's
#    docker/e2e/Dockerfile from a context of git archives: HEAD as moirai-model-plane/ and Arachne
#    at NW19_ARACHNE_REV (default bd7b647) as arachne-model-plane/. NW19_IMAGE_BUILD=0 uses an
#    existing image of that tag instead (the spec-time rehearsal image) and never removes it.
# 3. The image's own defaults (I-6), each in a container of the run removed with `docker rm -f -v`:
#    IMG-ROUTER /usr/local/bin/moirai-router is in it; IMG-NN and IMG-MN network_node (the image's
#    command) and model_node with nothing transport-related set print `Zenoh transport in session`
#    within 30 s; IMG-NN-BOOT and IMG-MN-BOOT each with BOOTNODE_URL and the transport unset exits
#    non-zero within 30 s, its output naming MOIRAI_TRANSPORT=tcp, with no `Zenoh transport in
#    session` line.
# 4. e2e_convergence on both backends at once: MOIRAI_E2E_IMAGE the tag, MOIRAI_E2E_NODE_BIN
#    Arachne's network_node built against this tree, MOIRAI_E2E_BOOTNODE_BIN the tree's
#    moirai-bootnode, one test thread, --nocapture. P4-SUITE every scenario that runs passes and
#    the counts are those registered (43 passed, 0 failed, 5 ignored); P4-SKIP no E2E-SKIP line;
#    P4-ROUTES every E2E-ROUTES line of a container scenario on the default (S4, T5, C1 to C5, R1,
#    NW08 D1 to D3, MP20 to MP23, MP36) names at least one peer and every route `zenoh`.
#    E2E-CUT lines are recorded, not judged here (C5 asserts its own).
# 5. P4-CLEAN no container of the run left (none from the tag, none named nw19-img-*), no network
#    of the run left (moirai-e2e-<pid>-*, moirai-p3-<pid>-*), every anonymous volume the run
#    created removed by name and listed; P4-PROTECTED every protected tag's id as before. The tag
#    is removed at the end unless NW19_IMAGE_KEEP=1 (the stack, P5, runs on it next).
set -uo pipefail
export LC_ALL=C
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
AR="${NW19_ARACHNE:-$ROOT/../arachne-model-plane}"
AR_REV="${NW19_ARACHNE_REV:-bd7b647}"
SCR="${NW19_SCRATCH:-/volatile/home/vp242103/.cache/cea-crdt-scratch/NW.19}"
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
TAG="${NW19_IMAGE_TAG:-moirai-json-crdt-model-plane:nw19}"
OUT="${1:?out dir}"
PROTECTED="moirai-json-crdt-model-plane:test moirai-json-crdt-model-plane:si10 moirai-json-crdt-model-plane:guide moirai-json-crdt-detune-demo:dt01a moirai-json-crdt-detune:test"
CONTAINER_SCENARIOS="s4_real_partition_heals t5_a_severed_donor_does_not_strand_the_joiner c1_disjoint_keys_across_a_partition c2_same_key_concurrent_updates_converge c3_concurrent_inserts_at_the_same_position c4_update_wins_over_a_concurrent_remove c5_a_cut_held_past_the_lease_reads_disconnected_then_heals r1_seeded_random_operations_converge nw08_d1_a_held_last_operation_that_renders_nothing_is_not_a_lost_one nw08_d2_a_hold_past_the_deadline_is_reported nw08_d3_the_same_count_of_different_operations_is_reported mp20_two_replicas_host_four_models_and_converge_per_model mp21_a_node_never_hosts_a_model_nobody_registered_there mp22_a_joiner_receives_each_hosted_model_separately mp23_a_partitioned_run_with_four_models_heals_per_model mp36_a_malformed_operation_is_refused_at_one_replica_and_reaches_none"
[ -e "$OUT" ] && { echo "NW.19 CONTAINERS REFUSED: $OUT exists"; exit 2; }
for v in $(env | cut -d= -f1 | grep -E '^(ZENOH_|MOIRAI_TRANSPORT$|MOIRAI_E2E_|ROUTER_REGIONS$)'); do
    echo "NW.19 CONTAINERS REFUSED: $v is set"; exit 2
done
case "$TAG" in
    moirai-json-crdt-model-plane:nw19|moirai-json-crdt-model-plane:nw19-*) ;;
    *) echo "NW.19 CONTAINERS REFUSED: $TAG is not a test-owned tag of NW.19"; exit 2 ;;
esac
mkdir -p "$OUT" "$SCR/tmp"
export TMPDIR="$SCR/tmp" CARGO_BUILD_JOBS=2
FAILS=0
pass() { printf 'PASS %s\n' "$*" | tee -a "$OUT/verdicts.txt"; }
fail() { printf 'FAIL %s\n' "$*" | tee -a "$OUT/verdicts.txt"; FAILS=$((FAILS + 1)); }
note() { printf 'RECORD %s\n' "$*" | tee -a "$OUT/verdicts.txt"; }
busy() { pgrep -x cargo; pgrep -x rustc; pgrep -f 'docker( |-)build|buildkitd|docker-buildx' | grep -v "^$$\$"; }
protected_ids() { for t in $PROTECTED; do printf '%s %s\n' "$t" "$(docker image inspect -f '{{.Id}}' "$t" 2>/dev/null || echo absent)"; done; }

# ------------------------------------------------------------------ 1. preconditions
br="$(git -C "$ROOT" branch --show-current)"; dirty="$(git -C "$ROOT" status --porcelain)"
HEAD="$(git -C "$ROOT" rev-parse HEAD)"
if [ "$br" != akira/moirai-modelstore ] || [ -n "$dirty" ]; then echo "NW.19 CONTAINERS REFUSED: branch $br, changed: $dirty"; exit 2; fi
if [ -n "$(busy)" ]; then echo "NW.19 CONTAINERS REFUSED: not alone: $(busy | tr '\n' ' ')"; exit 2; fi
avail="$(df -BG --output=avail /volatile | tail -1 | tr -dc 0-9)"
if [ "${avail:-0}" -lt 30 ]; then
    { echo "NW.19 CONTAINERS BLOCKED: ${avail} GB free on /volatile, under 30"; df -BG /volatile; docker system df; } | tee "$OUT/blocked.txt"
    exit 3
fi
protected_ids > "$OUT/protected-before.txt"
docker volume ls -q | sort > "$OUT/volumes-before.txt"
docker network ls --format '{{.Name}}' | sort > "$OUT/networks-before.txt"
note "HEAD $HEAD, arachne $(git -C "$AR" rev-parse "$AR_REV"), tag $TAG, ${avail} GB free"

# ------------------------------------------------------------------ 2. the image
if [ "${NW19_IMAGE_BUILD:-1}" = 1 ]; then
    if docker image inspect "$TAG" >/dev/null 2>&1; then echo "NW.19 CONTAINERS REFUSED: $TAG exists already"; exit 2; fi
    CTX="$OUT/context"
    mkdir -p "$CTX/moirai-model-plane" "$CTX/arachne-model-plane"
    git -C "$ROOT" archive HEAD | tar -x -C "$CTX/moirai-model-plane"
    git -C "$AR" archive "$AR_REV" | tar -x -C "$CTX/arachne-model-plane"
    t0=$(date +%s)
    nice -n 10 docker build -f "$CTX/moirai-model-plane/docker/e2e/Dockerfile" -t "$TAG" \
        --build-arg MOIRAI_DIR=moirai-model-plane --build-arg ARACHNE_DIR=arachne-model-plane "$CTX" > "$OUT/image-build.log" 2>&1
    brc=$?
    rm -rf "$CTX"
    if [ "$brc" != 0 ]; then fail "P4-IMAGE-BUILD exit $brc: $(tail -5 "$OUT/image-build.log" | tr '\n' ' ')"; echo "NW.19 CONTAINERS: FAIL"; exit 1; fi
    note "image $TAG $(docker image inspect -f '{{.Id}}' "$TAG") built from HEAD $HEAD in $(( $(date +%s) - t0 )) s"
else
    docker image inspect "$TAG" >/dev/null 2>&1 || { echo "NW.19 CONTAINERS REFUSED: NW19_IMAGE_BUILD=0 and no $TAG"; exit 2; }
    note "image $TAG $(docker image inspect -f '{{.Id}}' "$TAG") used as found (NW19_IMAGE_BUILD=0)"
fi

# ------------------------------------------------------------------ 3. the image's own defaults
img_run() { # name, seconds, docker run args... -> waits, writes the log, removes the container
    local name=$1 secs=$2; shift 2
    docker rm -f -v "$name" >/dev/null 2>&1
    docker run -d --name "$name" "$@" > /dev/null 2> "$OUT/$name.err" || return 1
    local deadline=$(( $(date +%s) + secs ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        docker logs "$name" > "$OUT/$name.log" 2>&1
        grep -q 'Zenoh transport in session' "$OUT/$name.log" && break
        [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" = true ] || break
        sleep 1
    done
    docker logs "$name" > "$OUT/$name.log" 2>&1
    docker inspect -f '{{.State.Running}} {{.State.ExitCode}}' "$name" > "$OUT/$name.state" 2>/dev/null
    docker rm -f -v "$name" >/dev/null 2>&1
}
if docker run --rm --entrypoint /bin/sh "$TAG" -c 'test -x /usr/local/bin/moirai-router' 2>/dev/null; then pass "IMG-ROUTER /usr/local/bin/moirai-router in $TAG"
else fail "IMG-ROUTER no executable /usr/local/bin/moirai-router in $TAG"; fi
for which in NN MN; do
    case $which in NN) entry=(); bin=network_node ;; MN) entry=(--entrypoint model_node); bin=model_node ;; esac
    img_run "nw19-img-$which" 30 "${entry[@]}" -e REPLICA_ID="nw19-img-$which" "$TAG"
    if grep -q 'Zenoh transport in session' "$OUT/nw19-img-$which.log"; then pass "IMG-$which the image's $bin with nothing set opens a Zenoh session: $(grep -m1 'Zenoh transport in session' "$OUT/nw19-img-$which.log")"
    else fail "IMG-$which the image's $bin with nothing set opened no Zenoh session within 30 s: $(head -5 "$OUT/nw19-img-$which.log" | tr '\n' ' ')"; fi
    img_run "nw19-img-$which-boot" 30 "${entry[@]}" -e REPLICA_ID="nw19-img-$which-boot" -e BOOTNODE_URL=http://bootnode:7000 "$TAG"
    read -r running code < "$OUT/nw19-img-$which-boot.state" 2>/dev/null || { running=unknown; code=unknown; }
    if [ "$running" = false ] && [ "$code" != 0 ] && grep -q 'MOIRAI_TRANSPORT=tcp' "$OUT/nw19-img-$which-boot.log" \
        && ! grep -q 'Zenoh transport in session' "$OUT/nw19-img-$which-boot.log"; then
        pass "IMG-$which-BOOT the image's $bin with BOOTNODE_URL and the transport unset exits $code naming MOIRAI_TRANSPORT=tcp"
    else fail "IMG-$which-BOOT running $running, exit $code: $(head -5 "$OUT/nw19-img-$which-boot.log" | tr '\n' ' ' | head -c 400)"; fi
done

# ------------------------------------------------------------------ 4. the suite
C=(flock "$LOCK" nice -n 10 cargo)
mkdir -p "$OUT/ar/arachne-model-plane" "$OUT/bin"
git -C "$AR" archive "$AR_REV" | tar -x -C "$OUT/ar/arachne-model-plane"
ln -s "$ROOT" "$OUT/ar/moirai-model-plane"
(CARGO_TARGET_DIR="$SCR/target-arachne" "${C[@]}" build -j 2 --offline --locked --manifest-path "$OUT/ar/arachne-model-plane/generated/json_crdt/Cargo.toml" --example network_node) > "$OUT/build-network-node.log" 2>&1 \
    && cp "$SCR/target-arachne/debug/examples/network_node" "$OUT/bin/"
(cd "$ROOT" && "${C[@]}" build -j 2 --locked -p moirai-bootnode) > "$OUT/build-bootnode.log" 2>&1 && cp "$ROOT/target/debug/moirai-bootnode" "$OUT/bin/"
(cd "$ROOT" && "${C[@]}" test -j 2 --locked -p moirai-network --test e2e_convergence --no-run) > "$OUT/build-e2e.log" 2>&1
sha256sum "$OUT"/bin/* > "$OUT/bin/sha256.txt" 2>/dev/null
if [ -n "$(busy)" ]; then echo "NW.19 CONTAINERS REFUSED: not alone before the suite: $(busy | tr '\n' ' ')"; exit 2; fi
t0=$(date +%s)
(cd "$ROOT" && env MOIRAI_E2E_IMAGE="$TAG" MOIRAI_E2E_NODE_BIN="$OUT/bin/network_node" MOIRAI_E2E_BOOTNODE_BIN="$OUT/bin/moirai-bootnode" \
    "${C[@]}" test -j 2 --locked -p moirai-network --test e2e_convergence -- --test-threads=1 --nocapture) > "$OUT/suite.log" 2>&1
src=$?
note "suite exit $src in $(( $(date +%s) - t0 )) s"
read -r passed failed ignored < <(grep -E '^test result' "$OUT/suite.log" | awk '{p+=$4; f+=$6; i+=$8} END {print p+0, f+0, i+0}')
# With --nocapture a scenario's lines interleave with libtest's `test ... ok`, so the failing
# scenarios are read from libtest's own `failures:` list, and the counts from `test result`.
failing="$(awk '/^failures:$/ {f++; next} f >= 2 && /^    [a-z0-9_]+$/ {print $1}' "$OUT/suite.log" | sort -u | tr '\n' ' ')"
if [ "$src" = 0 ] && [ "$passed $failed $ignored" = "43 0 5" ]; then pass "P4-SUITE 43 passed, 0 failed, 5 ignored"
else fail "P4-SUITE exit $src; passed $passed, failed $failed, ignored $ignored (43 0 5 registered); failing: $failing"; fi
skips="$(grep -c 'E2E-SKIP' "$OUT/suite.log")"
if [ "$skips" = 0 ]; then pass "P4-SKIP no E2E-SKIP"; else fail "P4-SKIP $skips E2E-SKIP lines: $(grep -m3 'E2E-SKIP' "$OUT/suite.log" | tr '\n' ' ')"; fi
grep -E '^E2E-ROUTES ' "$OUT/suite.log" > "$OUT/routes.txt"
grep -E '^E2E-CUT ' "$OUT/suite.log" > "$OUT/cuts.txt"
python3 -I - "$OUT/routes.txt" $CONTAINER_SCENARIOS > "$OUT/routes-judged.txt" <<'PY'
import json, sys
path, scenarios = sys.argv[1], set(sys.argv[2:])
seen, bad = {}, []
for line in open(path):
    parts = line.rstrip("\n").split(" ", 3)
    if len(parts) < 4:
        continue
    _, scen, node, routes = parts
    if scen not in scenarios:
        continue
    seen.setdefault(scen, 0)
    seen[scen] += 1
    try:
        r = json.loads(routes)
    except ValueError:
        bad.append(f"{scen} {node}: unreadable routes {routes}")
        continue
    if not isinstance(r, dict) or not r or any(v != "zenoh" for v in r.values()):
        bad.append(f"{scen} {node}: {routes}")
missing = sorted(s for s in scenarios if s not in seen)
for b in bad:
    print("BAD " + b)
print(f"scenarios with route lines {len(seen)} of {len(scenarios)}; lines {sum(seen.values())}; missing {missing}")
sys.exit(1 if bad or missing else 0)
PY
if [ $? = 0 ]; then pass "P4-ROUTES every container scenario on the default reports every peer by zenoh ($(tail -1 "$OUT/routes-judged.txt"))"
else fail "P4-ROUTES $(head -8 "$OUT/routes-judged.txt" | tr '\n' ' ' | head -c 900)"; fi
note "cuts: $(tr '\n' ';' < "$OUT/cuts.txt")"

# ------------------------------------------------------------------ 5. clean-up and protected tags
left_c="$(docker ps -a --filter "ancestor=$TAG" --format '{{.Names}}'; docker ps -a --format '{{.Names}}' | grep '^nw19-img-')"
docker network ls --format '{{.Name}}' | sort > "$OUT/networks-after.txt"
left_n="$(comm -13 "$OUT/networks-before.txt" "$OUT/networks-after.txt" | grep -E '^moirai-(e2e|p3)-' || true)"
docker volume ls -q | sort > "$OUT/volumes-after.txt"
new_v="$(comm -13 "$OUT/volumes-before.txt" "$OUT/volumes-after.txt")"
for v in $new_v; do
    if [[ "$v" =~ ^[0-9a-f]{64}$ ]]; then docker volume rm "$v" >/dev/null 2>&1 && note "removed anonymous volume $v the run created"; fi
done
if [ -z "$left_c" ] && [ -z "$left_n" ]; then pass "P4-CLEAN no container or network of the run left; volumes created: $(printf '%s' "$new_v" | wc -w)"
else fail "P4-CLEAN containers left: $(printf '%s' "$left_c" | tr '\n' ' '); networks left: $(printf '%s' "$left_n" | tr '\n' ' ')"; fi
if [ "${NW19_IMAGE_BUILD:-1}" = 1 ] && [ "${NW19_IMAGE_KEEP:-0}" != 1 ]; then
    docker image rm "$TAG" > "$OUT/image-rm.log" 2>&1 && note "image $TAG removed"
fi
protected_ids > "$OUT/protected-after.txt"
if cmp -s "$OUT/protected-before.txt" "$OUT/protected-after.txt"; then pass "P4-PROTECTED every protected tag as before ($(awk '{print $1"="substr($2,8,12)}' "$OUT/protected-after.txt" | tr '\n' ' '))"
else fail "P4-PROTECTED $(diff "$OUT/protected-before.txt" "$OUT/protected-after.txt" | tr '\n' ' ')"; fi

if [ "$FAILS" -eq 0 ]; then echo "NW.19 CONTAINERS: PASS" | tee -a "$OUT/verdicts.txt"; exit 0; fi
echo "NW.19 CONTAINERS: FAIL ($FAILS failing checks)" | tee -a "$OUT/verdicts.txt"
exit 1
