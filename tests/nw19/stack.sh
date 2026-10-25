#!/usr/bin/env bash
# NW.19, P5 (vault spec/items/NW.19.md, sections 1, 2 and 7): the stack under NW.19's own names and
# ports, on a test-owned image, through docker/compose/stack_interpreter.sh. A load step: run it
# alone (no cargo, rustc or Docker build anywhere; the editor's three e2e files run inside it).
#
#   tests/nw19/stack.sh <out-dir> [Z|T|ZT]          (default ZT)
#
# Names: infrastructure project moirai-infra-nw19, session network moirai_stack_nw19, router
# nw19-router, bootnode nw19-bootnode, relay nw19-relay, replicas nw19-alice, nw19-bob, nw19-carol
# and nw19-dave (each its own project, moirai-replica-<name>), SESSION_ID nw19; host ports picked
# free at run time, never 7000, 7447, 8081, 8082 or 8090. Image NW19_IMAGE_TAG (default
# moirai-json-crdt-model-plane:nw19), never built or removed here.
#
# Z, the default transport (the stack script with no transport named):
#   Z1  the dry run (I-4): `config infra` shows one router named nw19-router on the session network
#       with the alias router, LISTEN_PORT 7447, no published port, no bootnode or relay; `config up`
#       a replica told MOIRAI_TRANSPORT=zenoh, ZENOH_ROUTER=tcp/router:7447, no BOOTNODE_URL; `--tcp
#       config infra` a bootnode and a relay and no router; `--tcp config up` a replica told
#       MOIRAI_TRANSPORT=tcp with the bootnode; MOIRAI_TRANSPORT=tcp in the script's environment
#       prints what --tcp prints; nothing created, started or changed (containers, networks and
#       volumes as before).
#   Z2  `infra up`: in the infrastructure project one container, nw19-router, running moirai-router;
#       no bootnode or relay; no published port; in its namespace moirai-router holds a LISTEN
#       socket on 7447 and no UDP socket at all (multicast scouting off; NW.16b R6's check); on the
#       session network with the alias router.
#   Z3  `up` nw19-alice, nw19-bob, nw19-carol: each container's environment states
#       MOIRAI_TRANSPORT=zenoh and ZENOH_ROUTER=tcp/router:7447 and no BOOTNODE_URL; each lists the
#       other two Connected within 30 s.
#   Z4  the data path: in each replica's namespace its model_node holds an established TCP
#       connection with each of the other two replicas' addresses where one end is port 9001, and
#       one with the router's address on 7447 (Docker's DNS, 127.0.0.11, left out); within 30 s.
#   Z5  a model registered on nw19-alice (the SimpleUML descriptor posted there), joined by id on
#       the other two, five classes written on each, all three serving the same document within 30 s.
#   Z6  the router stopped (`docker stop`): ten more classes on each, converged within 30 s; the
#       router started again (`docker start`), healthy (a new process listening on 7447 within
#       30 s); nw19-dave `up`, its environment as in Z3, the four list each other Connected within
#       30 s, dave joins the model and serves the same document within 30 s.
#   Z7  `down nw19-dave` (dave holds the model and originated no operation in it): the other three
#       read dave Disconnected within 15 s; `up nw19-dave` again, the four Connected within 30 s,
#       dave joins and serves the same document within 30 s. Z7B, recorded, never counted:
#       nw19-carol, a writer, down and up under its old name and joining again (J3b's known gap).
#   Z8  the editor at NW19_EDITOR_REV (default ae645e7, a git archive of repos/model-editor with its
#       node_modules copied): SysML's descriptor (Arachne's examples/sysml/sysml.metamodel.json at
#       NW19_ARACHNE_REV) posted to nw19-alice, then e2e/addMetamodel.e2e.ts, e2e/ipEditor.e2e.ts and
#       e2e/sysmlEditor.e2e.ts against nw19-alice and nw19-bob (MOIRAI_LIVE_NODES; the replicas allow
#       the origin http://127.0.0.1:*): three files passed, no failed test, no E2E-SKIP.
#   Z9  `down-all`: no container of the run left, the session network gone.
# T, the TCP mode (`stack_interpreter.sh --tcp ...`; NW19_TCP_SELECT=none names no transport, for
#    the baseline at the TCP-default parent):
#   T1  `infra up`: nw19-bootnode and nw19-relay running, no nw19-router.
#   T2  `up` nw19-alice, nw19-bob, nw19-carol; each lists the other two Connected within 30 s
#       (through the bootnode). T2-ENV each container's environment states MOIRAI_TRANSPORT=tcp.
#   T3  a model registered on nw19-alice, joined on the others, five classes on each, converged
#       within 30 s. (Z8's editor files here too when NW19_EDITOR_T=1, the baseline's.) T5,
#       recorded, never counted: Z7B on TCP.
#   T4  `down-all`: nothing of the run left.
# Then CLEAN (no container, network or anonymous volume of the run left; volumes it created removed
# by name) and PROTECTED (every protected tag's image id as before).
#
# NW19_LAYOUT=hand (the spec-time rehearsal only, Z only): the same names, image and checks, the
# containers started with `docker run` as the registered interface would start them, no stack
# script; Z1 is not run there.
set -uo pipefail
export LC_ALL=C
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
AR="${NW19_ARACHNE:-$ROOT/../arachne-model-plane}"
AR_REV="${NW19_ARACHNE_REV:-bd7b647}"
ED_REPO="${NW19_EDITOR_REPO:-$ROOT/../model-editor}"
ED_REV="${NW19_EDITOR_REV:-ae645e7}"
SCR="${NW19_SCRATCH:-/volatile/home/vp242103/.cache/cea-crdt-scratch/NW.19}"
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
IMAGE="${NW19_IMAGE_TAG:-moirai-json-crdt-model-plane:nw19}"
LAYOUT="${NW19_LAYOUT:-script}"
OUT="${1:?out dir}"
RUNS="${2:-ZT}"
NET=moirai_stack_nw19; INFRA=moirai-infra-nw19
ROUTER=nw19-router; BOOT=nw19-bootnode; RELAY=nw19-relay
SESSION=nw19; LOGID=19191919191919191919191919191919; ORIGINS='http://127.0.0.1:*'
PROTECTED="moirai-json-crdt-model-plane:test moirai-json-crdt-model-plane:si10 moirai-json-crdt-model-plane:guide moirai-json-crdt-detune-demo:dt01a moirai-json-crdt-detune:test"
ALL_NAMES="nw19-alice nw19-bob nw19-carol nw19-dave $ROUTER $BOOT $RELAY"
[ -e "$OUT" ] && { echo "NW.19 STACK REFUSED: $OUT exists"; exit 2; }
for v in $(env | cut -d= -f1 | grep -E '^(ZENOH_|MOIRAI_TRANSPORT$|MOIRAI_E2E_|MOIRAI_IMAGE$|MOIRAI_STACK_NETWORK$|MOIRAI_INFRA_PROJECT$|ROUTER_REGIONS$)'); do
    echo "NW.19 STACK REFUSED: $v is set"; exit 2
done
case "$IMAGE" in moirai-json-crdt-model-plane:nw19|moirai-json-crdt-model-plane:nw19-*) ;; *) echo "NW.19 STACK REFUSED: $IMAGE is not NW.19's"; exit 2 ;; esac
TCPSEL=(--tcp); [ "${NW19_TCP_SELECT:-flag}" = none ] && TCPSEL=()
mkdir -p "$OUT" "$SCR/tmp"
export TMPDIR="$SCR/tmp"
FAILS=0
pass() { printf 'PASS %s\n' "$*" | tee -a "$OUT/verdicts.txt"; }
fail() { printf 'FAIL %s\n' "$*" | tee -a "$OUT/verdicts.txt"; FAILS=$((FAILS + 1)); }
note() { printf 'RECORD %s\n' "$*" | tee -a "$OUT/verdicts.txt"; }
log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" >> "$OUT/driver.txt"; }
busy() { pgrep -x cargo; pgrep -x rustc; pgrep -f 'docker( |-)build|buildkitd|docker-buildx' | grep -v "^$$\$"; }
protected_ids() { for t in $PROTECTED; do printf '%s %s\n' "$t" "$(docker image inspect -f '{{.Id}}' "$t" 2>/dev/null || echo absent)"; done; }

# ------------------------------------------------------------------ preflight
docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "NW.19 STACK REFUSED: no image $IMAGE"; exit 2; }
for n in $ALL_NAMES; do docker container inspect "$n" >/dev/null 2>&1 && { echo "NW.19 STACK REFUSED: a container $n exists"; exit 2; }; done
docker network inspect "$NET" >/dev/null 2>&1 && { echo "NW.19 STACK REFUSED: network $NET exists"; exit 2; }
avail="$(df -BG --output=avail /volatile | tail -1 | tr -dc 0-9)"
if [ "${avail:-0}" -lt 30 ]; then { echo "NW.19 STACK BLOCKED: ${avail} GB free"; df -BG /volatile; docker system df; } | tee "$OUT/blocked.txt"; exit 3; fi
# The step program and the editor copy are built before anything starts.
STEP_BIN="$( (cd "$ROOT" && CARGO_BUILD_JOBS=2 flock "$LOCK" nice -n 10 cargo test -j 2 --locked -p moirai-interp --features network \
    --test nw19_stack_l4 --no-run --message-format=json 2>"$OUT/build-step.log") | python3 -I -c 'import json,sys
last=""
for l in sys.stdin:
    try: m=json.loads(l)
    except Exception: continue
    if m.get("reason")=="compiler-artifact" and m.get("executable"): last=m["executable"]
print(last)')"
[ -x "$STEP_BIN" ] || { echo "NW.19 STACK REFUSED: the step program did not build: $(tail -3 "$OUT/build-step.log")"; exit 2; }
cp "$STEP_BIN" "$OUT/nw19_stack_l4"; STEP_BIN="$OUT/nw19_stack_l4"
ED="$SCR/editor-$ED_REV"
if [ "${NW19_EDITOR:-1}" = 1 ] && [ ! -d "$ED/node_modules" ]; then
    rm -rf "$ED"; mkdir -p "$ED"
    git -C "$ED_REPO" archive "$ED_REV" | tar -x -C "$ED"
    cp -a "$ED_REPO/node_modules" "$ED/node_modules"
fi
git -C "$AR" show "$AR_REV:examples/sysml/sysml.metamodel.json" > "$OUT/sysml.metamodel.json"
if [ -n "$(busy)" ]; then echo "NW.19 STACK REFUSED: not alone: $(busy | tr '\n' ' ')"; exit 2; fi
free_ports() { python3 -I - "$1" <<'PY'
import socket, sys
out, socks = [], []
avoid = {7000, 7447, 8081, 8082, 8090}
while len(out) < int(sys.argv[1]):
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; socks.append(s)
    if p not in avoid and p not in out:
        out.append(p)
print(" ".join(map(str, out)))
PY
}
read -r P_ALICE P_BOB P_CAROL P_DAVE P_BOOT < <(free_ports 5)
declare -A PORT=([nw19-alice]=$P_ALICE [nw19-bob]=$P_BOB [nw19-carol]=$P_CAROL [nw19-dave]=$P_DAVE)
protected_ids > "$OUT/protected-before.txt"
docker volume ls -q | sort > "$OUT/volumes-before.txt"
docker network ls --format '{{.Name}}' | sort > "$OUT/networks-before.txt"
note "image $IMAGE $(docker image inspect -f '{{.Id}}' "$IMAGE"), layout $LAYOUT, runs $RUNS, ports alice $P_ALICE bob $P_BOB carol $P_CAROL dave $P_DAVE bootnode $P_BOOT, tree $(git -C "$ROOT" rev-parse HEAD)"

cleanup() {
    for n in $ALL_NAMES; do docker rm -f -v "$n" >/dev/null 2>&1; done
    docker network rm "$NET" >/dev/null 2>&1
}
trap cleanup EXIT

# ------------------------------------------------------------------ the lifecycle
SI() { # SI_TRANSPORT=tcp before a call puts MOIRAI_TRANSPORT=tcp in the script's environment
    env ${SI_TRANSPORT:+MOIRAI_TRANSPORT="$SI_TRANSPORT"} MOIRAI_IMAGE="$IMAGE" MOIRAI_STACK_NETWORK="$NET" MOIRAI_INFRA_PROJECT="$INFRA" \
        MOIRAI_ROUTER_NAME="$ROUTER" MOIRAI_BOOTNODE_NAME="$BOOT" MOIRAI_RELAY_NAME="$RELAY" \
        BOOTNODE_HTTP_PORT="$P_BOOT" SESSION_ID="$SESSION" LOG_ID="$LOGID" DASHBOARD_URL= \
        HTTP_ALLOWED_ORIGINS="$ORIGINS" bash "$ROOT/docker/compose/stack_interpreter.sh" "$@"
}
infra_up() { # mode (z|t)
    log "infra up $1"
    if [ "$LAYOUT" = hand ]; then
        docker network create "$NET" >/dev/null && docker run -d --name "$ROUTER" --network "$NET" --network-alias router \
            -e LISTEN_PORT=7447 "$IMAGE" moirai-router >/dev/null
    elif [ "$1" = t ]; then SI "${TCPSEL[@]}" infra up >> "$OUT/stack-script.log" 2>&1
    else SI infra up >> "$OUT/stack-script.log" 2>&1; fi
}
replica_up() { # mode name
    local name=$2 port=${PORT[$2]}
    log "up $1 $name $port"
    if [ "$LAYOUT" = hand ]; then
        docker run -d --name "$name" --hostname "$name" --network "$NET" --network-alias "$name" -p "$port:8081" \
            -e REPLICA_ID="$name" -e LISTEN_PORT=9001 -e HTTP_PORT=8081 -e MOIRAI_TRANSPORT=zenoh \
            -e ZENOH_ROUTER=tcp/router:7447 -e SESSION_ID="$SESSION" -e LOG_ID="$LOGID" -e METAMODEL_DIR=/metamodels \
            -e HTTP_ALLOWED_ORIGINS="$ORIGINS" -e PEERS= -e DASHBOARD_URL= -e ADVERTISE_ADDR="$name:9001" \
            --entrypoint model_node "$IMAGE" >/dev/null || return 1
        local deadline=$(( $(date +%s) + 60 ))
        until curl -fsS --max-time 2 "http://127.0.0.1:$port/api/health" >/dev/null 2>&1; do
            [ "$(date +%s)" -lt "$deadline" ] || return 1; sleep 1
        done
    elif [ "$1" = t ]; then SI "${TCPSEL[@]}" up "$name" --port "$port" >> "$OUT/stack-script.log" 2>&1
    else SI up "$name" --port "$port" >> "$OUT/stack-script.log" 2>&1; fi
}
replica_down() { # mode name
    log "down $1 $2"
    if [ "$LAYOUT" = hand ]; then docker rm -f -v "$2" >/dev/null 2>&1
    elif [ "$1" = t ]; then SI "${TCPSEL[@]}" down "$2" >> "$OUT/stack-script.log" 2>&1
    else SI down "$2" >> "$OUT/stack-script.log" 2>&1; fi
}
down_all() { # mode
    log "down-all $1"
    if [ "$LAYOUT" = hand ]; then cleanup; return 0
    elif [ "$1" = t ]; then SI "${TCPSEL[@]}" down-all >> "$OUT/stack-script.log" 2>&1
    else SI down-all >> "$OUT/stack-script.log" 2>&1; fi
}
nodes_of() { local out="" n; for n in "$@"; do out="$out,$n=http://127.0.0.1:${PORT[$n]}"; done; printf '%s' "${out#,}"; }
prog() { # label step arg tag timeout names...   (NW19_RECORD_ONLY=1: a RECORD line, never a failure)
    local label=$1 step=$2 arg=$3 tag=$4 timeout=$5; shift 5
    local nodes; nodes="$(nodes_of "$@")"
    local rc
    NW19_STEP="$step" NW19_NODES="$nodes" NW19_STATE="$OUT/state.json" NW19_ARG="$arg" NW19_TAG="$tag" \
        NW19_TIMEOUT_S="$timeout" "$STEP_BIN" --ignored --exact step --nocapture > "$OUT/prog-$label.log" 2>&1
    rc=$?
    local line; line="$(grep -m1 -E 'NW19-STACK .* (PASS|FAIL)' "$OUT/prog-$label.log" | head -c 700)"
    if [ "${NW19_RECORD_ONLY:-0}" = 1 ]; then note "$label (recorded, not counted) exit $rc: ${line#NW19-STACK }"; return $rc; fi
    if [ $rc = 0 ] && grep -q 'NW19-STACK .* PASS' "$OUT/prog-$label.log"; then pass "$label ${line#NW19-STACK }"
    else fail "$label exit $rc: ${line:-$(tail -3 "$OUT/prog-$label.log" | tr '\n' ' ' | head -c 500)}"; return 1; fi
}
# Sockets of the process `bin` in container `c`: its socket inodes, then the IPv4 and IPv6 tables.
ns_dump() { docker exec "$1" sh -c '
for p in /proc/[0-9]*; do [ "$(cat $p/comm 2>/dev/null)" = "'"$2"'" ] || continue
  echo "PID ${p#/proc/}"; for f in $p/fd/*; do readlink $f; done; done
echo "== tcp"; cat /proc/net/tcp; echo "== tcp6"; cat /proc/net/tcp6 2>/dev/null
echo "== udp"; cat /proc/net/udp; echo "== udp6"; cat /proc/net/udp6 2>/dev/null' 2>&1; }
ns_parse() { # file mode [args]: listens PORT | udp | links (prints remote ip:port local port lines)
    python3 -I - "$@" <<'PY'
import socket, struct, sys
path, mode = sys.argv[1], sys.argv[2]
inodes, tables, cur, pids = set(), {}, None, []
for line in open(path, errors="replace"):
    line = line.rstrip("\n")
    if line.startswith("PID "):
        pids.append(line[4:]); continue
    if line.startswith("socket:["):
        inodes.add(line[8:-1]); continue
    if line.startswith("== "):
        cur = line[3:]; tables[cur] = []; continue
    if cur and line.strip() and not line.strip().startswith("sl"):
        tables[cur].append(line.split())
def addr(hexaddr):
    ip, port = hexaddr.split(":")
    if len(ip) == 8:
        return socket.inet_ntoa(struct.pack("<I", int(ip, 16))), int(port, 16)
    return "v6:" + ip, int(port, 16)
own = lambda f: len(f) > 9 and f[9] in inodes
if not pids:
    print("NOPROC"); sys.exit(2)
if mode == "listens":
    want = int(sys.argv[3])
    ok = any(own(f) and f[3] == "0A" and addr(f[1])[1] == want for t in ("tcp", "tcp6") for f in tables.get(t, []))
    print(f"pids {pids} listen {want}: {ok}"); sys.exit(0 if ok else 1)
if mode == "udp":
    mine = [f[1] for t in ("udp", "udp6") for f in tables.get(t, []) if own(f)]
    print(f"pids {pids} udp sockets: {mine}"); sys.exit(1 if mine else 0)
if mode == "links":
    for t in ("tcp", "tcp6"):
        for f in tables.get(t, []):
            if own(f) and f[3] == "01":
                (lip, lport), (rip, rport) = addr(f[1]), addr(f[2])
                if rip.startswith("127.") or lip.startswith("127."):
                    continue
                print(f"{lip}:{lport} {rip}:{rport}")
PY
}
ip_on() { docker inspect -f "{{with index .NetworkSettings.Networks \"$NET\"}}{{.IPAddress}}{{end}}" "$1" 2>/dev/null; }
env_of() { docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$1" 2>/dev/null; }
env_says() { # container mode -> 0 when its environment states the mode's variables
    local e; e="$(env_of "$1")"
    case "$2" in
        zenoh) grep -qx 'MOIRAI_TRANSPORT=zenoh' <<< "$e" && grep -qx 'ZENOH_ROUTER=tcp/router:7447' <<< "$e" \
                && ! grep -qE '^BOOTNODE_URL=.+' <<< "$e" ;;
        tcp) grep -qx 'MOIRAI_TRANSPORT=tcp' <<< "$e" ;;
    esac
}
router_ready() { # seconds -> 0 when moirai-router listens on 7447 in its namespace
    local deadline=$(( $(date +%s) + $1 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        ns_dump "$ROUTER" moirai-router > "$OUT/router-ns.txt"
        ns_parse "$OUT/router-ns.txt" listens 7447 >/dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}
editor() { # label alice bob
    local label=$1 a=$2 b=$3
    local code; code="$(curl -s -o "$OUT/$label-sysml-post.txt" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data-binary @"$OUT/sysml.metamodel.json" "http://127.0.0.1:${PORT[$a]}/api/metamodels")"
    note "$label SysML descriptor posted to $a: HTTP $code"
    (cd "$ED" && env MOIRAI_LIVE_NODES="http://127.0.0.1:${PORT[$a]},http://127.0.0.1:${PORT[$b]}" CHROME_BIN=/usr/bin/google-chrome \
        timeout 1800 node_modules/.bin/vitest run --config vitest.e2e.config.ts e2e/addMetamodel.e2e.ts e2e/ipEditor.e2e.ts e2e/sysmlEditor.e2e.ts) \
        > "$OUT/$label-editor.log" 2>&1
    local rc=$? skips files tests
    skips="$(grep -c 'E2E-SKIP' "$OUT/$label-editor.log")"
    files="$(grep -E '^ *Test Files' "$OUT/$label-editor.log" | tail -1 | sed 's/\x1b\[[0-9;]*m//g' | tr -s ' ')"
    tests="$(grep -E '^ *Tests ' "$OUT/$label-editor.log" | tail -1 | sed 's/\x1b\[[0-9;]*m//g' | tr -s ' ')"
    if [ $rc = 0 ] && [ "$skips" = 0 ] && grep -qE 'Test Files +3 passed \(3\)' <<< "$files" && ! grep -q failed <<< "$tests"; then
        pass "$label the editor's three stack files against $a and $b:$files;$tests"
    else fail "$label exit $rc, E2E-SKIP $skips:$files;$tests $(grep -m3 -E 'FAIL|Error|E2E-SKIP' "$OUT/$label-editor.log" | tr '\n' ' ' | head -c 500)"; fi
}

# ------------------------------------------------------------------ Z
run_z() {
    local r
    : > "$OUT/state.json"
    if [ "$LAYOUT" = script ]; then
        docker ps -a --format '{{.Names}}' | sort > "$OUT/z1-containers-before.txt"
        local d="$OUT/z1"; mkdir -p "$d"
        SI config infra > "$d/infra.json" 2> "$d/infra.err"
        python3 -I "$HERE/compose_check.py" infra "$d/infra.json" zenoh "$ROUTER" "$NET" > "$d/c1.txt" 2>&1 && pass "Z1-INFRA $(cat "$d/c1.txt")" || fail "Z1-INFRA $(cat "$d/c1.txt") $(head -c 300 "$d/infra.err")"
        SI config up nw19-alice --port "$P_ALICE" > "$d/up.json" 2> "$d/up.err"
        python3 -I "$HERE/compose_check.py" replica "$d/up.json" zenoh nw19-alice > "$d/c2.txt" 2>&1 && pass "Z1-REPLICA $(cat "$d/c2.txt")" || fail "Z1-REPLICA $(cat "$d/c2.txt") $(head -c 300 "$d/up.err")"
        SI --tcp config infra > "$d/infra-tcp.json" 2> "$d/infra-tcp.err"
        python3 -I "$HERE/compose_check.py" infra "$d/infra-tcp.json" tcp > "$d/c3.txt" 2>&1 && pass "Z1-INFRA-TCP $(cat "$d/c3.txt")" || fail "Z1-INFRA-TCP $(cat "$d/c3.txt") $(head -c 300 "$d/infra-tcp.err")"
        SI --tcp config up nw19-alice --port "$P_ALICE" > "$d/up-tcp.json" 2> "$d/up-tcp.err"
        python3 -I "$HERE/compose_check.py" replica "$d/up-tcp.json" tcp nw19-alice > "$d/c4.txt" 2>&1 && pass "Z1-REPLICA-TCP $(cat "$d/c4.txt")" || fail "Z1-REPLICA-TCP $(cat "$d/c4.txt") $(head -c 300 "$d/up-tcp.err")"
        SI_TRANSPORT=tcp SI config infra > "$d/infra-env.json" 2>/dev/null
        SI_TRANSPORT=tcp SI config up nw19-alice --port "$P_ALICE" > "$d/up-env.json" 2>/dev/null
        if python3 -I -c 'import json,sys; a=[json.load(open(p)) for p in sys.argv[1:]]; sys.exit(0 if a[0]==a[1] and a[2]==a[3] else 1)' \
            "$d/infra-tcp.json" "$d/infra-env.json" "$d/up-tcp.json" "$d/up-env.json" 2>/dev/null; then pass "Z1-TCP-ENV MOIRAI_TRANSPORT=tcp in the script's environment prints what --tcp prints"
        else fail "Z1-TCP-ENV MOIRAI_TRANSPORT=tcp and --tcp print different configurations (or none)"; fi
        docker ps -a --format '{{.Names}}' | sort > "$OUT/z1-containers-after.txt"
        if cmp -s "$OUT/z1-containers-before.txt" "$OUT/z1-containers-after.txt" && ! docker network inspect "$NET" >/dev/null 2>&1 \
            && cmp -s "$OUT/volumes-before.txt" <(docker volume ls -q | sort); then pass "Z1-NOTHING the dry runs created, started or changed nothing"
        else fail "Z1-NOTHING the dry runs left something: $(diff "$OUT/z1-containers-before.txt" "$OUT/z1-containers-after.txt" | tr '\n' ' ') network $(docker network inspect "$NET" >/dev/null 2>&1 && echo present || echo absent)"; fi
    fi

    if ! infra_up z; then fail "Z2-UP infra up failed: $(tail -5 "$OUT/stack-script.log" | tr '\n' ' ')"; return; fi
    local in_project
    if [ "$LAYOUT" = script ]; then in_project="$(docker ps -a --filter "label=com.docker.compose.project=$INFRA" --format '{{.Names}}' | sort | tr '\n' ' ')"
    else in_project="$ROUTER "; fi
    local cmd; cmd="$(docker inspect -f '{{join .Config.Cmd " "}} {{join .Config.Entrypoint " "}}' "$ROUTER" 2>/dev/null)"
    if [ "$in_project" = "$ROUTER " ] && [[ "$cmd" == *moirai-router* ]] && ! docker container inspect "$BOOT" >/dev/null 2>&1 \
        && ! docker container inspect "$RELAY" >/dev/null 2>&1; then pass "Z2-ROUTER the infrastructure is $in_project($cmd), no bootnode, no relay"
    else fail "Z2-ROUTER in the project: [$in_project]; router command [$cmd]; bootnode $(docker container inspect "$BOOT" >/dev/null 2>&1 && echo present || echo absent); relay $(docker container inspect "$RELAY" >/dev/null 2>&1 && echo present || echo absent)"; return; fi
    local ports bindings
    ports="$(docker port "$ROUTER" 2>/dev/null)"; bindings="$(docker inspect -f '{{json .HostConfig.PortBindings}}' "$ROUTER")"
    if [ -z "$ports" ] && { [ "$bindings" = "{}" ] || [ "$bindings" = "null" ]; }; then pass "Z2-NO-HOST-PORT the router publishes no port"
    else fail "Z2-NO-HOST-PORT published: $ports $bindings"; fi
    if router_ready 30; then pass "Z2-LISTEN $(ns_parse "$OUT/router-ns.txt" listens 7447)"; else fail "Z2-LISTEN moirai-router holds no LISTEN socket on 7447: $(head -c 300 "$OUT/router-ns.txt")"; fi
    if ns_parse "$OUT/router-ns.txt" udp > "$OUT/router-udp.txt" 2>&1; then pass "Z2-NO-UDP $(cat "$OUT/router-udp.txt")"; else fail "Z2-NO-UDP $(cat "$OUT/router-udp.txt")"; fi
    local aliases; aliases="$(docker inspect -f "{{with index .NetworkSettings.Networks \"$NET\"}}{{json .Aliases}} {{json .DNSNames}}{{end}}" "$ROUTER")"
    if grep -q '"router"' <<< "$aliases"; then pass "Z2-ALIAS on $NET as router ($aliases)"; else fail "Z2-ALIAS on $NET: $aliases"; fi

    for r in nw19-alice nw19-bob nw19-carol; do
        replica_up z "$r" || { fail "Z3-UP $r: $(tail -5 "$OUT/stack-script.log" | tr '\n' ' ')"; return; }
    done
    local bad=""
    for r in nw19-alice nw19-bob nw19-carol; do env_says "$r" zenoh || bad="$bad $r:[$(env_of "$r" | grep -E '^(MOIRAI_TRANSPORT|ZENOH_ROUTER|BOOTNODE_URL)=' | tr '\n' ' ')]"; done
    [ -z "$bad" ] && pass "Z3-ENV each replica told MOIRAI_TRANSPORT=zenoh, ZENOH_ROUTER=tcp/router:7447, no BOOTNODE_URL" || fail "Z3-ENV$bad"
    prog Z3-MESH mesh "" "" 30 nw19-alice nw19-bob nw19-carol || return

    declare -A IP; for r in nw19-alice nw19-bob nw19-carol $ROUTER; do IP[$r]="$(ip_on "$r")"; done
    note "Z4 addresses on $NET: alice ${IP[nw19-alice]} bob ${IP[nw19-bob]} carol ${IP[nw19-carol]} router ${IP[$ROUTER]}"
    local deadline=$(( $(date +%s) + 30 )) missing
    while :; do
        missing=""
        for r in nw19-alice nw19-bob nw19-carol; do
            ns_dump "$r" model_node > "$OUT/z4-$r-ns.txt"
            ns_parse "$OUT/z4-$r-ns.txt" links > "$OUT/z4-$r-links.txt" 2>&1
            for q in nw19-alice nw19-bob nw19-carol; do
                [ "$q" = "$r" ] && continue
                grep -qE "^[0-9.]+:9001 ${IP[$q]}:[0-9]+\$|^[0-9.]+:[0-9]+ ${IP[$q]}:9001\$" "$OUT/z4-$r-links.txt" || missing="$missing $r-$q"
            done
            grep -qE " ${IP[$ROUTER]}:7447\$" "$OUT/z4-$r-links.txt" || missing="$missing $r-router"
        done
        [ -z "$missing" ] && break
        [ "$(date +%s)" -lt "$deadline" ] || break
        sleep 2
    done
    if [ -z "$missing" ]; then pass "Z4-PEER-TO-PEER each replica links directly to the other two on 9001 and to the router on 7447 ($(for r in nw19-alice nw19-bob nw19-carol; do printf '%s: %s; ' "$r" "$(tr '\n' ' ' < "$OUT/z4-$r-links.txt")"; done))"
    else fail "Z4-PEER-TO-PEER missing links:$missing ($(for r in nw19-alice nw19-bob nw19-carol; do printf '%s: %s; ' "$r" "$(tr '\n' ' ' < "$OUT/z4-$r-links.txt")"; done))"; fi

    prog Z5-REGISTER register "" "" 30 nw19-alice nw19-bob nw19-carol || return
    prog Z5-WRITE write 5 w1 30 nw19-alice nw19-bob nw19-carol || return
    prog Z5-CONVERGE converge "" "" 30 nw19-alice nw19-bob nw19-carol

    log "router stop"; docker stop "$ROUTER" > /dev/null
    [ "$(docker inspect -f '{{.State.Running}}' "$ROUTER")" = false ] && note "Z6 router stopped" || fail "Z6-STOP the router still runs"
    prog Z6-WRITE-STOPPED write 10 w2 30 nw19-alice nw19-bob nw19-carol
    prog Z6-CONVERGE-STOPPED converge "" "" 30 nw19-alice nw19-bob nw19-carol
    local before; before="$(date +%s)"
    log "router start"; docker start "$ROUTER" > /dev/null
    local started; started="$(date -d "$(docker inspect -f '{{.State.StartedAt}}' "$ROUTER")" +%s 2>/dev/null || echo 0)"
    if router_ready 30 && [ "$started" -ge "$((before - 2))" ]; then pass "Z6-RESTART the router runs again, a new process started $((started - before)) s after the start command, listening on 7447"
    else fail "Z6-RESTART the router is not back within 30 s (started $started, command at $before)"; fi
    replica_up z nw19-dave || { fail "Z6-UP dave: $(tail -5 "$OUT/stack-script.log" | tr '\n' ' ')"; return; }
    env_says nw19-dave zenoh && pass "Z6-ENV dave told MOIRAI_TRANSPORT=zenoh and the router" || fail "Z6-ENV dave: $(env_of nw19-dave | grep -E '^(MOIRAI_TRANSPORT|ZENOH_ROUTER|BOOTNODE_URL)=' | tr '\n' ' ')"
    prog Z6-MESH-DAVE mesh "" "" 30 nw19-alice nw19-bob nw19-carol nw19-dave
    prog Z6-JOIN-DAVE join nw19-dave "" 30 nw19-alice nw19-bob nw19-carol nw19-dave
    prog Z6-CONVERGE-DAVE converge "" "" 30 nw19-alice nw19-bob nw19-carol nw19-dave

    # Z7: dave holds the model and has originated no operation in it, so under its name it comes
    # back as a fresh member and a donor serves it a transfer (T6's rule).
    replica_down z nw19-dave
    docker container inspect nw19-dave >/dev/null 2>&1 && fail "Z7-DOWN nw19-dave still exists" || note "Z7 dave down"
    prog Z7-DISCONNECTED disconnected nw19-dave "" 15 nw19-alice nw19-bob nw19-carol
    replica_up z nw19-dave || { fail "Z7-UP dave: $(tail -5 "$OUT/stack-script.log" | tr '\n' ' ')"; return; }
    prog Z7-MESH mesh "" "" 30 nw19-alice nw19-bob nw19-carol nw19-dave
    prog Z7-JOIN join nw19-dave "" 30 nw19-alice nw19-bob nw19-carol nw19-dave
    prog Z7-CONVERGE converge "" "" 30 nw19-alice nw19-bob nw19-carol nw19-dave
    # Z7B, recorded and never counted: carol wrote in the model, so under its old name it is a
    # returning member, refused a transfer and left with nothing: e2e_convergence's
    # j3b_a_returning_member_under_its_old_id_is_stranded (ignored, phase 3), on any transport.
    replica_down z nw19-carol
    replica_up z nw19-carol && NW19_RECORD_ONLY=1 prog Z7B-WRITER-RETURNS join nw19-carol "" 20 nw19-alice nw19-bob nw19-carol nw19-dave

    [ "${NW19_EDITOR:-1}" = 1 ] && editor Z8-EDITOR nw19-alice nw19-bob

    down_all z
    local left; left="$(for n in $ALL_NAMES; do docker container inspect "$n" >/dev/null 2>&1 && printf '%s ' "$n"; done)"
    if [ -z "$left" ] && ! docker network inspect "$NET" >/dev/null 2>&1; then pass "Z9-DOWN-ALL nothing of the Zenoh stack left, the session network gone"
    else fail "Z9-DOWN-ALL left: $left network $(docker network inspect "$NET" >/dev/null 2>&1 && echo present)"; fi
}

# ------------------------------------------------------------------ T
run_t() {
    local r
    : > "$OUT/state.json"
    if ! infra_up t; then fail "T1-UP infra up failed: $(tail -5 "$OUT/stack-script.log" | tr '\n' ' ')"; return; fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$BOOT" 2>/dev/null)" = true ] && [ "$(docker inspect -f '{{.State.Running}}' "$RELAY" 2>/dev/null)" = true ] \
        && ! docker container inspect "$ROUTER" >/dev/null 2>&1; then pass "T1-INFRA $BOOT and $RELAY running, no router"
    else fail "T1-INFRA bootnode $(docker inspect -f '{{.State.Running}}' "$BOOT" 2>&1), relay $(docker inspect -f '{{.State.Running}}' "$RELAY" 2>&1), router $(docker container inspect "$ROUTER" >/dev/null 2>&1 && echo present || echo absent)"; return; fi
    for r in nw19-alice nw19-bob nw19-carol; do
        replica_up t "$r" || { fail "T2-UP $r: $(tail -5 "$OUT/stack-script.log" | tr '\n' ' ')"; return; }
    done
    local bad=""
    for r in nw19-alice nw19-bob nw19-carol; do env_says "$r" tcp || bad="$bad $r:[$(env_of "$r" | grep -E '^(MOIRAI_TRANSPORT|BOOTNODE_URL)=' | tr '\n' ' ')]"; done
    [ -z "$bad" ] && pass "T2-ENV each replica told MOIRAI_TRANSPORT=tcp" || fail "T2-ENV$bad"
    prog T2-MESH mesh "" "" 30 nw19-alice nw19-bob nw19-carol || return
    prog T3-REGISTER register "" "" 30 nw19-alice nw19-bob nw19-carol || return
    prog T3-WRITE write 5 t1 30 nw19-alice nw19-bob nw19-carol
    prog T3-CONVERGE converge "" "" 30 nw19-alice nw19-bob nw19-carol
    [ "${NW19_EDITOR_T:-0}" = 1 ] && editor T3-EDITOR nw19-alice nw19-bob
    # T5, recorded and never counted: Z7B's writer returning under its old name, on TCP.
    replica_down t nw19-carol
    replica_up t nw19-carol && NW19_RECORD_ONLY=1 prog T5-WRITER-RETURNS join nw19-carol "" 20 nw19-alice nw19-bob nw19-carol
    down_all t
    local left; left="$(for n in $ALL_NAMES; do docker container inspect "$n" >/dev/null 2>&1 && printf '%s ' "$n"; done)"
    if [ -z "$left" ] && ! docker network inspect "$NET" >/dev/null 2>&1; then pass "T4-DOWN-ALL nothing of the TCP stack left, the session network gone"
    else fail "T4-DOWN-ALL left: $left network $(docker network inspect "$NET" >/dev/null 2>&1 && echo present)"; fi
}

case "$RUNS" in *Z*) log "run Z"; run_z; cleanup ;; esac
case "$RUNS" in *T*) [ "$LAYOUT" = script ] && { log "run T"; run_t; cleanup; } ;; esac

# ------------------------------------------------------------------ clean-up and protected tags
cleanup
left="$(for n in $ALL_NAMES; do docker container inspect "$n" >/dev/null 2>&1 && printf '%s ' "$n"; done; docker ps -a --filter 'label=com.docker.compose.project' --format '{{.Label "com.docker.compose.project"}}' | grep -E '^(moirai-infra-nw19|moirai-replica-nw19-)' | tr '\n' ' ')"
docker volume ls -q | sort > "$OUT/volumes-after.txt"
new_v="$(comm -13 "$OUT/volumes-before.txt" "$OUT/volumes-after.txt")"
for v in $new_v; do [[ "$v" =~ ^[0-9a-f]{64}$ ]] && docker volume rm "$v" >/dev/null 2>&1 && note "removed anonymous volume $v the run created"; done
if [ -z "${left// /}" ] && ! docker network inspect "$NET" >/dev/null 2>&1; then pass "CLEAN no container, project or network of the run left; volumes created: $(printf '%s' "$new_v" | wc -w)"
else fail "CLEAN left: $left"; fi
protected_ids > "$OUT/protected-after.txt"
if cmp -s "$OUT/protected-before.txt" "$OUT/protected-after.txt"; then pass "PROTECTED every protected tag as before"
else fail "PROTECTED $(diff "$OUT/protected-before.txt" "$OUT/protected-after.txt" | tr '\n' ' ')"; fi
trap - EXIT
if [ "$FAILS" -eq 0 ]; then echo "NW.19 STACK: PASS ($RUNS, $LAYOUT)" | tee -a "$OUT/verdicts.txt"; exit 0; fi
echo "NW.19 STACK: FAIL ($FAILS failing checks; $RUNS, $LAYOUT)" | tee -a "$OUT/verdicts.txt"
exit 1
