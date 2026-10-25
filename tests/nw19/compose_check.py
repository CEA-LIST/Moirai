#!/usr/bin/env python3
"""NW.19, the reader of what Compose would run (vault spec/items/NW.19.md, sections 2 and 7).

Reads the JSON object `docker compose ... config --format json` prints (through the stack
script's dry run, I-4, or directly for the rig) and checks one registered property, printing one
`PASS` or `FAIL` line; exit 0 on PASS, 1 on FAIL, 2 when the input is not such an object.

  compose_check.py infra <json> zenoh [ROUTER_NAME NETWORK]
      exactly one service runs moirai-router (its command or entrypoint names it), with
      LISTEN_PORT 7447, no published port, on the session network (NETWORK, the external
      network's name) with the alias `router`, container named ROUTER_NAME when given; no
      service runs moirai-bootnode or moirai-relay (I-2).
  compose_check.py infra <json> tcp
      one service runs moirai-bootnode and one moirai-relay; none runs moirai-router (I-5).
  compose_check.py replica <json> zenoh|tcp [NAME]
      exactly one service, container named NAME when given, its environment stating
      MOIRAI_TRANSPORT as the mode; under zenoh ZENOH_ROUTER=tcp/router:7447, BOOTNODE_URL absent
      or empty, PEERS absent or empty; under tcp BOOTNODE_URL=http://bootnode:7000 and
      ZENOH_ROUTER absent or empty; HTTP_ALLOWED_ORIGINS present in both (NW.11's s2) (I-3, I-5).
  compose_check.py rig <json>
      every service of the measurement rig whose environment sets LISTEN_PORT (a replica) states
      MOIRAI_TRANSPORT=tcp (the rig pinned to TCP whole).
  compose_check.py dockerfile <path>
      the image builds moirai-router (a RUN running `cargo build` names it) and ships it (a COPY
      names it and /usr/local/bin) (I-6).
"""
import json
import sys


def runs(service, binary):
    words = []
    for key in ("command", "entrypoint"):
        value = service.get(key)
        if isinstance(value, list):
            words += value
        elif isinstance(value, str):
            words += value.split()
    return any(binary in w for w in words)


def env(service):
    e = service.get("environment") or {}
    if isinstance(e, list):
        out = {}
        for item in e:
            k, _, v = item.partition("=")
            out[k] = v
        return out
    return {k: ("" if v is None else str(v)) for k, v in e.items()}


def result(ok, what, detail):
    print(f"{'PASS' if ok else 'FAIL'} {what}: {detail}")
    return 0 if ok else 1


def infra(doc, mode, args):
    services = doc.get("services") or {}
    by = {b: [n for n, s in services.items() if runs(s, b)]
          for b in ("moirai-router", "moirai-bootnode", "moirai-relay")}
    if mode == "tcp":
        ok = len(by["moirai-bootnode"]) == 1 and len(by["moirai-relay"]) == 1 and not by["moirai-router"]
        return result(ok, "INFRA-TCP bootnode and relay, no router", json.dumps(by))
    problems = []
    if len(by["moirai-router"]) != 1:
        problems.append(f"services running moirai-router: {by['moirai-router']}")
    if by["moirai-bootnode"] or by["moirai-relay"]:
        problems.append(f"bootnode {by['moirai-bootnode']}, relay {by['moirai-relay']}")
    if len(by["moirai-router"]) == 1:
        name = by["moirai-router"][0]
        s = services[name]
        e = env(s)
        if e.get("LISTEN_PORT") != "7447":
            problems.append(f"{name} LISTEN_PORT={e.get('LISTEN_PORT')!r}")
        if s.get("ports"):
            problems.append(f"{name} publishes {s.get('ports')}")
        if len(args) >= 1 and s.get("container_name") != args[0]:
            problems.append(f"{name} container_name {s.get('container_name')!r}, not {args[0]!r}")
        nets = doc.get("networks") or {}
        aliased = [key for key, v in (s.get("networks") or {}).items()
                   if "router" in ((v or {}).get("aliases") or [])]
        names = [(nets.get(key) or {}).get("name", key) for key in aliased]
        if not aliased or (len(args) >= 2 and args[1] not in names):
            problems.append(f"{name} has the alias `router` on {names}, the session network "
                            f"{args[1] if len(args) >= 2 else '(any)'} expected")
    return result(not problems, "INFRA-ZENOH one router, no bootnode or relay",
                  "; ".join(problems) or json.dumps(by))


def replica(doc, mode, args):
    services = doc.get("services") or {}
    if len(services) != 1:
        return result(False, f"REPLICA-{mode.upper()}", f"{len(services)} services: {list(services)}")
    name, s = next(iter(services.items()))
    e = env(s)
    problems = []
    if args and s.get("container_name") != args[0]:
        problems.append(f"container_name {s.get('container_name')!r}, not {args[0]!r}")
    if e.get("MOIRAI_TRANSPORT") != mode:
        problems.append(f"MOIRAI_TRANSPORT={e.get('MOIRAI_TRANSPORT')!r}")
    if "HTTP_ALLOWED_ORIGINS" not in e:
        problems.append("no HTTP_ALLOWED_ORIGINS")
    if mode == "zenoh":
        if e.get("ZENOH_ROUTER") != "tcp/router:7447":
            problems.append(f"ZENOH_ROUTER={e.get('ZENOH_ROUTER')!r}")
        if e.get("BOOTNODE_URL", ""):
            problems.append(f"BOOTNODE_URL={e.get('BOOTNODE_URL')!r}")
        if e.get("PEERS", ""):
            problems.append(f"PEERS={e.get('PEERS')!r}")
    else:
        if e.get("BOOTNODE_URL") != "http://bootnode:7000":
            problems.append(f"BOOTNODE_URL={e.get('BOOTNODE_URL')!r}")
        if e.get("ZENOH_ROUTER", ""):
            problems.append(f"ZENOH_ROUTER={e.get('ZENOH_ROUTER')!r}")
    shown = {k: e.get(k) for k in ("MOIRAI_TRANSPORT", "ZENOH_ROUTER", "BOOTNODE_URL", "PEERS")}
    return result(not problems, f"REPLICA-{mode.upper()} {name}", "; ".join(problems) or json.dumps(shown))


def rig(doc):
    services = doc.get("services") or {}
    replicas = {n: env(s).get("MOIRAI_TRANSPORT") for n, s in services.items() if "LISTEN_PORT" in env(s)}
    ok = bool(replicas) and all(v == "tcp" for v in replicas.values())
    return result(ok, "RIG-TCP every rig replica states MOIRAI_TRANSPORT=tcp", json.dumps(replicas))


def dockerfile(path):
    """The Dockerfile builds moirai-router (a RUN running `cargo build` names it) and ships it (a
    COPY names it and /usr/local/bin); instructions read with continuation lines joined."""
    import re
    try:
        text = open(path).read()
    except OSError as e:
        return result(False, "IMAGE-ROUTER", f"{path}: {e}")
    lines, cur = [], ""
    for raw in text.splitlines():
        if raw.lstrip().startswith("#") and not cur:
            continue
        if raw.rstrip().endswith("\\"):
            cur += raw.rstrip()[:-1] + " "
            continue
        lines.append(cur + raw)
        cur = ""
    built = any(re.match(r"\s*RUN\b", l) and "cargo build" in l and "moirai-router" in l for l in lines)
    shipped = any(re.match(r"\s*COPY\b", l) and "moirai-router" in l and "/usr/local/bin" in l
                  for l in lines)
    return result(built and shipped, "IMAGE-ROUTER the image builds and ships moirai-router",
                  f"built {built}, shipped {shipped}")


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "dockerfile":
        return dockerfile(sys.argv[2])
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    try:
        with open(sys.argv[2]) as f:
            doc = json.load(f)
    except (OSError, ValueError) as e:
        print(f"FAIL {sys.argv[1].upper()}: {sys.argv[2]} is not a Compose configuration: {e}")
        return 2
    if not isinstance(doc, dict):
        print(f"FAIL {sys.argv[1].upper()}: {sys.argv[2]} is not a JSON object")
        return 2
    kind = sys.argv[1]
    if kind == "infra":
        return infra(doc, sys.argv[3], sys.argv[4:])
    if kind == "replica":
        return replica(doc, sys.argv[3], sys.argv[4:])
    if kind == "rig":
        return rig(doc)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
