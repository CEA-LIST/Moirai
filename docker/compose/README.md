# Two ways to start this system, and which one you want

There are now two, and the difference is the lifecycle, not the topology.

`docker-compose.yml` is **the measurement rig**: one Compose project holding a bootnode, a relay, N scaled `node` replicas, an `islander` on a second bridge with no route to them, an observer sampling into `runs/metrics.csv`, a load driver and a dashboard. It is what `rig.sh`, `moirai-network/tests/e2e_convergence.rs` and the scripts under `experiments/` drive, and every committed curve was measured on it. One project means one lifecycle: everything goes up together and comes down together, which is exactly right for a run you intend to plot.

`infra.yml` + `replica.yml`, driven by `stack_interpreter.sh`, is **the session**: the infrastructure as one project (a router under Zenoh, a bootnode and a relay under TCP), and each replica as a project of its own. That buys the one thing the rig cannot do — a replica that comes and goes while the session it belongs to keeps running:

    ./stack_interpreter.sh build
    ./stack_interpreter.sh infra up --dashboard
    ./stack_interpreter.sh up alice --port 8081
    ./stack_interpreter.sh up bob   --port 8082
    ./stack_interpreter.sh down bob          # alice keeps serving its models
    ./stack_interpreter.sh up bob --port 8082  # bob rejoins the same session
    ./stack_interpreter.sh down-all

## Before the first build: the folder names

The build reads two sibling checkouts, and both folder names matter. Clone them like this, side by side in one folder:

    git clone -b akira/moirai-modelstore git@github.com:CEA-LIST/Moirai.git moirai-model-plane
    git clone -b akira/interpreter-modelstore git@github.com:CEA-LIST/Arachne.git arachne
    cd moirai-model-plane/docker/compose && ./stack_interpreter.sh build

The Moirai checkout must be named exactly `moirai-model-plane`: the generated crate in the Arachne branch (`generated/json_crdt/Cargo.toml`) points at `../../../moirai-model-plane/...`. The Arachne checkout sits next to it as `arachne` (or `arachne-model-plane`, which the script prefers when it exists). With the default clone names `Moirai` and `Arachne`, `build` fails: the script derives the image tag from the Moirai folder name and only understands a lowercase `moirai...` name, then cannot find the Arachne checkout. No environment variable is needed once the names are right.

## The stack's transport

The stack runs on the transport the binaries run on unless told otherwise, and states it in every replica it starts (`MOIRAI_TRANSPORT` in the container's environment), never leaving it to the image. `--tcp` before the command, or `MOIRAI_TRANSPORT=tcp` in the environment, is TCP; `MOIRAI_TRANSPORT=zenoh` is Zenoh. Give the same transport to `infra up` and to every `up` of the session: `up` refuses a replica on the other transport than the running infrastructure's, and `ls` and `urls` report the running infrastructure's transport, whatever the call names. `logs infra` shows the router, the bootnode and the relay, whichever runs.

- Zenoh: `infra up` starts one router (`moirai-router`, container `router`, listening on 7447 on the session bridge under the alias `router`, no published port, multicast scouting off; infra.yml's `zenoh` profile). Each replica is told `ZENOH_ROUTER=tcp/router:7447` and no `BOOTNODE_URL`; the router introduces the replicas, which then link directly to each other on 9001 and keep converging while the router is down. There is no directory, so `ls` shows none.
- TCP: `infra up` starts the bootnode and the relay as before (infra.yml's `tcp` profile), and each replica is told `MOIRAI_TRANSPORT=tcp` and `BOOTNODE_URL=http://bootnode:7000`:

      ./stack_interpreter.sh --tcp infra up
      ./stack_interpreter.sh --tcp up alice --port 8081
      ./stack_interpreter.sh --tcp down-all

`stack_interpreter.sh config infra` and `config up NAME --port N` print, as JSON, the Compose configuration `infra up` or `up` with the same arguments and environment would use, and start, create or change nothing (`--tcp config ...` for TCP's).

The rig is TCP whole: `docker-compose.yml` states `MOIRAI_TRANSPORT=tcp` for every replica, because what it measures is the bootnode and the relay.

Use the rig to measure. Use the stack to demonstrate, or to keep a session up across a working session of your own. `stack_interpreter.sh --help` is the full argument list.

Both entry points build their own image, and it is the same image: same `docker/e2e/Dockerfile`, same context two levels up, same build args, same default tag `moirai-json-crdt<worktree suffix>:test`. `stack_interpreter.sh build` does not call `rig.sh`, and `rig.sh build` does not call it, so either script works with the other absent — but a rig measured on an image the stack built is measuring the same binaries, which is the point of keeping the tag shared. The one place they differ on purpose is the generated crate: `rig.sh` can regenerate it over what is on disk (`generate --force`, guarded by `--allow-dirty`), and `stack_interpreter.sh` only ever generates one that is genuinely absent.

Every command that starts a container first says which image it is using and how old it is, because a rig running one commit behind looks exactly like a rig running the current one.

## They coexist

The rig owns `moirai_session` and `moirai_island` and creates and removes them itself. The stack uses `moirai_stack`, created by `stack_interpreter.sh` outside Compose and declared `external` in both YAML files, because separate Compose projects otherwise get separate bridges and a replica project could not reach the infrastructure project. Deliberately a third network rather than a shared one: with the names distinct, `docker compose -f docker-compose.yml down` still removes exactly its own two and warns about nothing, which was verified by running both at once and tearing the rig down under a live stack.

The published host ports are the only thing that actually collide. The rig's `--edit` replicas take 8081 and 8082 and its dashboard takes 8090, so give the stack other ones, or the rig other ones through `EDIT_HTTP_A` / `EDIT_HTTP_B` / `DASHBOARD_PORT`.

## What a restarted replica does and does not remember

Nothing. A replica is a container with in-memory state, so `stack_interpreter.sh up bob` after a `down` gives a replica that has registered with the directory and hosts only its default log. It rejoins a model the way any replica joins one — `POST /api/models {"model_id": ..., "metamodel_id": ...}` — and then fills it by state transfer. That is the same out-of-band join `register-models.sh` performs, and there is no catalogue to consult: the model plane's design leaves the id to a person pasting it, so a rejoin is a paste as well. Under the same name this does not work yet: see the known bug below.

## Known bug: a replica restarted under the same name stays empty

`down bob` followed by `up bob` (the same name) gives a replica whose model stays empty. Rejoining the model (`POST /api/models` with its `model_id`) answers 200, but `GET /api/model/<id>/state` stays `null`, and `/api/model/<id>/metrics` shows pending operations that are never delivered. The other replicas refuse a state transfer to a returning member (their log says merging its history with a snapshot is not implemented), and the delta sync they fall back to never fills the gap. Seen on Zenoh and on TCP on 2026-11-16. Until it is fixed, restart a replica under a new name (`up carol --port ...`): it joins as a fresh member and fills by state transfer within seconds.

## The other scripts here

`register-models.sh` registers N models across the rig's replicas, `drive.sh` is the load driver's loop and `observe.sh` the observer's; all three are the rig's and are mounted into it. None of them is used by `stack_interpreter.sh`.
