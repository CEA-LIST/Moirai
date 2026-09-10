# Two ways to start this system, and which one you want

There are now two, and the difference is the lifecycle, not the topology.

`docker-compose.yml` is **the measurement rig**: one Compose project holding a bootnode, a relay, N scaled `node` replicas, an `islander` on a second bridge with no route to them, an observer sampling into `runs/metrics.csv`, a load driver and a dashboard. It is what `rig.sh`, `moirai-network/tests/e2e_convergence.rs` and the scripts under `experiments/` drive, and every committed curve was measured on it. One project means one lifecycle: everything goes up together and comes down together, which is exactly right for a run you intend to plot.

`infra.yml` + `replica.yml`, driven by `stack.sh`, is **the session**: the bootnode and relay as one project, and each replica as a project of its own. That buys the one thing the rig cannot do — a replica that comes and goes while the session it belongs to keeps running:

    ./stack.sh infra up --dashboard
    ./stack.sh up alice --port 8081
    ./stack.sh up bob   --port 8082
    ./stack.sh down bob          # alice keeps serving its models
    ./stack.sh up bob --port 8082  # bob rejoins the same session
    ./stack.sh down-all

Use the rig to measure. Use the stack to demonstrate, or to keep a session up across a working session of your own. `stack.sh --help` is the full argument list.

## They coexist

The rig owns `moirai_session` and `moirai_island` and creates and removes them itself. The stack uses `moirai_stack`, created by `stack.sh` outside Compose and declared `external` in both YAML files, because separate Compose projects otherwise get separate bridges and a replica project could not reach the infrastructure project. Deliberately a third network rather than a shared one: with the names distinct, `docker compose -f docker-compose.yml down` still removes exactly its own two and warns about nothing, which was verified by running both at once and tearing the rig down under a live stack.

The published host ports are the only thing that actually collide. The rig's `--edit` replicas take 8081 and 8082 and its dashboard takes 8090, so give the stack other ones, or the rig other ones through `EDIT_HTTP_A` / `EDIT_HTTP_B` / `DASHBOARD_PORT`.

## What a restarted replica does and does not remember

Nothing. A replica is a container with in-memory state, so `stack.sh up bob` after a `down` gives a replica that has registered with the directory and hosts only its default log. It rejoins a model the way any replica joins one — `POST /api/models {"model_id": ..., "metamodel_id": ...}` — and then fills it by state transfer. That is the same out-of-band join `register-models.sh` performs, and there is no catalogue to consult: the model plane's design leaves the id to a person pasting it, so a rejoin is a paste as well.

## The other scripts here

`register-models.sh` registers N models across the rig's replicas, `drive.sh` is the load driver's loop and `observe.sh` the observer's; all three are the rig's and are mounted into it. None of them is used by `stack.sh`.
