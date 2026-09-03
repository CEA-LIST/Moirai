# Hand-written mutants

One file per mutant, applied and reverted by `../hand-mutants.py`: a JSON
object with `file` (relative to the Moirai or Arachne worktree, prefixed
`moirai:` or `arachne:`), `find` (text that must occur exactly once), `replace`,
the `class` the validation plan names, `test` (the command whose failure
kills the mutant, run from that worktree) and, where the mutant is built into
a binary, `after` (run once the source is reverted, to rebuild that binary). These are the classes
`cargo-mutants` cannot express or reach: a swapped `LogId` in dispatch is a
change of key and not of operator; the header code lives in an example binary
of another workspace; and the digest comparison and the store write are
TypeScript.

The node-binary mutants build the `network_node` example before `cargo test`:
`cargo test --test model_plane` alone builds only that test target and would
spawn whatever example binary was built last, which is how a mutant of the
binary can "survive" without ever running.
