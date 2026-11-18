"""Bounded executable model checks; these do NOT compile or execute Rust.

Reference predicates use complete DAGs and all partial predecessors. The model
uses the current VoteLog, OmegaCommit AtomicBroadcastProtocol, CommitmentLog,
and sorted child-log responsibilities. Event IDs use canonical stable replica IDs;
local Resolver translation is covered by the Rust tests, not this model.
"""
from dataclasses import dataclass
from functools import lru_cache
from random import Random
import argparse
import json
from pathlib import Path

@dataclass(frozen=True)
class Vertex:
    origin: int
    vc: tuple
    vote: int
    partial: bool
    previous: tuple | None
    @property
    def seq(self): return self.vc[self.origin]
    @property
    def id(self): return self.origin, self.seq
    @property
    def key(self): return sum(self.vc), self.origin, self.seq

def before(a, b): return a.id != b.id and a.seq <= b.vc[a.origin]
def present(v, graph): return frozenset(e for e in graph if e == v or before(e, v))

@lru_cache(maxsize=20000)
def support(v, graph):
    result = {}
    for r in range(len(v.vc)):
        trace = sorted((e for e in graph if e.origin == r), key=lambda e: e.seq)
        past = [e for e in trace if before(e, v)]
        if not past: continue
        future = [e for e in trace if before(v, e)]
        last = future[0] if future else trace[-1]
        if all(e.vote == v.origin for e in trace if past[-1].seq <= e.seq <= last.seq):
            result[r] = bool(future)
    return result

@lru_cache(maxsize=12000)
def reference(graph, n):
    # The user's intentional variant: recurse over partials in past(w),
    # excluding w's own vote when selecting its predecessor checkpoints.
    checkpoints = {v for v in graph if sum(support(v, graph).values()) > n // 2}
    while True:
        expanded = set(checkpoints)
        for w in checkpoints:
            past = frozenset(e for e in graph if before(e, w))
            expanded.update(v for v in past if len(support(v, past)) > n // 2)
        if expanded == checkpoints: break
        checkpoints = expanded
    ordered = sorted(checkpoints, key=lambda v: v.key)
    assert all(before(a, b) for a, b in zip(ordered, ordered[1:])), 'Reference checkpoint concurrency'
    committed, seen = [], set()
    for anchor in ordered:
        batch = present(anchor, graph) - seen
        committed += sorted(batch, key=lambda v: v.key)
        seen |= batch
    optimistic = committed + sorted(graph - seen, key=lambda v: v.key)
    return tuple(committed), tuple(optimistic)

class VoteLog:
    def __init__(self, n):
        assert n > 0
        self.n = n
        self.votes = {}
        self.candidates = {}
        self.last = None
        self.ignored = 0

    def after_committed(self, v):
        return self.last is None or (v.id != self.last.id and all(a >= b for a,b in zip(v.vc, self.last.vc)))

    def is_potential(self, origin):
        partial = sum(t[max(t)] == origin for t in self.votes.values() if t) > self.n // 2
        options = [v for v,state in self.candidates.items() if len(state) > self.n // 2]
        prev = max(options, key=lambda v:v.key).id if partial and options else None
        return partial, prev

    def insert(self, v):
        assert v.seq > 0
        trace = self.votes.setdefault(v.origin, {})
        assert max(trace, default=0) == v.seq - 1
        trace[v.seq] = v.vote
        for candidate, state in self.candidates.items():
            if state.get(v.origin) is not False: continue
            if v.vote != candidate.origin:
                del state[v.origin]
                continue
            if v.vc[candidate.origin] >= candidate.seq:
                state[v.origin] = True
        if v.partial and self.after_committed(v):
            state = {}
            for r, trace in self.votes.items():
                base = v.seq - 1 if r == v.origin else v.vc[r]
                if base == 0: continue
                assert base in trace, ('pruned candidate predecessor', v, r, base, self.last)
                if all(vote == v.origin for seq,vote in trace.items() if seq >= base):
                    state[r] = False
            self.candidates[v] = state
        elif v.partial:
            self.ignored += 1

    def ready_to_commit(self):
        leaders = [v for v,state in self.candidates.items() if self.after_committed(v) and sum(state.values()) > self.n // 2]
        if not leaders: return []
        anchor = max(leaders, key=lambda v:v.key)
        ready = {anchor}
        by_id = {v.id:v for v in self.candidates}
        while anchor.previous:
            r,seq = anchor.previous
            if self.last is not None and seq <= self.last.vc[r]: break
            previous = by_id[anchor.previous]
            assert previous.id != anchor.id and all(a <= b for a, b in zip(previous.vc, anchor.vc))
            assert previous not in ready
            ready.add(previous)
            anchor = previous
        return sorted(ready, key=lambda v:v.key)

    def get_causal_frontier(self, version):
        return tuple(max(0, seq - 1 - (r == version.origin))
                     for r, seq in enumerate(version.vc))

    def prune(self, frontier):
        self.votes = {r: {seq: vote for seq, vote in trace.items() if seq > frontier[r]}
                      for r, trace in self.votes.items()}
        self.candidates = {v: state for v, state in self.candidates.items()
                           if v.seq > frontier[v.origin]}

    def set_committed(self, version):
        # Rust looks up the retained candidate by Version, after metadata pruning.
        candidate = next((v for v in self.candidates
                          if v.origin == version.origin and v.vc == version.vc), None)
        if candidate is not None:
            assert self.after_committed(candidate)
            self.last = candidate


@dataclass(frozen=True)
class SupportOp:
    op: object
    vote: int
    is_partial: bool
    last_partial: tuple | None


class OmegaCommit:
    """AtomicBroadcastProtocol model; canonical IDs abstract away Resolver indices."""
    def __init__(self, n, own_id, pruning=True):
        assert 0 <= own_id < n
        self.own_id = own_id
        self.oracle = 0
        self.vote_log = VoteLog(n)
        self.pruning = pruning

    def ab_prepare(self, op):
        assert 0 <= self.oracle < self.vote_log.n
        partial, previous = self.vote_log.is_potential(self.own_id)
        return SupportOp(op, self.oracle, partial, previous)

    @staticmethod
    def child_op(op):
        return op.op

    def ab_handler(self, vertex):
        assert 0 <= vertex.vote < self.vote_log.n
        if vertex.previous is not None:
            origin, seq = vertex.previous
            assert 0 <= origin < self.vote_log.n and seq > 0
            assert seq <= vertex.vc[origin] - (origin == vertex.origin)
        self.vote_log.insert(vertex)

    def ab_ready_to_commit(self):
        # Returns a snapshot before the caller mutates protocol state.
        return self.vote_log.ready_to_commit()

    def ab_prune(self, version):
        if self.pruning:
            self.vote_log.prune(self.vote_log.get_causal_frontier(version))

    def ab_set_last_committed(self, version):
        self.vote_log.set_committed(version)


class SequenceLog:
    """Child log with the sorted commitment/optimistic policy in examples/tests.rs."""
    def __init__(self):
        self.committed = []
        self.unstable = set()

    @staticmethod
    def prepare(op):
        return op

    def effect(self, vertex):
        self.unstable.add(vertex)

    def stabilize(self, version):
        batch = present(version, self.unstable)
        self.committed += sorted(batch, key=lambda v: v.key)
        self.unstable -= batch

    def execute_query(self):
        return tuple(self.committed + sorted(self.unstable, key=lambda v: v.key))


class CommitmentLog:
    def __init__(self, n, own_id=0, pruning=True):
        self.child = SequenceLog()
        self.ab_protocol = OmegaCommit(n, own_id, pruning)
        self.cuts = 0

    def prepare(self, command):
        return self.ab_protocol.ab_prepare(self.child.prepare(command))

    def effect(self, vertex):
        self.child.effect(vertex)
        self.ab_protocol.ab_handler(vertex)
        ready = self.ab_protocol.ab_ready_to_commit()
        for version in ready:
            self.child.stabilize(version)
            self.ab_protocol.ab_prune(version)
            self.ab_protocol.ab_set_last_committed(version)
            self.cuts += 1

    def stabilize(self, frontier):
        # TCSB causal stability must never advance application commitment.
        pass

    def execute_query(self):
        return self.child.execute_query()

class Execution:
    def __init__(self, n, seed):
        self.n=n
        self.rng=Random(seed)
        self.nodes=[CommitmentLog(n, r) for r in range(n)]
        self.full=[CommitmentLog(n, r, pruning=False) for r in range(n)]
        self.seen=[set() for _ in range(n)]
        self.all=set()
        self.deliveries=0

    def clock(self, r):
        return tuple(max((e.seq for e in self.seen[r] if e.origin == s),default=0) for s in range(self.n))

    def emit(self,r,vote):
        self.nodes[r].ab_protocol.oracle = vote
        self.full[r].ab_protocol.oracle = vote
        payload = self.nodes[r].prepare(None)
        assert payload == self.full[r].prepare(None), 'Pruning changed prepared metadata'
        partial, prev = payload.is_partial, payload.last_partial
        current=frozenset(self.seen[r])
        expected_partial=sum(next((e.vote for e in sorted(current,key=lambda v:v.seq,reverse=True) if e.origin==s),None)==r for s in range(self.n))>self.n//2
        options=[v for v in current if len(support(v,current))>self.n//2]
        expected_prev=max(options,key=lambda v:v.key).id if expected_partial and options else None
        assert (partial,prev)==(expected_partial,expected_prev), 'Sender predecessor differs from full-DAG rule'
        vc=list(self.clock(r)); vc[r]+=1
        v=Vertex(r,tuple(vc),payload.vote,partial,prev)
        self.all.add(v)
        self.deliver(r,v)
        return v

    def deliver(self,r,v):
        if v in self.seen[r]: return
        clock=self.clock(r)
        assert all(clock[s] >= seq-(s==v.origin) for s,seq in enumerate(v.vc))
        self.nodes[r].effect(v)
        self.full[r].effect(v)
        self.seen[r].add(v)
        graph=frozenset(self.seen[r])
        committed,optimistic=reference(graph,self.n)
        assert tuple(self.nodes[r].child.committed)==committed, ('reference commitment mismatch',r,v)
        assert self.nodes[r].execute_query()==optimistic, ('reference optimistic mismatch',r,v)
        assert self.nodes[r].child.committed==self.full[r].child.committed
        for node in self.nodes:
            a,b=self.nodes[r].child.committed,node.child.committed
            assert a[:len(b)]==b or b[:len(a)]==a, 'Prefix inconsistency'
        pos={v:i for i,v in enumerate(optimistic)}
        for e in graph:
            assert all(pos[p]<pos[e] for p in graph if before(p,e)), 'Causality violation'
        self.deliveries+=1

    def drain(self, receivers):
        for v in sorted(self.all,key=lambda v:v.key):
            for r in receivers:
                if v not in self.seen[r]: self.deliver(r,v)

    def run(self, steps):
        for step in range(steps):
            r=self.rng.randrange(self.n)
            clock=self.clock(r)
            ready=[v for v in sorted(self.all-self.seen[r],key=lambda v:v.key) if all(clock[s]>=seq-(s==v.origin) for s,seq in enumerate(v.vc))]
            if ready and self.rng.random()<0.65:
                self.deliver(r,self.rng.choice(ready))
            else:
                # Alternate stability and arbitrary oracle output to exercise
                # actual cuts, changed votes and historical partial leaders.
                leader=0 if (step//15)%3!=2 else self.rng.randrange(self.n)
                self.emit(r,leader)
        # Crash a minority; survivors disseminate old events and keep issuing.
        live=list(range(self.n//2+1))
        targets=set(self.all)
        self.drain(live)
        for _ in range(5):
            for r in live:
                self.emit(r,0)
                self.drain(live)
        for r in live:
            assert targets <= set(self.nodes[r].child.committed), 'Progress lost after a crash'
        assert all(self.nodes[r].execute_query()==self.nodes[live[0]].execute_query() for r in live)
        return self.deliveries,sum(n.cuts for n in self.nodes),sum(n.ab_protocol.vote_log.ignored for n in self.nodes)

def targeted_pruning():
    log=CommitmentLog(3)
    for r,vc,vote,partial in [
        (0,(1,0,0),0,False),(1,(0,1,0),2,False),(2,(0,0,1),2,False),
        (1,(0,2,0),2,False),(0,(2,2,1),0,False),(0,(3,2,1),0,False),
        (0,(4,2,1),0,False),(1,(4,3,1),0,False),(0,(5,3,1),0,True),
        (0,(6,3,1),0,False),(1,(5,4,1),0,False),
    ]: log.effect(Vertex(r,vc,vote,partial,None))
    votes = log.ab_protocol.vote_log
    assert votes.last.id==(0,5) and 2 not in votes.votes[1]
    delayed=Vertex(2,(2,2,2),2,True,None)
    log.effect(delayed)
    assert delayed not in votes.candidates and votes.votes[2][2]==2

def targeted_pre_vertex_pointer():
    e=Execution(3,0)
    a1=e.emit(0,0); b1=e.emit(1,0)
    e.deliver(0,b1)
    a2=e.emit(0,0); a3=e.emit(0,0); a4=e.emit(0,1)
    c1=e.emit(2,1)
    for v in [a1,a2,a3,a4,c1]: e.deliver(1,v)
    b2=e.emit(1,1)
    assert b2.previous==a2.id
    for v in [c1,b2]: e.deliver(0,v)
    a5=e.emit(0,1)
    for v in [a1,b1,a2,a3,a4,b2]: e.deliver(2,v)
    e.emit(2,1)
    e.deliver(2,a5)
    assert e.nodes[2].ab_protocol.vote_log.last.id==b2.id
    assert a2 in e.nodes[2].child.committed

def targeted_protocol_lifecycle():
    log = CommitmentLog(2)
    payload = log.prepare("child operation")
    assert payload == SupportOp("child operation", 0, False, None)
    assert OmegaCommit.child_op(payload) == "child operation"
    assert not log.ab_protocol.vote_log.votes, 'Preparing must not deliver an event'
    for v in [Vertex(0, (1, 0), 0, False, None),
              Vertex(1, (0, 1), 0, False, None)]:
        log.effect(v)
    log.stabilize((1, 1))
    assert not log.child.committed, 'Causal stability advanced commitment'
    assert log.prepare(None).is_partial
    checkpoint = Vertex(0, (2, 1), 0, True, None)
    log.effect(checkpoint)
    log.effect(Vertex(0, (3, 1), 0, False, None))
    assert not log.child.committed, 'Committed before a majority closed support'
    log.effect(Vertex(1, (2, 2), 0, False, None))
    votes = log.ab_protocol.vote_log
    assert votes.last == checkpoint, 'Pruning lost the checkpoint before set_committed'
    assert len(log.child.committed) == 3 and len(log.child.unstable) == 2
    assert votes.get_causal_frontier(checkpoint) == (0, 0)
    assert 1 in votes.votes[0], 'Metadata must retain the checkpoint predecessor'
    assert not log.ab_protocol.ab_ready_to_commit(), 'Checkpoint would commit twice'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--seeds', type=int, default=40, help='executions per membership size (default: 40)')
    p.add_argument('--steps', type=int, default=90, help='random actions before crash/progress checks (default: 90)')
    p.add_argument('--seed-start', type=int, default=0, help='first random seed, for replay (default: 0)')
    p.add_argument('--replicas', type=int, nargs='+', default=[3, 5], help='membership sizes (default: 3 5)')
    p.add_argument('--output', type=Path, help='write a JSON validation report')
    args = p.parse_args()
    if args.seeds <= 0 or args.steps < 0 or any(n <= 0 or n > 65536 for n in args.replicas):
        p.error('seeds must be positive, steps nonnegative, and replica counts between 1 and 65536')
    if not __debug__:
        p.error('assertions are required; run Python without -O or PYTHONOPTIMIZE')
    targeted_pruning()
    targeted_pre_vertex_pointer()
    targeted_protocol_lifecycle()
    totals = [0, 0, 0]
    for n in args.replicas:
        for seed in range(args.seed_start, args.seed_start + args.seeds):
            support.cache_clear(); reference.cache_clear()
            try:
                values = Execution(n, seed).run(args.steps)
            except AssertionError as error:
                raise AssertionError(f'n={n}, seed={seed}, steps={args.steps}: {error}') from error
            totals = [a + b for a, b in zip(totals, values)]
        print(f'PASS: n={n}, {args.seeds} seeded executions, changing votes, pruning, crash recovery', flush=True)
    print(f'PASS: {totals[0]} event deliveries, {totals[1]} checkpoint applications, {totals[2]} stale candidate admissions suppressed', flush=True)
    if args.output:
        report = {
            'status': 'passed',
            'executes_rust': False,
            'seed_start': args.seed_start,
            'seeds_per_replica_count': args.seeds,
            'steps': args.steps,
            'seeded_executions': len(args.replicas) * args.seeds,
            'replica_counts': args.replicas,
            'event_deliveries': totals[0],
            'checkpoint_applications': totals[1],
            'stale_candidates_suppressed': totals[2],
            'targeted_checks': ['delayed candidate after pruning', 'pre-vertex predecessor choice',
                                'protocol/child log lifecycle'],
        }
        args.output.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
