use std::{collections::VecDeque, rc::Rc};

use crate::{
    clocks::{
        clock::{Clock, Full, Partial},
        dot::Dot,
        matrix_clock::MatrixClock,
    },
    protocol::{event::Event, log::Log, membership::ViewData, pulling::Since},
};

#[derive(Clone, Debug)]
pub enum Duet<F, S> {
    First(F),
    Second(S),
}

#[derive(Clone, Debug, Default)]
pub struct DuetLog<Fl, Sl> {
    first: Fl,
    second: Sl,
}

impl<Fl, Sl> Log for DuetLog<Fl, Sl>
where
    Fl: Log,
    Sl: Log,
{
    type Value = (Fl::Value, Sl::Value);
    type Op = Duet<Fl::Op, Sl::Op>;

    fn new() -> Self {
        Self {
            first: Fl::new(),
            second: Sl::new(),
        }
    }

    fn new_event(&mut self, event: &Event<Self::Op>) {
        match event.op {
            Duet::First(ref op) => {
                let event = Event::new_nested(op.clone(), event.metadata.clone(), event.lamport());
                self.first.new_event(&event);
            }
            Duet::Second(ref op) => {
                let event = Event::new_nested(op.clone(), event.metadata.clone(), event.lamport());
                self.second.new_event(&event);
            }
        }
    }

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool, ltm: &MatrixClock) {
        match &event.op {
            Duet::First(op) => {
                let event = Event::new(op.clone(), event.metadata().clone(), event.lamport());
                self.first.prune_redundant_events(&event, is_r_0, ltm);
            }
            Duet::Second(op) => {
                let event = Event::new(op.clone(), event.metadata().clone(), event.lamport());
                self.second.prune_redundant_events(&event, is_r_0, ltm);
            }
        }
    }

    fn purge_stable_metadata(&mut self, dot: &Dot) {
        self.first.purge_stable_metadata(dot);
        self.second.purge_stable_metadata(dot);
    }

    fn stable_by_clock(&mut self, clock: &Clock<Full>) {
        self.first.stable_by_clock(clock);
        self.second.stable_by_clock(clock);
    }

    fn collect_events_since(&self, since: &Since, ltm: &MatrixClock) -> Vec<Event<Self::Op>> {
        let mut result = self
            .first
            .collect_events_since(since, ltm)
            .into_iter()
            .map(|e| Event::new(Duet::First(e.op.clone()), e.metadata().clone(), e.lamport()))
            .collect::<Vec<_>>();
        result.extend(
            self.second
                .collect_events_since(since, ltm)
                .into_iter()
                .map(|e| {
                    Event::new(
                        Duet::Second(e.op.clone()),
                        e.metadata().clone(),
                        e.lamport(),
                    )
                }),
        );
        result
    }

    fn clock_from_event(&self, event: &Event<Self::Op>) -> Clock<Full> {
        match &event.op {
            Duet::First(op) => self.first.clock_from_event(&Event::new(
                op.clone(),
                event.metadata().clone(),
                event.lamport(),
            )),
            Duet::Second(op) => self.second.clock_from_event(&Event::new(
                op.clone(),
                event.metadata().clone(),
                event.lamport(),
            )),
        }
    }

    fn r_n(&mut self, metadata: &Clock<Full>, conservative: bool) {
        self.first.r_n(metadata, conservative);
        self.second.r_n(metadata, conservative);
    }

    fn redundant_itself(&self, event: &Event<Self::Op>) -> bool {
        match event.op {
            Duet::First(ref op) => {
                let first = Event::new(op.clone(), event.metadata().clone(), event.lamport());
                self.first.redundant_itself(&first)
            }
            Duet::Second(ref op) => {
                let second = Event::new(op.clone(), event.metadata().clone(), event.lamport());
                self.second.redundant_itself(&second)
            }
        }
    }

    fn eval(&self) -> Self::Value {
        (self.first.eval(), self.second.eval())
    }

    fn stabilize(&mut self, dot: &Dot) {
        self.first.stabilize(dot);
        self.second.stabilize(dot);
    }

    fn is_empty(&self) -> bool {
        self.first.is_empty() && self.second.is_empty()
    }

    fn deps(
        &mut self,
        clocks: &mut VecDeque<Clock<Partial>>,
        view: &Rc<ViewData>,
        dot: &Dot,
        op: &Self::Op,
    ) {
        match op {
            Duet::First(ref op) => self.first.deps(clocks, view, dot, op),
            Duet::Second(ref op) => {
                self.second.deps(clocks, view, dot, op);
            }
        }
    }
}
