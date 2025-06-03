use std::{collections::VecDeque, rc::Rc};

use crate::{
    clocks::{
        clock::{Clock, Full, Partial},
        dot::Dot,
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
                let event = Event::new(op.clone(), event.metadata().clone());
                self.first.new_event(&event);
            }
            Duet::Second(ref op) => {
                let event = Event::new(op.clone(), event.metadata().clone());
                self.second.new_event(&event);
            }
        }
    }

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool) {
        match &event.op {
            Duet::First(op) => {
                let event = Event::new(op.clone(), event.metadata().clone());
                self.first.prune_redundant_events(&event, is_r_0);
            }
            Duet::Second(op) => {
                let event = Event::new(op.clone(), event.metadata().clone());
                self.second.prune_redundant_events(&event, is_r_0);
            }
        }
    }

    fn purge_stable_metadata(&mut self, metadata: &Clock<Partial>) {
        self.first.purge_stable_metadata(metadata);
        self.second.purge_stable_metadata(metadata);
    }

    fn collect_events(
        &self,
        upper_bound: &Clock<Full>,
        lower_bound: &Clock<Full>,
    ) -> Vec<Event<Self::Op>> {
        let events_fl = self.first.collect_events(upper_bound, lower_bound);
        let events_sl = self.second.collect_events(upper_bound, lower_bound);
        let mut result = vec![];
        for e in events_fl {
            result.push(Event::new(Duet::First(e.op.clone()), e.metadata().clone()));
        }
        for e in events_sl {
            result.push(Event::new(Duet::Second(e.op.clone()), e.metadata().clone()));
        }
        result
    }

    fn collect_events_since(&self, since: &Since) -> Vec<Event<Self::Op>> {
        let mut result = self
            .first
            .collect_events_since(since)
            .into_iter()
            .map(|e| Event::new(Duet::First(e.op.clone()), e.metadata().clone()))
            .collect::<Vec<_>>();
        result.extend(
            self.second
                .collect_events_since(since)
                .into_iter()
                .map(|e| Event::new(Duet::Second(e.op.clone()), e.metadata().clone())),
        );
        result
    }

    fn r_n(&mut self, metadata: &Clock<Full>, conservative: bool) {
        self.first.r_n(metadata, conservative);
        self.second.r_n(metadata, conservative);
    }

    fn any_r(&self, event: &Event<Self::Op>) -> bool {
        match event.op {
            Duet::First(ref op) => {
                let first = Event::new(op.clone(), event.metadata().clone());
                self.first.any_r(&first)
            }
            Duet::Second(ref op) => {
                let second = Event::new(op.clone(), event.metadata().clone());
                self.second.any_r(&second)
            }
        }
    }

    fn eval(&self) -> Self::Value {
        (self.first.eval(), self.second.eval())
    }

    fn stabilize(&mut self, metadata: &Clock<Partial>) {
        self.first.stabilize(metadata);
        self.second.stabilize(metadata);
    }

    fn is_empty(&self) -> bool {
        self.first.is_empty() && self.second.is_empty()
    }

    fn size(&self) -> usize {
        self.first.size() + self.second.size()
    }

    fn deps(
        &self,
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
