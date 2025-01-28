use crate::protocol::{event::Event, log::Log, metadata::Metadata};

#[derive(Clone, Debug)]
enum Duet<F, S> {
    First(F),
    Second(S),
}

#[derive(Clone, Debug, Default)]
struct DuetLog<Fl, Sl> {
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

    fn new_event(&mut self, event: &Event<Self::Op>) {
        match event.op {
            Duet::First(ref op) => {
                let event = Event::new(op.clone(), event.metadata.clone());
                self.first.new_event(&event);
            }
            Duet::Second(ref op) => {
                let event = Event::new(op.clone(), event.metadata.clone());
                self.second.new_event(&event);
            }
        }
    }

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool) {
        match &event.op {
            Duet::First(op) => {
                let event = Event::new(op.clone(), event.metadata.clone());
                self.first.prune_redundant_events(&event, is_r_0);
            }
            Duet::Second(op) => {
                let event = Event::new(op.clone(), event.metadata.clone());
                self.second.prune_redundant_events(&event, is_r_0);
            }
        }
    }

    fn purge_stable_metadata(&mut self, metadata: &Metadata) {
        self.first.purge_stable_metadata(metadata);
        self.second.purge_stable_metadata(metadata);
    }

    fn collect_events(&self, upper_bound: &Metadata) -> Vec<Event<Self::Op>> {
        let events_fl = self.first.collect_events(upper_bound);
        let events_sl = self.second.collect_events(upper_bound);
        let mut result = vec![];
        for e in events_fl {
            result.push(Event::new(Duet::First(e.op), e.metadata));
        }
        for e in events_sl {
            result.push(Event::new(Duet::Second(e.op), e.metadata));
        }
        result
    }

    fn any_r(&self, event: &Event<Self::Op>) -> bool {
        match event.op {
            Duet::First(ref op) => {
                let first = Event::new(op.clone(), event.metadata.clone());
                self.first.any_r(&first)
            }
            Duet::Second(ref op) => {
                let second = Event::new(op.clone(), event.metadata.clone());
                self.second.any_r(&second)
            }
        }
    }

    fn eval(&self) -> Self::Value {
        (self.first.eval(), self.second.eval())
    }

    fn stabilize(&mut self, metadata: &Metadata) {
        self.first.stabilize(metadata);
        self.second.stabilize(metadata);
    }
}
