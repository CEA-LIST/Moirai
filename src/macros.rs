#[macro_export]
macro_rules! object {
    ($name:ident { $($field:ident : $T:path),* $(,)? }) => {
        paste::paste! {
            #[derive(Clone, Debug)]
            pub enum $name {
                $(
                    [<$field:camel>](< $T as $crate::protocol::log::Log>::Op),
                )*
            }

            #[derive(Clone, Debug, Default)]
            pub struct [<$name Value>] {
                $(
                    pub $field: <$T as $crate::protocol::log::Log>::Value,
                )*
            }

            #[derive(Clone, Debug, Default)]
            pub struct [<$name Log>] {
                $(
                    pub $field: $T,
                )*
            }

            impl $crate::protocol::log::Log for [<$name Log>] {
                type Value = [<$name Value>];
                type Op = $name;

                fn new() -> Self {
                    Self {
                        $(
                            $field: $T::new(),
                        )*
                    }
                }

                fn new_event(&mut self, event: &$crate::protocol::event::Event<Self::Op>) {
                    match &event.op {
                        $(
                            $name::[<$field:camel>](ref op) => {
                                let ev = $crate::protocol::event::Event::new_nested(op.clone(), event.metadata.clone(), event.lamport());
                                self.$field.new_event(&ev);
                            }
                        )*
                    }
                }

                fn prune_redundant_events(&mut self, event: &$crate::protocol::event::Event<Self::Op>, is_r_0: bool, ltm: &$crate::clocks::matrix_clock::MatrixClock) {
                    match &event.op {
                        $(
                            $name::[<$field:camel>](op) => {
                                let ev = $crate::protocol::event::Event::new(op.clone(), event.metadata().clone(), event.lamport());
                                self.$field.prune_redundant_events(&ev, is_r_0, ltm);
                            }
                        )*
                    }
                }

                fn purge_stable_metadata(&mut self, dot: &$crate::clocks::dot::Dot) {
                    $(
                        self.$field.purge_stable_metadata(dot);
                    )*
                }

                fn stable_by_clock(&mut self, clock: &$crate::clocks::clock::Clock<$crate::clocks::clock::Full>) {
                    $(
                        self.$field.stable_by_clock(clock);
                    )*
                }

                fn collect_events_since(&self, since: &$crate::protocol::pulling::Since, ltm: &$crate::clocks::matrix_clock::MatrixClock) -> Vec<$crate::protocol::event::Event<Self::Op>> {
                    let mut result = Vec::new();
                    $(
                        result.extend(
                            self.$field
                                .collect_events_since(since, ltm)
                                .into_iter()
                                .map(|e| $crate::protocol::event::Event::new_nested(
                                    $name::[<$field:camel>](e.op.clone()),
                                    e.metadata.clone(),
                                    e.lamport(),
                                ))
                        );
                    )*
                    result
                }

                fn clock_from_event(&self, event: &$crate::protocol::event::Event<Self::Op>) -> $crate::clocks::clock::Clock<$crate::clocks::clock::Full> {
                    match &event.op {
                        $(
                            $name::[<$field:camel>](op) => {
                                self.$field.clock_from_event(
                                    &$crate::protocol::event::Event::new(op.clone(), event.metadata().clone(), event.lamport())
                                )
                            }
                        )*
                    }
                }

                fn r_n(&mut self, metadata: &$crate::clocks::clock::Clock<$crate::clocks::clock::Full>, conservative: bool) {
                    $(
                        self.$field.r_n(metadata, conservative);
                    )*
                }

                fn redundant_itself(&self, event: &$crate::protocol::event::Event<Self::Op>) -> bool {
                    match &event.op {
                        $(
                            $name::[<$field:camel>](op) => {
                                let ev = $crate::protocol::event::Event::new(op.clone(), event.metadata().clone(), event.lamport());
                                self.$field.redundant_itself(&ev)
                            }
                        )*
                    }
                }

                fn eval(&self) -> Self::Value {
                    [<$name Value>] {
                        $(
                            $field: self.$field.eval(),
                        )*
                    }
                }

                fn stabilize(&mut self, dot: &$crate::clocks::dot::Dot) {
                    $(
                        self.$field.stabilize(dot);
                    )*
                }

                fn is_empty(&self) -> bool {
                    true $(&& self.$field.is_empty())*
                }

                fn deps(
                    &mut self,
                    clocks: &mut std::collections::VecDeque<$crate::clocks::clock::Clock<$crate::clocks::clock::Partial>>,
                    view: &std::rc::Rc<$crate::protocol::membership::ViewData>,
                    dot: &$crate::clocks::dot::Dot,
                    op: &Self::Op,
                ) {
                    match op {
                        $(
                            $name::[<$field:camel>](ref op) => {
                                self.$field.deps(clocks, view, dot, op);
                            }
                        )*
                    }
                }
            }
        }
    };
}
