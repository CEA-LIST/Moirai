#[macro_export]
macro_rules! union {
    (
        $union:ident = $($variant:ident ($ty:ty, $log:ty))|+ $(,)?
    ) => {
        $crate::paste::paste! {
            #[derive(Clone, Debug)]
            pub enum $union {
                $(
                    $variant($ty),
                )*
            }

            impl $union {
                fn is_match_log(&self, log: &[<$union Child>]) -> bool {
                    match (self, log) {
                        $(
                            (Self::$variant(_), [<$union Child>]::$variant(_)) => true,
                        )*
                        _ => false,
                    }
                }
            }

            #[derive(Clone, Debug)]
            pub enum [<$union Child>] {
                $(
                    $variant($log),
                )*
            }

            #[derive(Clone, Debug)]
            pub enum [<$union ChildValue>] {
                $(
                    $variant(<$log as $crate::moirai_protocol::state::log::IsLog>::Value),
                )*
            }

            #[repr(usize)]
            enum [<$union ChildValueRank>] {
                $(
                    $variant,
                )*
            }

            impl [<$union ChildValue>] {
                fn rank(&self) -> usize {
                    match self {
                        $(
                            Self::$variant(_) => [<$union ChildValueRank>]::$variant as usize,
                        )*
                    }
                }
            }

            impl PartialEq for [<$union ChildValue>] {
                fn eq(&self, other: &Self) -> bool {
                    self.rank() == other.rank()
                }
            }

            impl Eq for [<$union ChildValue>] {}

            impl PartialOrd for [<$union ChildValue>] {
                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                    Some(self.cmp(other))
                }
            }

            impl Ord for [<$union ChildValue>] {
                fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                    self.rank().cmp(&other.rank())
                }
            }

            #[derive(Clone, Debug, Default, PartialEq)]
            pub enum [<$union Value>] {
                #[default]
                Unset,
                Value(Box<[<$union ChildValue>]>),
                Conflict(Vec<[<$union ChildValue>]>),
            }

            #[derive(Clone, Debug, Default)]
            pub enum [<$union Container>] {
                #[default]
                Unset,
                Value(Box<[<$union Child>]>),
                Conflicts(Vec<[<$union Child>]>),
            }

            #[derive(Clone, Debug, Default)]
            pub struct [<$union Log>] {
                pub child: [<$union Container>],
            }

            impl $crate::moirai_protocol::state::log::IsLog for [<$union Log>] {
                type Value = [<$union Value>];
                type Op = [<$union>];

                fn new() -> Self {
                    Self::default()
                }

                fn is_enabled(&self, op: &Self::Op) -> bool {
                    match &self.child {
                        [<$union Container>]::Unset => true,
                        [<$union Container>]::Value(child) => match (op, child.as_ref()) {
                            $(
                                (
                                    $union::$variant(o),
                                    [<$union Child>]::$variant(log),
                                ) => {
                                    let child_op: <$log as $crate::moirai_protocol::state::log::IsLog>::Op =
                                        <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o.clone());
                                    log.is_enabled(&child_op)
                                }
                            )*
                            _ => false,
                        },
                        [<$union Container>]::Conflicts(children) => children
                            .iter()
                            .any(|child| match (op, child) {
                                $(
                                    (
                                        $union::$variant(o),
                                        [<$union Child>]::$variant(log),
                                    ) => {
                                        let child_op: <$log as $crate::moirai_protocol::state::log::IsLog>::Op =
                                            <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o.clone());
                                        log.is_enabled(&child_op)
                                    }
                                )*
                                _ => false,
                            }),
                    }
                }

                fn effect(&mut self, event: $crate::moirai_protocol::event::Event<Self::Op>) {
                    match event.op().clone() {
                        $(
                            $union::$variant(o) => {
                                match &mut self.child {
                                    [<$union Container>]::Unset => {
                                        let log = {
                                            let mut log = $log::new();
                                            let child_op: <$log as $crate::moirai_protocol::state::log::IsLog>::Op = <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o);
                                            let child_event = $crate::moirai_protocol::event::Event::unfold(event, child_op);
                                            log.effect(child_event);
                                            log
                                        };
                                        self.child = [<$union Container>]::Value(Box::new([<$union Child>]::$variant(log)));
                                    }
                                    [<$union Container>]::Value(existing_child) => {
                                        if let [<$union Child>]::$variant(existing_log) = existing_child.as_mut() {
                                            let child_event = $crate::moirai_protocol::event::Event::unfold(event, <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o));
                                            existing_log.effect(child_event);
                                        } else {
                                            let mut new_children = vec![];
                                            new_children.push((**existing_child).clone());
                                            let log = {
                                                let mut log = $log::new();
                                                let child_event = $crate::moirai_protocol::event::Event::unfold(event, <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o));
                                                log.effect(child_event);
                                                log
                                            };
                                            new_children.push([<$union Child>]::$variant(log));
                                            self.child = [<$union Container>]::Conflicts(new_children);
                                        }
                                    }
                                    [<$union Container>]::Conflicts(children) => {
                                        if let Some([<$union Child>]::$variant(log)) = children
                                            .iter_mut()
                                            .find(|c| matches!(c, [<$union Child>]::$variant(_)))
                                        {
                                            let child_event = $crate::moirai_protocol::event::Event::unfold(event, <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o));
                                            log.effect(child_event);
                                        } else {
                                            let log = {
                                                let mut log = $log::new();
                                                let child_event = $crate::moirai_protocol::event::Event::unfold(event, <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o));
                                                log.effect(child_event);
                                                log
                                            };
                                            children.push([<$union Child>]::$variant(log));
                                        }
                                    }
                                }
                            }
                        )*
                    }
                }

                fn stabilize(&mut self, _version: &$crate::moirai_protocol::clock::version_vector::Version) {}

                fn redundant_by_parent(&mut self, version: &$crate::moirai_protocol::clock::version_vector::Version, conservative: bool) {
                    match &mut self.child {
                        [<$union Container>]::Unset => {}
                        [<$union Container>]::Value(union_child) => match union_child.as_mut() {
                            $(
                                [<$union Child>]::$variant(log) => {
                                    log.redundant_by_parent(version, conservative);
                                }
                            )*
                        },
                        [<$union Container>]::Conflicts(union_childs) => {
                            for union_child in union_childs {
                                match union_child {
                                    $(
                                        [<$union Child>]::$variant(log) => {
                                            log.redundant_by_parent(version, conservative);
                                        }
                                    )*
                                }
                            }
                        }
                    }
                }

                fn is_default(&self) -> bool {
                    matches!(self.child, [<$union Container>]::Unset)
                }
            }

            impl $crate::moirai_protocol::crdt::eval::EvalNested<$crate::moirai_protocol::crdt::query::Read<<Self as $crate::moirai_protocol::state::log::IsLog>::Value>> for [<$union Log>] {
                fn execute_query(
                    &self,
                    _q: $crate::moirai_protocol::crdt::query::Read<Self::Value>,
                ) -> <$crate::moirai_protocol::crdt::query::Read<Self::Value> as $crate::moirai_protocol::crdt::query::QueryOperation>::Response {
                    match &self.child {
                        [<$union Container>]::Unset => [<$union Value>]::Unset,
                        [<$union Container>]::Value(child) => match child.as_ref() {
                            $(
                                [<$union Child>]::$variant(log) => {
                                    let value = log.execute_query($crate::moirai_protocol::crdt::query::Read::new());
                                    [<$union Value>]::Value(Box::new([<$union ChildValue>]::$variant(value)))
                                }
                            )*
                        },
                        [<$union Container>]::Conflicts(children) => {
                            let mut values = vec![];
                            for child in children {
                                let value = match child {
                                    $(
                                        [<$union Child>]::$variant(log) => {
                                            let v = log.execute_query($crate::moirai_protocol::crdt::query::Read::new());
                                            [<$union ChildValue>]::$variant(v)
                                        }
                                    )*
                                };
                                values.push(value);
                            }
                            values.sort();
                            [<$union Value>]::Conflict(values)
                        }
                    }
                }
            }
        }
    };
}
