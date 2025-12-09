// A macro to define a record type with multiple fields, each field being a different log type.
// It generates the necessary structures and implements the Log trait for the record.

#[macro_export]
macro_rules! record {
    ($name:ident { $($field:ident : $T:path),* $(,)? }) => {
        paste::paste! {
            #[derive(Clone, Debug)]
            pub enum $name {
                $(
                    [<$field:camel>](<$T as $crate::protocol::state::log::IsLog>::Op),
                )*
            }

            #[derive(Debug, Default, PartialEq)]
            pub struct [<$name Value>] {
                $(
                    pub $field: <$T as $crate::protocol::state::log::IsLog>::Value,
                )*
            }

            #[derive(Debug, Default, Clone)]
            pub struct [<$name Log>] {
                $(
                    pub $field: $T,
                )*
            }

            impl $crate::protocol::state::log::IsLog for [<$name Log>] {
                type Value = [<$name Value>];
                type Op = $name;

                fn new() -> Self {
                    Self {
                        $(
                            $field: <$T as $crate::protocol::state::log::IsLog>::new(),
                        )*
                    }
                }

                fn effect(&mut self, event: $crate::protocol::event::Event<Self::Op>) {
                    match event.op().clone() {
                        $(
                            $name::[<$field:camel>](op) => {
                                let child_op = $crate::protocol::event::Event::unfold(event, op);
                                self.$field.effect(child_op);
                            }
                        )*
                    }
                }

                fn stabilize(&mut self, version: &$crate::protocol::clock::version_vector::Version) {
                    $(
                        self.$field.stabilize(version);
                    )*
                }

                fn redundant_by_parent(&mut self, version: &$crate::protocol::clock::version_vector::Version, conservative: bool) {
                    $(
                        self.$field.redundant_by_parent(version, conservative);
                    )*
                }

                fn is_default(&self) -> bool {
                    $(
                        if !self.$field.is_default() {
                            return false;
                        }
                    )*
                    true
                }

                fn is_enabled(&self, op: &Self::Op) -> bool {
                    match op {
                        $(
                            $name::[<$field:camel>](o) => self.$field.is_enabled(o),
                        )*
                    }
                }
            }

            impl $crate::protocol::crdt::eval::EvalNested<$crate::protocol::crdt::query::Read<<Self as $crate::protocol::state::log::IsLog>::Value>> for [<$name Log>] {
                fn execute_query(&self, _q: $crate::protocol::crdt::query::Read<<Self as $crate::protocol::state::log::IsLog>::Value>) -> [<$name Value>] {
                    [<$name Value>] {
                        $(
                            $field: self.$field.execute_query($crate::protocol::crdt::query::Read::new()),
                        )*
                    }
                }
            }

            /// Evaluate a particular field of the record.
            // TODO: this impl is too strong, as it requires all fields to implement EvalNested<Q>
            impl<Q> $crate::protocol::crdt::eval::EvalNested<$crate::protocol::crdt::query::Get<String, Q>> for [<$name Log>]
            where
                Q: $crate::protocol::crdt::query::QueryOperation,
                $(
                    $T: $crate::protocol::crdt::eval::EvalNested<Q>,
                )*
            {
                fn execute_query(&self, q: $crate::protocol::crdt::query::Get<String, Q>) -> <$crate::protocol::crdt::query::Get<String, Q> as $crate::protocol::crdt::query::QueryOperation>::Response {
                    match q.key.as_str() {
                        $(
                            stringify!($field) => {
                                let field = &self.$field;
                                let response = <_ as $crate::protocol::crdt::eval::EvalNested<Q>>::execute_query(field, q.nested_query);
                                Some(response)
                            },
                        )*
                        _ => None,
                    }
                }
            }
        }
    };
}

#[macro_export]
macro_rules! make_union {
    (
        $union:ident = $($variant:ident ($ty:ty, $log:ty))|+ $(,)?
    ) => {
        paste::paste! {
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

            #[derive(Clone, Debug, PartialEq, Eq)]
            pub enum [<$union ChildValue>] {
                $(
                    $variant(<$log as $crate::protocol::state::log::IsLog>::Value),
                )*
            }

            impl [<$union ChildValue>] {
                fn key(&self) -> &'static str {
                    match self {
                        $(Self::$variant { .. } => stringify!($variant)),*
                    }
                }
            }

            impl Ord for [<$union ChildValue>] {
                fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                    self.key().cmp(other.key())
                }
            }

            impl PartialOrd for [<$union ChildValue>] {
                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                    Some(self.cmp(other))
                }
            }

            #[derive(Clone, Debug, PartialEq, Eq)]
            pub enum [<$union Value>] {
                Value([<$union ChildValue>]),
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
                child: [<$union Container>],
            }

            impl $crate::protocol::state::log::IsLog for [<$union Log>] {
                type Value = Option<[<$union Value>]>;
                type Op = [<$union>];

                fn new() -> Self {
                    Self::default()
                }

                fn is_enabled(&self, op: &Self::Op) -> bool {
                    match &self.child {
                        [<$union Container>]::Unset => true,
                        [<$union Container>]::Value(child) => op.is_match_log(child),
                        [<$union Container>]::Conflicts(children) => children
                            .iter()
                            .any(|child| op.is_match_log(child)),
                    }
                }

                fn effect(&mut self, event: $crate::protocol::event::Event<Self::Op>) {
                    match event.op().clone() {
                        $(
                            $union::$variant(o) => {
                                match &mut self.child {
                                    [<$union Container>]::Unset => {
                                        let log = {
                                            let mut log = $log::new();
                                            let child_op: <$log as IsLog>::Op = <$ty as $crate::utils::unboxer::Unboxer<_>>::unbox(o);
                                            let child_event = $crate::protocol::event::Event::unfold(event, child_op);
                                            log.effect(child_event);
                                            log
                                        };
                                        self.child = [<$union Container>]::Value(Box::new([<$union Child>]::$variant(log)));
                                    }
                                    [<$union Container>]::Value(existing_child) => {
                                        if let [<$union Child>]::$variant(existing_log) = existing_child.as_mut() {
                                            let child_event = $crate::protocol::event::Event::unfold(event, <$ty as $crate::utils::unboxer::Unboxer<_>>::unbox(o));
                                            existing_log.effect(child_event);
                                        } else {
                                            let mut new_children = vec![];
                                            new_children.push((**existing_child).clone());
                                            let log = {
                                                let mut log = $log::new();
                                                let child_event = $crate::protocol::event::Event::unfold(event, <$ty as $crate::utils::unboxer::Unboxer<_>>::unbox(o));
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
                                            let child_event = $crate::protocol::event::Event::unfold(event, <$ty as $crate::utils::unboxer::Unboxer<_>>::unbox(o));
                                            log.effect(child_event);
                                        } else {
                                            let log = {
                                                let mut log = $log::new();
                                                let child_event = $crate::protocol::event::Event::unfold(event, <$ty as $crate::utils::unboxer::Unboxer<_>>::unbox(o));
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

                fn stabilize(&mut self, _version: &$crate::protocol::clock::version_vector::Version) {}

                fn redundant_by_parent(&mut self, version: &$crate::protocol::clock::version_vector::Version, conservative: bool) {
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

            impl $crate::protocol::crdt::eval::EvalNested<$crate::protocol::crdt::query::Read<<Self as $crate::protocol::state::log::IsLog>::Value>> for [<$union Log>] {
                fn execute_query(
                    &self,
                    _q: $crate::protocol::crdt::query::Read<Self::Value>,
                ) -> <$crate::protocol::crdt::query::Read<Self::Value> as $crate::protocol::crdt::query::QueryOperation>::Response {
                    match &self.child {
                        [<$union Container>]::Unset => None,
                        [<$union Container>]::Value(child) => Some(match child.as_ref() {
                            $(
                                [<$union Child>]::$variant(log) => {
                                    let value = log.execute_query($crate::protocol::crdt::query::Read::new());
                                    [<$union Value>]::Value([<$union ChildValue>]::$variant(value))
                                }
                            )*
                        }),
                        [<$union Container>]::Conflicts(children) => {
                            let mut values = vec![];
                            for child in children {
                                let value = match child {
                                    $(
                                        [<$union Child>]::$variant(log) => {
                                            let v = log.execute_query($crate::protocol::crdt::query::Read::new());
                                            [<$union ChildValue>]::$variant(v)
                                        }
                                    )*
                                };
                                values.push(value);
                            }
                            values.sort();
                            Some([<$union Value>]::Conflict(values))
                        }
                    }
                }
            }
        }
    };
}
