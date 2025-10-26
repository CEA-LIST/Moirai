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
            // TODO: it seems that this impl is too strong, as it requires all fields to implement EvalNested<Q>
            // TODO: consider making it more flexible
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
