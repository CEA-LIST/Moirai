// A macro to define a record type with multiple fields, each field being a different log type.
// It generates the necessary structures and implements the Log trait for the record.

#[macro_export]
macro_rules! record {
    ($name:ident { $($field:ident : $T:ty),* $(,)? }) => {
        $crate::paste::paste! {
            #[derive(Clone, Debug)]
            pub enum $name {
                $(
                    [<$field:camel>](<$T as $crate::moirai_protocol::state::log::IsLog>::Op),
                )*
                New,
            }

            #[derive(Debug, Clone, Default, PartialEq)]
            pub struct [<$name Value>] {
                $(
                    pub $field: <$T as $crate::moirai_protocol::state::log::IsLog>::Value,
                )*
            }

            #[derive(Debug, Default, Clone)]
            pub struct [<$name Log>] {
                __id: Option<$crate::moirai_protocol::event::id::EventId>,
                $(
                    pub $field: $T,
                )*
            }

            impl $crate::moirai_protocol::state::log::IsLog for [<$name Log>] {
                type Value = [<$name Value>];
                type Op = $name;

                fn new() -> Self {
                    Self {
                        __id: None,
                        $(
                            $field: <$T as $crate::moirai_protocol::state::log::IsLog>::new(),
                        )*
                    }
                }

                fn effect(&mut self, event: $crate::moirai_protocol::event::Event<Self::Op>) {
                    if self.__id.is_none() {
                        self.__id = Some(event.id().clone());
                    }

                    match event.op().clone() {
                        $(
                            $name::[<$field:camel>](op) => {
                                let child_op = $crate::moirai_protocol::event::Event::unfold(event, op);
                                self.$field.effect(child_op);
                            }
                        )*
                        $name::New => {}
                    }
                }

                fn stabilize(&mut self, version: &$crate::moirai_protocol::clock::version_vector::Version) {
                    $(
                        self.$field.stabilize(version);
                    )*
                }

                fn redundant_by_parent(&mut self, version: &$crate::moirai_protocol::clock::version_vector::Version, conservative: bool) {
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
                        $name::New => self.__id.is_none(),
                        _ => unreachable!(),
                    }
                }
            }

            impl $crate::moirai_protocol::crdt::eval::EvalNested<$crate::moirai_protocol::crdt::query::Read<<Self as $crate::moirai_protocol::state::log::IsLog>::Value>> for [<$name Log>] {
                fn execute_query(&self, _q: $crate::moirai_protocol::crdt::query::Read<<Self as $crate::moirai_protocol::state::log::IsLog>::Value>) -> [<$name Value>] {
                    [<$name Value>] {
                        $(
                            $field: self.$field.execute_query($crate::moirai_protocol::crdt::query::Read::new()),
                        )*
                    }
                }
            }

            impl $crate::moirai_protocol::crdt::eval::EvalNested<$crate::moirai_protocol::crdt::query::ReadId> for [<$name Log>] {
                fn execute_query(&self, _q: $crate::moirai_protocol::crdt::query::ReadId) -> Option<$crate::moirai_protocol::event::id::EventId> {
                    self.__id.clone()
                }
            }

            /// Evaluate a particular field of the record.
            // TODO: this impl is too strong, as it requires all fields to implement EvalNested<Q>
            impl<Q> $crate::moirai_protocol::crdt::eval::EvalNested<$crate::moirai_protocol::crdt::query::Get<String, Q>> for [<$name Log>]
            where
                Q: $crate::moirai_protocol::crdt::query::QueryOperation,
                $(
                    $T: $crate::moirai_protocol::crdt::eval::EvalNested<Q>,
                )*
            {
                fn execute_query(&self, q: $crate::moirai_protocol::crdt::query::Get<String, Q>) -> <$crate::moirai_protocol::crdt::query::Get<String, Q> as $crate::moirai_protocol::crdt::query::QueryOperation>::Response {
                    match q.key.as_str() {
                        $(
                            stringify!($field) => {
                                let field = &self.$field;
                                let response = <_ as $crate::moirai_protocol::crdt::eval::EvalNested<Q>>::execute_query(field, q.nested_query);
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
