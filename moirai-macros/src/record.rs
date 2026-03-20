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

            impl $crate::moirai_protocol::utils::translate_ids::TranslateIds for $name {
                fn translate_ids(&self, from: $crate::moirai_protocol::replica::ReplicaIdx, interner: &$crate::moirai_protocol::utils::intern_str::Interner) -> Self {
                    match self {
                        $(
                            Self::[<$field:camel>](o) => Self::[<$field:camel>](o.translate_ids(from, interner)),
                        )*
                        Self::New => Self::New,
                    }
                }
            }

            #[derive(Debug, Clone, Default, PartialEq)]
            pub struct [<$name Value>] {
                $(
                    pub $field: <$T as $crate::moirai_protocol::state::log::IsLog>::Value,
                )*
            }

            #[derive(Debug, Default, Clone)]
            pub struct [<$name Log>] {
                $(
                    $field: $T,
                )*
            }

            impl [<$name Log>] {
                $(
                        pub fn $field(&self) -> &$T {
                        &self.$field
                    }
                )*
            }

            impl $crate::moirai_protocol::state::log::IsLog for [<$name Log>] {
                type Value = [<$name Value>];
                type Op = $name;

                fn new() -> Self {
                    Self {
                        $(
                            $field: <$T as $crate::moirai_protocol::state::log::IsLog>::new(),
                        )*
                    }
                }

                fn effect(&mut self, event: $crate::moirai_protocol::event::Event<Self::Op>) {
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
                        $name::New => self.is_default(),
                        _ => unreachable!(),
                    }
                }
            }

            impl $crate::moirai_protocol::state::sink::IsLogSink for [<$name Log>] {
                fn effect_with_sink(
                    &mut self,
                    event: $crate::moirai_protocol::event::Event<Self::Op>,
                    path: $crate::moirai_protocol::state::sink::ObjectPath,
                    sink: &mut $crate::moirai_protocol::state::sink::SinkCollector,
                ) {
                    match event.op().clone() {
                        $(
                            $name::[<$field:camel>](op) => {
                                sink.collect($crate::moirai_protocol::state::sink::Sink::update(path.clone()));
                                let path = path.field(stringify!($field));
                                sink.collect($crate::moirai_protocol::state::sink::Sink::update(path.clone()));
                                let child_op = $crate::moirai_protocol::event::Event::unfold(event, op);
                                self.$field.effect_with_sink(child_op, path, sink);
                            }
                        )*
                        $name::New => {
                            sink.collect($crate::moirai_protocol::state::sink::Sink::create(path.clone()));
                            // new initialize each field to default, so we need to emit a create for each field
                            $(
                                let child_path = path.clone().field(stringify!($field));
                                sink.collect($crate::moirai_protocol::state::sink::Sink::create(child_path));
                            )*
                        }
                    }
                }
            }

            impl $crate::moirai_protocol::crdt::query::IsSemanticallyEmpty for [<$name Value>]
            where
                $(
                    <$T as $crate::moirai_protocol::state::log::IsLog>::Value:
                        $crate::moirai_protocol::crdt::query::IsSemanticallyEmpty,
                )*
            {
                fn is_semantically_empty(&self) -> bool {
                    true $(
                        && self.$field.is_semantically_empty()
                    )*
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

            #[cfg(feature = "fuzz")]
            impl ::moirai_fuzz::metrics::FuzzMetrics for [<$name Log>] {
                fn structure_metrics(&self) -> ::moirai_fuzz::metrics::StructureMetrics {
                    ::moirai_fuzz::metrics::StructureMetrics::object([
                        $(
                            ::moirai_fuzz::metrics::FuzzMetrics::structure_metrics(&self.$field),
                        )*
                    ])
                }
            }

            /// Evaluate a particular field of the record.
            // TODO: this impl is too strong, as it requires all fields to implement EvalNested<Q>
            impl<Q> $crate::moirai_protocol::crdt::eval::EvalNested<$crate::moirai_protocol::crdt::query::Get<::std::string::String, Q>> for [<$name Log>]
            where
                Q: $crate::moirai_protocol::crdt::query::QueryOperation,
                $(
                    $T: $crate::moirai_protocol::crdt::eval::EvalNested<Q>,
                )*
            {
                fn execute_query(&self, q: $crate::moirai_protocol::crdt::query::Get<::std::string::String, Q>) -> <$crate::moirai_protocol::crdt::query::Get<::std::string::String, Q> as $crate::moirai_protocol::crdt::query::QueryOperation>::Response {
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
