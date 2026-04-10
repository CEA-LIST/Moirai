// A macro to define a record type with multiple fields, each field being a different log type.
// It generates the necessary structures and implements the Log trait for the record.

#[macro_export]
macro_rules! record {
    ($name:ident { $($field:ident : $T:ty),* $(,)? }) => {
        $crate::paste::paste! {
            #[derive(Clone, Debug)]
            #[cfg_attr(feature = "test_utils", derive(::deepsize::DeepSizeOf))]
            pub enum $name {
                $(
                    [<$field:camel>](<$T as $crate::moirai_protocol::state::log::IsLog>::Op),
                )*
                New,
            }

            impl $crate::moirai_protocol::utils::intern_str::InternalizeOp for $name {
                fn internalize(self, interner: &$crate::moirai_protocol::utils::intern_str::Interner) -> Self {
                    match self {
                        $(
                            Self::[<$field:camel>](o) => Self::[<$field:camel>](o.internalize(interner)),
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

                fn effect(
                    &mut self,
                    event: $crate::moirai_protocol::event::Event<Self::Op>,
                    #[cfg(feature = "sink")]
                    path: $crate::moirai_protocol::state::object_path::ObjectPath,
                    #[cfg(feature = "sink")]
                    sink: &mut $crate::moirai_protocol::state::sink::SinkCollector,
                    #[cfg(feature = "sink")]
                    ownership: $crate::moirai_protocol::state::sink::SinkOwnership)
                {
                    match event.op().clone() {
                        $(
                            $name::[<$field:camel>](op) => {
                                #[cfg(feature = "sink")]
                                let is_default = <Self as $crate::moirai_protocol::state::log::IsLog>::is_default(self);
                                #[cfg(feature = "sink")] {
                                    if is_default {
                                        self.default_sink_expansion(path.clone(), sink);
                                    } else {
                                        sink.collect($crate::moirai_protocol::state::sink::Sink::update(path.clone()));
                                    }
                                }
                                #[cfg(feature = "sink")]
                                let path = path.field(stringify!($field));
                                #[cfg(feature = "sink")]
                                if !is_default {
                                    sink.collect($crate::moirai_protocol::state::sink::Sink::update(path.clone()));
                                }
                                let child_op = $crate::moirai_protocol::event::Event::unfold(event, op);
                                self.$field.effect(child_op, #[cfg(feature = "sink")] path, #[cfg(feature = "sink")] sink, #[cfg(feature = "sink")] $crate::moirai_protocol::state::sink::SinkOwnership::Owned);
                            }
                        )*
                        $name::New => {
                            #[cfg(feature = "sink")] {
                                self.default_sink_expansion(path.clone(), sink);
                            }
                        }
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

                #[cfg(feature = "sink")]
                fn default_sink_expansion(
                    &self,
                    path: $crate::moirai_protocol::state::object_path::ObjectPath,
                    sink: &mut $crate::moirai_protocol::state::sink::SinkCollector,
                ) {
                    sink.collect($crate::moirai_protocol::state::sink::Sink::create(path.clone()));
                    $(
                        <$T as $crate::moirai_protocol::state::log::IsLog>::new().default_sink_expansion(
                            path.clone().field(stringify!($field)),
                            sink,
                        );
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
        }
    };
}
