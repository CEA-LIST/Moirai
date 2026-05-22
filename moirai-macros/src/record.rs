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

            #[derive(Debug, Clone)]
            pub enum [<$name Rejection>] {
                $(
                    [<$field:camel>](<$T as $crate::moirai_protocol::state::log::IsLog>::Rejection),
                )*
                AlreadyInitialized,
            }

            impl std::fmt::Display for [<$name Rejection>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    match self {
                        $(
                            Self::[<$field:camel>](e) => write!(f, "{}: {}", stringify!($field), e),
                        )*
                        Self::AlreadyInitialized => write!(f, "Already initialized"),
                    }
                }
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
                type Rejection = [<$name Rejection>];

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
                    ctx: &mut $crate::moirai_protocol::state::effect_context::EffectContext<'_>)
                {
                    match event.op().clone() {
                        $(
                            $name::[<$field:camel>](op) => {
                                let is_default = <Self as $crate::moirai_protocol::state::log::IsLog>::is_default(self);

                                if is_default {
                                    self.default_sink_expansion(ctx);
                                } else {
                                    ctx.update();
                                }

                                let child_op = $crate::moirai_protocol::event::Event::unfold(event, op);
                                ctx.with_field(stringify!($field), |ctx| {
                                    if !is_default {
                                        ctx.update();
                                    }
                                    self.$field.effect(child_op, ctx);
                                });
                            }
                        )*
                        $name::New => {
                            self.default_sink_expansion(ctx);
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

                fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
                    match op {
                        $(
                            $name::[<$field:camel>](o) => self.$field.is_enabled(o).map_err(Self::Rejection::[<$field:camel>]),
                        )*
                        $name::New => if self.is_default() { Ok(()) } else { Err(Self::Rejection::AlreadyInitialized) },
                        _ => unreachable!(),
                    }
                }

                fn default_sink_expansion(
                    &self,
                    ctx: &mut $crate::moirai_protocol::state::effect_context::EffectContext<'_>,
                ) {
                    ctx.create();
                    $(
                        ctx.with_field(stringify!($field), |ctx| {
                            <$T as $crate::moirai_protocol::state::log::IsLog>::new()
                                .default_sink_expansion(ctx);
                        });
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
