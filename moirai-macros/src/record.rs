// A macro to define a record type with multiple fields, each field being a different log type.
// It generates the necessary structures and implements the Log trait for the record.

#[macro_export]
macro_rules! record {
    ($name:ident { $($field:ident : $T:ty),* $(,)? }) => {
        $crate::paste::paste! {
            /// Set of operations that can be applied to the record.
            /// Each operation corresponds to an operation on one of the fields, or a "New" operation to initialize the record.
            #[derive(Clone, Debug)]
            #[cfg_attr(feature = "test_utils", derive(::deepsize::DeepSizeOf))]
            pub enum $name {
                $(
                    [<$field:camel>](<$T as $crate::moirai_protocol::state::log::IsLog>::Op),
                )*
                New,
            }

            /// Internalize an operation, i.e., convert any contained event id from a remote replica into the local interner's mapping.
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

            /// Returned value when reading the record, containing the values of all fields.
            #[derive(Debug, Clone, Default, PartialEq)]
            pub struct [<$name Value>] {
                $(
                    pub $field: <$T as $crate::moirai_protocol::state::log::IsLog>::Value,
                )*
            }

            /// Record log type, containing a log for each field and a cache store for the read value.
            #[derive(Debug, Default, Clone)]
            pub struct [<$name Log>] {
                $(
                    $field: $T,
                )*
                __moirai_read_cache: $crate::moirai_protocol::state::cache::CacheCell<[<$name Value>]>,
            }

            /// Accessor methods for each field log.
            impl [<$name Log>] {
                $(
                        pub fn $field(&self) -> &$T {
                        &self.$field
                    }
                )*
            }

            /// Implementation of the Log trait for the record.
            /// No semantics are defined at the record level, all semantics are defined at the field level.
            /// The record just forwards operations to the corresponding field log.
            impl $crate::moirai_protocol::state::log::IsLog for [<$name Log>] {
                type Value = [<$name Value>];
                type Op = $name;
                type Rejection = [<$name Rejection>];

                fn new() -> Self {
                    Self {
                        $(
                            $field: <$T as $crate::moirai_protocol::state::log::IsLog>::new(),
                        )*
                        __moirai_read_cache: $crate::moirai_protocol::state::cache::CacheCell::new(),
                    }
                }

                fn effect(
                    &mut self,
                    event: $crate::moirai_protocol::event::Event<Self::Op>,
                    ctx: &mut $crate::moirai_protocol::state::effect_context::EffectContext<'_>)
                {
                    self.__moirai_read_cache.invalidate();
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
                    self.__moirai_read_cache.invalidate();
                    $(
                        self.$field.stabilize(version);
                    )*
                }

                fn redundant_by_parent(&mut self, version: &$crate::moirai_protocol::clock::version_vector::Version, conservative: bool) {
                    self.__moirai_read_cache.invalidate();
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
                        // "New" can only be applied if the record is in its default state
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

            impl $crate::moirai_protocol::crdt::eval::EvalNested<$crate::moirai_protocol::crdt::query::Read<<Self as $crate::moirai_protocol::state::log::IsLog>::Value>> for [<$name Log>]
            where
                $(
                    $T: $crate::moirai_protocol::crdt::eval::BorrowedRead,
                    <$T as $crate::moirai_protocol::state::log::IsLog>::Value: Clone,
                )*
            {
                fn execute_query(&self, _q: $crate::moirai_protocol::crdt::query::Read<<Self as $crate::moirai_protocol::state::log::IsLog>::Value>) -> [<$name Value>] {
                    $crate::moirai_protocol::crdt::eval::BorrowedRead::read_ref(self).clone()
                }
            }

            impl $crate::moirai_protocol::crdt::eval::BorrowedRead for [<$name Log>]
            where
                $(
                    $T: $crate::moirai_protocol::crdt::eval::BorrowedRead,
                    <$T as $crate::moirai_protocol::state::log::IsLog>::Value: Clone,
                )*
            {
                fn read_ref(&self) -> &Self::Value {
                    self.__moirai_read_cache.get_or_compute(|| self.read_uncached())
                }
            }

            impl [<$name Log>]
            where
                $(
                    $T: $crate::moirai_protocol::crdt::eval::BorrowedRead,
                    <$T as $crate::moirai_protocol::state::log::IsLog>::Value: Clone,
                )*
            {
                fn read_uncached(&self) -> [<$name Value>] {
                    [<$name Value>] {
                        $(
                            $field: $crate::moirai_protocol::crdt::eval::BorrowedRead::read_ref(&self.$field).clone(),
                        )*
                    }
                }
            }

            /// Possible rejections when trying to apply an operation to the record, containing the rejections of all fields
            /// or an "AlreadyInitialized" rejection if trying to apply a "New" operation to an initialized record.
            #[derive(Debug)]
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
        }
    };
}
