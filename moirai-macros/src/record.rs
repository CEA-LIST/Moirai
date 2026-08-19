// A macro to define a record type with multiple fields, each field being a different log type.
// It generates the necessary structures and implements the Log trait for the record.

#[macro_export]
macro_rules! record {
    ($name:ident { $($field:ident : $T:ty),* $(,)? }) => {
        $crate::paste::paste! {
            /// Set of operations that can be applied to the record.
            /// Each operation corresponds to an operation on one of the fields, or a "New" operation to initialize the record.
            #[derive(Clone, Debug)]
            pub enum $name {
                $(
                    [<$field:camel>](<$T as $crate::moirai_protocol::state::log::IsLog>::Op),
                )*
                New,
            }

            #[cfg(feature = "test_utils")]
            impl ::deepsize::DeepSizeOf for $name {
                fn deep_size_of_children(
                    &self,
                    context: &mut ::deepsize::Context,
                ) -> ::std::primitive::usize {
                    match self {
                        $(
                            Self::[<$field:camel>](op) =>
                                ::deepsize::DeepSizeOf::deep_size_of_children(op, context),
                        )*
                        Self::New => 0,
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

            /// Record log type, containing a log for each field.
            #[derive(Debug, Default, Clone)]
            pub struct [<$name Log>] {
                $(
                    $field: $T,
                )*
            }

            /// Accessor methods for each field log.
            impl [<$name Log>] {
                $(
                    pub fn $field(&self) -> &$T {
                        &self.$field
                    }
                )*

                #[doc(hidden)]
                pub fn __moirai_default_sink_expansion(
                    &self,
                    ctx: &mut $crate::moirai_protocol::state::effect_context::EffectContext<'_>,
                ) {
                    ctx.create_typed(::std::stringify!($name));
                    $(
                        ctx.with_field(::std::stringify!($field), |ctx| {
                            <$T as $crate::moirai_protocol::state::log::__DefaultSinkExpansion>::default_sink_expansion(
                                &<$T as $crate::moirai_protocol::state::log::IsLog>::new(),
                                ctx,
                            );
                        });
                    )*
                }
            }

            /// Implementation of the Log trait for the record.
            /// No semantics are defined at the record level, all semantics are defined at the field level.
            /// The record just forwards operations to the corresponding field log.
            impl $crate::moirai_protocol::state::log::IsLog for [<$name Log>] {
                type Value = [<$name Value>];
                type Command = $name;
                type Op = $name;
                type Rejection = [<$name Rejection>];

                fn new() -> Self {
                    Self {
                        $(
                            $field: <$T as $crate::moirai_protocol::state::log::IsLog>::new(),
                        )*
                    }
                }

                fn prepare(&self, cmd: Self::Command) -> Self::Op {
                    cmd
                }

                fn effect(
                    &mut self,
                    event: $crate::moirai_protocol::event::Event<Self::Op>,
                    ctx: &mut $crate::moirai_protocol::state::effect_context::EffectContext<'_>)
                {
                    match ::std::clone::Clone::clone(event.op()) {
                        $(
                            $name::[<$field:camel>](op) => {
                                let is_default = <Self as $crate::moirai_protocol::state::log::IsLog>::is_default(self);

                                if is_default {
                                    Self::__moirai_default_sink_expansion(self, ctx);
                                } else {
                                    ctx.update_typed(::std::stringify!($name));
                                }

                                let child_op = $crate::moirai_protocol::event::Event::unfold(event, op);
                                ctx.with_field(::std::stringify!($field), |ctx| {
                                    if !is_default {
                                        ctx.update();
                                    }
                                    <$T as $crate::moirai_protocol::state::log::IsLog>::effect(
                                        &mut self.$field,
                                        child_op,
                                        ctx,
                                    );
                                });
                            }
                        )*
                        $name::New => {
                            Self::__moirai_default_sink_expansion(self, ctx);
                        }
                    }
                }

                fn stabilize(&mut self, version: &$crate::moirai_protocol::clock::version_vector::Version) {
                    $(
                        <$T as $crate::moirai_protocol::state::log::IsLog>::stabilize(
                            &mut self.$field,
                            version,
                        );
                    )*
                }

                fn redundant_by_parent(&mut self, version: &$crate::moirai_protocol::clock::version_vector::Version, conservative: ::std::primitive::bool) {
                    $(
                        <$T as $crate::moirai_protocol::state::log::IsLog>::redundant_by_parent(
                            &mut self.$field,
                            version,
                            conservative,
                        );
                    )*
                }

                fn is_default(&self) -> ::std::primitive::bool {
                    $(
                        if !<$T as $crate::moirai_protocol::state::log::IsLog>::is_default(
                            &self.$field,
                        ) {
                            return false;
                        }
                    )*
                    true
                }

                fn is_enabled(
                    &self,
                    op: &Self::Op,
                ) -> ::std::result::Result<(), Self::Rejection> {
                    match op {
                        $(
                            $name::[<$field:camel>](o) =>
                                <$T as $crate::moirai_protocol::state::log::IsLog>::is_enabled(
                                    &self.$field,
                                    o,
                                )
                                .map_err([<$name Rejection>]::[<$field:camel>]),
                        )*
                        // "New" can only be applied if the record is in its default state
                        $name::New => if <Self as $crate::moirai_protocol::state::log::IsLog>::is_default(self) {
                            ::std::result::Result::Ok(())
                        } else {
                            ::std::result::Result::Err([<$name Rejection>]::AlreadyInitialized)
                        },
                        _ => ::std::unreachable!(),
                    }
                }

            }

            impl $crate::moirai_protocol::crdt::eval::EvalNested<$crate::moirai_protocol::crdt::query::Read<<Self as $crate::moirai_protocol::state::log::IsLog>::Value>> for [<$name Log>] {
                fn execute_query(&self, _q: $crate::moirai_protocol::crdt::query::Read<<Self as $crate::moirai_protocol::state::log::IsLog>::Value>) -> [<$name Value>] {
                    [<$name Value>] {
                        $(
                            $field: $crate::moirai_protocol::crdt::eval::EvalNested::execute_query(
                                &self.$field,
                                $crate::moirai_protocol::crdt::query::Read::new(),
                            ),
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

            impl ::std::fmt::Display for [<$name Rejection>] {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    match self {
                        $(
                            Self::[<$field:camel>](e) => ::std::write!(
                                f,
                                "{}: {}",
                                ::std::stringify!($field),
                                e,
                            ),
                        )*
                        Self::AlreadyInitialized => ::std::write!(f, "Already initialized"),
                    }
                }
            }
        }
    };
}
