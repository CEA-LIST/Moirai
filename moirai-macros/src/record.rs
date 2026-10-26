/// Defines a record CRDT whose fields are nested logs.
///
/// The generic form lets callers choose the value projection for each field when querying the
/// record. The explicit form uses `Log => Value` fields and generates a concrete, nominal value
/// type.
///
/// A field may also use `field: Operation, Log => Value`, matching `union!`'s
/// variant syntax. `Operation` is the record variant's payload; `Boxer` converts
/// it to `Log::Op` when forwarding. This separates operation layout from log and
/// read-value layout, so recursive collection operations need no log adapter.
/// Existing field forms keep deriving their payload from `Log::Op`.
#[macro_export]
macro_rules! record {
    // The optional first type is an explicit operation payload. Without it,
    // derive the operation from the log as before. Normalize once so forwarding
    // needs no policy flags and large records need no recursive token parser.
    ($name:ident { $($field:ident : $T:ty $(, $L:ty)? => $V:ty),+ $(,)? }) => {
        $crate::record!(@impl [] $name {
            $($field: (
                $crate::record!(@op $T $(, $L)?),
                $crate::record!(@log $T $(, $L)?) => $V
            )),+
        });
        $crate::record!(@explicit_eval $name {
            $($field: $crate::record!(@log $T $(, $L)?) => $V),+
        });
    };
    // Without explicit values, preserve the generic value projection.
    ($name:ident { $($field:ident : $T:ty),* $(,)? }) => {
        $crate::paste::paste! {
            $crate::record!(@impl [$([<$field:camel Value>]),*] $name {
                $($field: (<$T as $crate::moirai_protocol::state::log::IsLog>::Op, $T => [<$field:camel Value>])),*
            });
            $crate::record!(@generic_eval [$([<$field:camel Value>]),*] $name {
                $($field: $T => [<$field:camel Value>]),*
            });
        }
    };
    (@op $O:ty, $L:ty) => { $O };
    (@op $L:ty) => { <$L as $crate::moirai_protocol::state::log::IsLog>::Op };
    (@log $O:ty, $L:ty) => { $L };
    (@log $L:ty) => { $L };
    (@impl [$($value_param:ident),*] $name:ident {
        $($field:ident : ($O:ty, $T:ty => $V:ty)),* $(,)?
    }) => {
        $crate::paste::paste! {
            /// Set of operations that can be applied to the record.
            /// Each operation corresponds to an operation on one of the fields, or a "New" operation to initialize the record.
            #[derive(Clone, Debug)]
            #[cfg_attr(feature = "serde",derive(serde::Serialize, serde::Deserialize))]
            pub enum $name {
                $(
                    [<$field:camel>]($O),
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
            #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
            pub struct [<$name Value>]<$($value_param),*> {
                $(
                    pub $field: $V,
                )*
            }

            /// Record log type, containing a log for each field.
            #[derive(Debug, Default, Clone)]
            pub struct [<$name Log>] {
                $(
                    $field: $T,
                )*
            }

            #[cfg(feature = "test_utils")]
            impl<$($value_param: ::deepsize::DeepSizeOf),*> ::deepsize::DeepSizeOf
                for [<$name Value>]<$($value_param),*>
            {
                fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> usize {
                    0 $(+ ::deepsize::DeepSizeOf::deep_size_of_children(&self.$field, context))*
                }
            }

            #[cfg(feature = "test_utils")]
            impl ::deepsize::DeepSizeOf for [<$name Log>] {
                fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> usize {
                    0 $(+ ::deepsize::DeepSizeOf::deep_size_of_children(&self.$field, context))*
                }
            }

            /// Accessor methods for each field log.
            impl [<$name Log>] {
                $(
                    pub fn $field(&self) -> &$T {
                        &self.$field
                    }
                )*

                #[doc(hidden)]
                pub fn default_sink_expansion(
                    &self,
                    ctx: &mut $crate::moirai_protocol::state::effect_context::EffectContext<'_>,
                ) {
                    ctx.create_typed(::std::stringify!($name));
                    $(
                        ctx.with_field(::std::stringify!($field), |ctx| {
                            <$T as $crate::moirai_protocol::state::log::DefaultSinkExpansion>::default_sink_expansion(
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
                                    Self::default_sink_expansion(self, ctx);
                                } else {
                                    ctx.update_typed(::std::stringify!($name));
                                }

                                let op: <$T as $crate::moirai_protocol::state::log::IsLog>::Op =
                                    <$O as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(op);
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
                            Self::default_sink_expansion(self, ctx);
                        }
                    }
                }

                fn stabilize(&mut self, frontier: &$crate::moirai_protocol::clock::causal_frontier::CausalFrontier) {
                    $(
                        <$T as $crate::moirai_protocol::state::log::IsLog>::stabilize(
                            &mut self.$field,
                            frontier,
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
                            $name::[<$field:camel>](o) => {
                                <$O as $crate::moirai_protocol::utils::boxer::Boxer<_>>::with_boxed(
                                    o,
                                    |op| <$T as $crate::moirai_protocol::state::log::IsLog>::is_enabled(
                                        &self.$field,
                                        op,
                                    ),
                                )
                                .map_err([<$name Rejection>]::[<$field:camel>])
                            },
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
    (@explicit_eval $name:ident {
        $($field:ident : $T:ty => $V:ty),* $(,)?
    }) => {
        $crate::paste::paste! {
            impl $crate::moirai_protocol::crdt::eval::EvalNested<
                $crate::moirai_protocol::crdt::query::Read<[<$name Value>]>
            > for [<$name Log>] {
                fn execute_query(
                    &self,
                    _q: &$crate::moirai_protocol::crdt::query::Read<[<$name Value>]>,
                ) -> [<$name Value>] {
                    [<$name Value>] {
                        $(
                            $field: <$T as $crate::moirai_protocol::crdt::eval::EvalNested<
                                $crate::moirai_protocol::crdt::query::Read<$V>
                            >>::execute_query(
                                &self.$field,
                                &$crate::moirai_protocol::crdt::query::Read::<$V>::new(),
                            ),
                        )*
                    }
                }
            }
        }
    };
    (@generic_eval [$($value_param:ident),*] $name:ident {
        $($field:ident : $T:ty => $V:ty),* $(,)?
    }) => {
        $crate::paste::paste! {
            impl<$($value_param),*>
                $crate::moirai_protocol::crdt::eval::EvalNested<
                    $crate::moirai_protocol::crdt::query::Read<
                        [<$name Value>]<$($value_param),*>
                    >
                > for [<$name Log>]
            where
                $(
                    $T: $crate::moirai_protocol::crdt::eval::EvalNested<
                        $crate::moirai_protocol::crdt::query::Read<$V>
                    >,
                )*
            {
                fn execute_query(
                    &self,
                    _q: &$crate::moirai_protocol::crdt::query::Read<
                        [<$name Value>]<$($value_param),*>
                    >,
                ) -> [<$name Value>]<$($value_param),*> {
                    [<$name Value>] {
                        $(
                            $field: <$T as $crate::moirai_protocol::crdt::eval::EvalNested<
                                $crate::moirai_protocol::crdt::query::Read<$V>
                            >>::execute_query(
                                &self.$field,
                                &$crate::moirai_protocol::crdt::query::Read::<$V>::new(),
                            ),
                        )*
                    }
                }
            }
        }
    };
}
