/////////////////////////////////////////////////////////////////////////////
//  Copyright (c) 2025, 2026 CEA LIST.
//
//  This program and the accompanying materials
//  are made available under the terms of the Apache License, Version 2.0
//  which accompanies this distribution, and is available at
//  https://www.apache.org/licenses/LICENSE-2.0
//
//  SPDX-License-Identifier: Apache-2.0
//
//  Contributors:
// Léo Olivier -  (CEA LIST) - Initial implementation
//
/////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! union {
    (
        $union:ident = $($variant:ident ($ty:ty, $log:ty => $value:ty))|+ $(,)?
    ) => {
        $crate::paste::paste! {
            /// List of variant names, used in the `Choose` operation to select a variant
            #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
            #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
            pub enum [<$union Variant>] {
                $(
                    $variant,
                )*
            }

            /// Set of Union CRDT operations
            #[derive(Clone, Debug)]
            #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
            pub enum $union {
                $(
                    $variant($ty),
                )*
                Choose([<$union Variant>]),
            }

            /// Set of Union CRDT child logs, one for each variant
            #[derive(Clone, Debug)]
            #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
            pub enum [<$union Child>] {
                $(
                    $variant($log),
                )*
            }

            #[cfg(feature = "test_utils")]
            impl ::deepsize::DeepSizeOf for [<$union Child>] {
                fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> usize {
                    match self {
                        $(Self::$variant(log) => ::deepsize::DeepSizeOf::deep_size_of_children(log, context),)*
                    }
                }
            }

            impl [<$union Child>] {
                /// Returns the variant name that this child log corresponds to.
                fn variant(&self) -> [<$union Variant>] {
                    match self {
                        $(
                            Self::$variant(_) => [<$union Variant>]::$variant,
                        )*
                    }
                }
            }

            /// Value returned by the child log of each variant
            #[derive(Clone, Debug)]
            #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
            pub enum [<$union ChildValue>] {
                $(
                    $variant($value),
                )*
            }

            #[cfg(feature = "test_utils")]
            impl ::deepsize::DeepSizeOf for [<$union ChildValue>] {
                fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> usize {
                    match self {
                        $(Self::$variant(value) => ::deepsize::DeepSizeOf::deep_size_of_children(value, context),)*
                    }
                }
            }

            /// Value returned by the union log, which may be a single value, a conflict of values, or unset.
            #[derive(Clone, Debug, Default, PartialEq, Eq)]
            #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
            #[cfg_attr(feature = "test_utils", derive(::deepsize::DeepSizeOf))]
            pub enum [<$union Value>] {
                #[default]
                Unset,
                Value(::std::boxed::Box<[<$union ChildValue>]>),
                Conflict(::std::vec::Vec<[<$union ChildValue>]>),
            }

            #[derive(Clone, Debug)]
            #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
            #[cfg_attr(feature = "test_utils", derive(::deepsize::DeepSizeOf))]
            struct [<$union Branch>] {
                child: [<$union Child>],
                // Wether the child log is currently active in the union.
                // `presence` contains at most one entry per replica.
                presence: ::std::collections::BTreeMap<
                    $crate::moirai_protocol::replica::ReplicaIdx,
                    $crate::moirai_protocol::clock::version_vector::Seq,
                >,
            }

            impl [<$union Branch>] {
                fn reset(
                    &mut self,
                    version: &$crate::moirai_protocol::clock::version_vector::Version,
                    conservative: ::std::primitive::bool,
                ) {
                    if conservative {
                        self.presence.retain(|&origin, seq| *seq > version.seq_by_idx(origin));
                    } else {
                        self.presence.clear();
                    }
                    match &mut self.child {
                        $(
                            [<$union Child>]::$variant(log) => {
                                <$log as $crate::moirai_protocol::state::log::IsLog>::redundant_by_parent(
                                    log, version, conservative,
                                );
                            }
                        )*
                    }
                }
            }

            /// Union log with at most one child log per variant.
            #[derive(Clone, Debug, Default)]
            #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
            #[cfg_attr(feature = "test_utils", derive(::deepsize::DeepSizeOf))]
            pub struct [<$union Log>] {
                children: ::std::collections::BTreeMap<[<$union Variant>], [<$union Branch>]>,
            }

            /// Rejection reasons for union operations
            /// i.e., when an operation is not enabled in the current state
            #[derive(Debug)]
            pub enum [<$union Rejection>] {
                WrongVariant,
                MissingVariant,
                NotConflict,
                $(
                    $variant(::std::boxed::Box<<$log as $crate::moirai_protocol::state::log::IsLog>::Rejection>),
                )*
            }

            impl [<$union Log>] {
                /// Whether the union currently has no visible variants.
                pub fn is_unset(&self) -> ::std::primitive::bool {
                    self.active_children().next().is_none()
                }

                /// Iterate over the currently present variants in declaration order.
                pub fn active_children(&self) -> impl ::std::iter::Iterator<Item = &[<$union Child>]> {
                    self.children
                        .values()
                        .filter(|branch| !branch.presence.is_empty())
                        .map(|branch| &branch.child)
                }
            }

            impl $crate::moirai_protocol::state::log::IsLog for [<$union Log>] {
                type Command = [<$union>];
                type Update = [<$union>];
                type Rejection = [<$union Rejection>];

                fn new() -> Self {
                    <Self as ::std::default::Default>::default()
                }

                fn prepare(&self, cmd: Self::Command) -> Self::Update {
                    cmd
                }

                fn is_enabled(&self, op: &Self::Update) -> ::std::result::Result<(), Self::Rejection> {
                    let mut children = self.active_children().peekable();
                    if let $union::Choose(choice) = op {
                        let mut selected = false;
                        let mut concurrent = false;
                        for child in children {
                            if child.variant() == *choice {
                                selected = true;
                            } else {
                                concurrent = true;
                            }
                        }
                        return match (selected, concurrent) {
                            (true, true) => ::std::result::Result::Ok(()),
                            (true, false) => ::std::result::Result::Err([<$union Rejection>]::NotConflict),
                            (false, _) => ::std::result::Result::Err([<$union Rejection>]::MissingVariant),
                        };
                    }

                    if children.peek().is_none() {
                        return ::std::result::Result::Ok(());
                    }

                    for child in children {
                        match (op, child) {
                            $(
                                ($union::$variant(o), [<$union Child>]::$variant(log)) => {
                                    let child_op =
                                        <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(
                                            ::std::clone::Clone::clone(o),
                                        );
                                    return <$log as $crate::moirai_protocol::state::log::IsLog>::is_enabled(
                                        log, &child_op,
                                    )
                                    .map_err(|error| [<$union Rejection>]::$variant(::std::boxed::Box::new(error)));
                                }
                            )*
                            _ => {}
                        }
                    }
                    ::std::result::Result::Err([<$union Rejection>]::WrongVariant)
                }

                fn effect(
                    &mut self,
                    event: $crate::moirai_protocol::event::Event<Self::Update>,
                    ctx: &mut $crate::moirai_protocol::state::effect_context::EffectContext<'_>,
                ) {
                    match ::std::clone::Clone::clone(event.op()) {
                        $(
                            $union::$variant(o) => {
                                let branch = self.children
                                    .entry([<$union Variant>]::$variant)
                                    .or_insert_with(|| [<$union Branch>] {
                                        child: [<$union Child>]::$variant(
                                            <$log as $crate::moirai_protocol::state::log::IsLog>::new(),
                                        ),
                                        presence: ::std::collections::BTreeMap::new(),
                                    });
                                #[allow(irrefutable_let_patterns)]
                                let [<$union Child>]::$variant(log) = &mut branch.child else {
                                    ::std::unreachable!("child log must match its variant");
                                };
                                let origin = event.id().idx();
                                let seq = event.id().seq();
                                ctx.with_variant(::std::stringify!([<$variant:lower>]), |ctx| {
                                    let child_op =
                                        <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o);
                                    let child_event = $crate::moirai_protocol::event::Event::unfold(event, child_op);
                                    <$log as $crate::moirai_protocol::state::log::IsLog>::effect(
                                        log, child_event, ctx,
                                    );
                                });
                                branch.presence.insert(origin, seq);
                            }
                        )*
                        $union::Choose(choice) => {
                            for (variant, branch) in &mut self.children {
                                if *variant != choice {
                                    branch.reset(event.version(), true);
                                }
                            }
                        }
                    }
                }

                fn stabilize(&mut self, frontier: &$crate::moirai_protocol::clock::causal_frontier::CausalFrontier) {
                    // Hidden children can still hold history needed by concurrent edits.
                    for branch in self.children.values_mut() {
                        match &mut branch.child {
                            $(
                                [<$union Child>]::$variant(log) => {
                                    <$log as $crate::moirai_protocol::state::log::IsLog>::stabilize(
                                        log, frontier,
                                    );
                                }
                            )*
                        }
                    }
                }

                fn redundant_by_parent(
                    &mut self,
                    version: &$crate::moirai_protocol::clock::version_vector::Version,
                    conservative: ::std::primitive::bool,
                ) {
                    for branch in self.children.values_mut() {
                        branch.reset(version, conservative);
                    }
                }

                fn is_default(&self) -> ::std::primitive::bool {
                    self.children.values().all(|branch| {
                        branch.presence.is_empty()
                            && match &branch.child {
                                $(
                                    [<$union Child>]::$variant(log) => {
                                        <$log as $crate::moirai_protocol::state::log::IsLog>::is_default(log)
                                    }
                                )*
                            }
                    })
                }
            }

            impl $crate::moirai_protocol::state::log::EvalNested<$crate::moirai_protocol::data_type::query::Read<[<$union Value>]>> for [<$union Log>] {
                fn execute_query(
                    &self,
                    _q: &$crate::moirai_protocol::data_type::query::Read<[<$union Value>]>,
                ) -> [<$union Value>] {
                    let values = self.active_children().map(|child| match child {
                        $(
                            [<$union Child>]::$variant(log) => {
                                [<$union ChildValue>]::$variant(
                                    $crate::moirai_protocol::state::log::EvalNested::execute_query(
                                        log,
                                        &$crate::moirai_protocol::data_type::query::Read::<$value>::new(),
                                    ),
                                )
                            }
                        )*
                    });
                    let mut values = values.peekable();
                    let ::std::option::Option::Some(first) = values.next() else {
                        return [<$union Value>]::Unset;
                    };
                    if values.peek().is_none() {
                        [<$union Value>]::Value(::std::boxed::Box::new(first))
                    } else {
                        // Each active variant appears once, in the same order as ChildValue::rank.
                        [<$union Value>]::Conflict(::std::iter::once(first).chain(values).collect())
                    }
                }
            }

            //* Deterministic ordering of child values */

            impl [<$union ChildValue>] {
                fn rank(&self) -> ::std::primitive::usize {
                    match self {
                        $(
                            Self::$variant(_) => [<$union Variant>]::$variant as ::std::primitive::usize,
                        )*
                    }
                }
            }

            impl ::std::cmp::PartialEq for [<$union ChildValue>] {
                fn eq(&self, other: &Self) -> ::std::primitive::bool {
                    match (self, other) {
                        $(
                            (Self::$variant(left), Self::$variant(right)) => left == right,
                        )*
                        _ => false,
                    }
                }
            }

            impl ::std::cmp::Eq for [<$union ChildValue>] {}

            impl ::std::cmp::PartialOrd for [<$union ChildValue>] {
                fn partial_cmp(&self, other: &Self) -> ::std::option::Option<::std::cmp::Ordering> {
                    ::std::option::Option::Some(<Self as ::std::cmp::Ord>::cmp(self, other))
                }
            }

            impl ::std::cmp::Ord for [<$union ChildValue>] {
                fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
                    <::std::primitive::usize as ::std::cmp::Ord>::cmp(
                        &self.rank(),
                        &other.rank(),
                    )
                }
            }

            //* Deepsize */

            #[cfg(feature = "test_utils")]
            impl ::deepsize::DeepSizeOf for [<$union Variant>] {
                fn deep_size_of_children(&self, _context: &mut ::deepsize::Context) -> ::std::primitive::usize {
                    0
                }
            }

            #[cfg(feature = "test_utils")]
            impl ::deepsize::DeepSizeOf for $union {
                fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> ::std::primitive::usize {
                    match self {
                        $(
                            Self::$variant(value) => ::deepsize::DeepSizeOf::deep_size_of_children(
                                value,
                                context,
                            ),
                        )*
                        Self::Choose(_) => 0,
                    }
                }
            }

            impl ::std::fmt::Display for [<$union Rejection>] {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    match self {
                        Self::WrongVariant => ::std::write!(f, "operation does not match the active union variant"),
                        Self::MissingVariant => ::std::write!(f, "chosen union variant is not currently set"),
                        Self::NotConflict => ::std::write!(f, "choose is only enabled when the union is in conflict"),
                        $(
                            Self::$variant(error) => ::std::write!(
                                f,
                                "{}: {}",
                                ::std::stringify!($variant),
                                error,
                            ),
                        )*
                    }
                }
            }
        }
    };
}
