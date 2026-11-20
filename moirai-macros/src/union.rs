#[macro_export]
macro_rules! union {
    (
        $union:ident = $($variant:ident ($ty:ty, $log:ty => $value:ty))|+ $(,)?
    ) => {
        $crate::paste::paste! {
            /// List of variant names, used in the `Choose` operation to select a variant
            #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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

            impl $union {
                /// Returns true if the given log corresponds to the same variant as this operation.
                fn is_match_log(&self, log: &[<$union Child>]) -> ::std::primitive::bool {
                    match (self, log) {
                        $(
                            (Self::$variant(_), [<$union Child>]::$variant(_)) => true,
                        )*
                        _ => false,
                    }
                }
            }

            /// Set of Union CRDT child logs, one for each variant
            #[derive(Clone, Debug)]
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
            #[derive(Clone, Debug, Default, PartialEq)]
            #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
            #[cfg_attr(feature = "test_utils", derive(::deepsize::DeepSizeOf))]
            pub enum [<$union Value>] {
                #[default]
                Unset,
                Value(::std::boxed::Box<[<$union ChildValue>]>),
                Conflict(::std::vec::Vec<[<$union ChildValue>]>),
            }

            /// Internal Union log state
            #[derive(Clone, Debug, Default)]
            #[cfg_attr(feature = "test_utils", derive(::deepsize::DeepSizeOf))]
            pub enum [<$union Container>] {
                #[default]
                Unset,
                Value(::std::boxed::Box<[<$union Child>]>),
                Conflicts(::std::vec::Vec<[<$union Child>]>),
            }

            /// Union log
            ///
            /// A variant's child is present while one of the events routed to it is not covered
            /// by a reset from the parent (e.g., the removal of a map key) or by a `Choose` of
            /// another variant. Once all of them are covered, the child is absent: it no longer
            /// counts for reads nor for enabling operations, but its log is kept, so that a
            /// concurrent update delivered later applies to the history the reset saw.
            #[derive(Clone, Debug, Default)]
            #[cfg_attr(feature = "test_utils", derive(::deepsize::DeepSizeOf))]
            pub struct [<$union Log>] {
                /// Present children
                pub child: [<$union Container>],
                /// Absent children, whose routed events have all been covered
                removed: ::std::vec::Vec<[<$union Child>]>,
                /// For each present child, the highest sequence number per origin of the events
                /// routed to it that no reset has covered yet
                uncovered: ::std::vec::Vec<(
                    [<$union Variant>],
                    $crate::moirai_protocol::clock::version_vector::Version,
                )>,
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
                fn child_is_default(child: &[<$union Child>]) -> ::std::primitive::bool {
                    match child {
                        $(
                            [<$union Child>]::$variant(log) => {
                                <$log as $crate::moirai_protocol::state::log::IsLog>::is_default(log)
                            }
                        )*
                    }
                }

                fn reset_child(
                    child: &mut [<$union Child>],
                    version: &$crate::moirai_protocol::clock::version_vector::Version,
                    conservative: ::std::primitive::bool,
                ) {
                    match child {
                        $(
                            [<$union Child>]::$variant(log) => {
                                <$log as $crate::moirai_protocol::state::log::IsLog>::redundant_by_parent(log, version, conservative);
                            }
                        )*
                    }
                }

                fn stabilize_child(
                    child: &mut [<$union Child>],
                    frontier: &$crate::moirai_protocol::clock::causal_frontier::CausalFrontier,
                ) {
                    match child {
                        $(
                            [<$union Child>]::$variant(log) => {
                                <$log as $crate::moirai_protocol::state::log::IsLog>::stabilize(log, frontier);
                            }
                        )*
                    }
                }

                /// Records that an event has been routed to the child of `variant`.
                fn record_routed(
                    &mut self,
                    variant: [<$union Variant>],
                    id: &$crate::moirai_protocol::event::id::EventId,
                ) {
                    let position = match self.uncovered.iter().position(|(v, _)| *v == variant) {
                        ::std::option::Option::Some(position) => position,
                        ::std::option::Option::None => {
                            self.uncovered.push((
                                variant,
                                $crate::moirai_protocol::clock::version_vector::Version::new(
                                    id.idx(),
                                    ::std::clone::Clone::clone(id.resolver()),
                                ),
                            ));
                            self.uncovered.len() - 1
                        }
                    };
                    let uncovered = &mut self.uncovered[position].1;
                    if uncovered.seq_by_idx(id.idx()) < id.seq() {
                        uncovered.set_by_idx(id.idx(), id.seq());
                    }
                }

                /// Forgets the uncovered events that `version` covers.
                /// Events of one origin are delivered in order, so `version` covers every uncovered
                /// event of an origin iff it covers the highest one.
                fn forget_covered(
                    uncovered: &mut $crate::moirai_protocol::clock::version_vector::Version,
                    version: &$crate::moirai_protocol::clock::version_vector::Version,
                ) {
                    let covered: ::std::vec::Vec<_> = uncovered
                        .iter()
                        .filter(|(idx, seq)| *seq > 0 && version.seq_by_idx(*idx) >= *seq)
                        .map(|(idx, _)| idx)
                        .collect();
                    for idx in covered {
                        uncovered.set_by_idx(idx, 0);
                    }
                }

                fn present_mut(&mut self, variant: [<$union Variant>]) -> ::std::option::Option<&mut [<$union Child>]> {
                    match &mut self.child {
                        [<$union Container>]::Unset => ::std::option::Option::None,
                        [<$union Container>]::Value(child) => {
                            if child.variant() == variant {
                                ::std::option::Option::Some(child.as_mut())
                            } else {
                                ::std::option::Option::None
                            }
                        }
                        [<$union Container>]::Conflicts(children) => {
                            children.iter_mut().find(|child| child.variant() == variant)
                        }
                    }
                }

                fn insert_present(&mut self, child: [<$union Child>]) {
                    self.child = match ::std::mem::take(&mut self.child) {
                        [<$union Container>]::Unset => [<$union Container>]::Value(::std::boxed::Box::new(child)),
                        [<$union Container>]::Value(existing) => {
                            [<$union Container>]::Conflicts(::std::vec![*existing, child])
                        }
                        [<$union Container>]::Conflicts(mut children) => {
                            children.push(child);
                            [<$union Container>]::Conflicts(children)
                        }
                    };
                }

                fn take_present(&mut self, variant: [<$union Variant>]) -> ::std::option::Option<[<$union Child>]> {
                    let (container, taken) = match ::std::mem::take(&mut self.child) {
                        [<$union Container>]::Unset => ([<$union Container>]::Unset, ::std::option::Option::None),
                        [<$union Container>]::Value(child) => {
                            if child.variant() == variant {
                                ([<$union Container>]::Unset, ::std::option::Option::Some(*child))
                            } else {
                                ([<$union Container>]::Value(child), ::std::option::Option::None)
                            }
                        }
                        [<$union Container>]::Conflicts(mut children) => {
                            let taken = children
                                .iter()
                                .position(|child| child.variant() == variant)
                                .map(|position| children.remove(position));
                            let container = match children.len() {
                                0 => [<$union Container>]::Unset,
                                1 => [<$union Container>]::Value(::std::boxed::Box::new(children.remove(0))),
                                _ => [<$union Container>]::Conflicts(children),
                            };
                            (container, taken)
                        }
                    };
                    self.child = container;
                    taken
                }

                fn take_removed(&mut self, variant: [<$union Variant>]) -> ::std::option::Option<[<$union Child>]> {
                    self.removed
                        .iter()
                        .position(|child| child.variant() == variant)
                        .map(|position| self.removed.remove(position))
                }

                /// Resets the children of every variant but `kept`, present or absent, at `version`.
                /// If the reset is conservative, a present child whose routed events are now all
                /// covered becomes absent.
                fn reset_children(
                    &mut self,
                    version: &$crate::moirai_protocol::clock::version_vector::Version,
                    conservative: ::std::primitive::bool,
                    kept: ::std::option::Option<[<$union Variant>]>,
                ) {
                    let is_reset = |variant: [<$union Variant>]| kept != ::std::option::Option::Some(variant);
                    match &mut self.child {
                        [<$union Container>]::Unset => {}
                        [<$union Container>]::Value(child) => {
                            if is_reset(child.variant()) {
                                Self::reset_child(child, version, conservative);
                            }
                        }
                        [<$union Container>]::Conflicts(children) => {
                            for child in children.iter_mut().filter(|child| is_reset(child.variant())) {
                                Self::reset_child(child, version, conservative);
                            }
                        }
                    }
                    for child in self.removed.iter_mut().filter(|child| is_reset(child.variant())) {
                        Self::reset_child(child, version, conservative);
                    }
                    if !conservative {
                        return;
                    }

                    let mut covered = ::std::vec::Vec::new();
                    for (variant, uncovered) in self.uncovered.iter_mut().filter(|(variant, _)| is_reset(*variant)) {
                        Self::forget_covered(uncovered, version);
                        if uncovered.sum() == 0 {
                            covered.push(*variant);
                        }
                    }
                    self.uncovered.retain(|(variant, _)| !covered.contains(variant));
                    for variant in covered {
                        if let ::std::option::Option::Some(child) = self.take_present(variant) {
                            self.removed.push(child);
                        }
                    }
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
                    match &self.child {
                        [<$union Container>]::Unset => match op {
                            $union::Choose(_) => ::std::result::Result::Err([<$union Rejection>]::MissingVariant),
                            _ => ::std::result::Result::Ok(()),
                        },
                        [<$union Container>]::Value(child) => match op {
                            $union::Choose(choice) => {
                                if child.variant() == *choice {
                                    ::std::result::Result::Err([<$union Rejection>]::NotConflict)
                                } else {
                                    ::std::result::Result::Err([<$union Rejection>]::MissingVariant)
                                }
                            }
                            _ => match (op, child.as_ref()) {
                                $(
                                    (
                                        $union::$variant(o),
                                        [<$union Child>]::$variant(log),
                                    ) => {
                                        let child_op: <$log as $crate::moirai_protocol::state::log::IsLog>::Update =
                                            <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(
                                                ::std::clone::Clone::clone(o),
                                            );
                                        <$log as $crate::moirai_protocol::state::log::IsLog>::is_enabled(
                                            log,
                                            &child_op,
                                        )
                                            .map_err(|error| [<$union Rejection>]::$variant(::std::boxed::Box::new(error)))
                                    }
                                )*
                                _ => ::std::result::Result::Err([<$union Rejection>]::WrongVariant),
                            },
                        },
                        [<$union Container>]::Conflicts(children) => {
                            if let $union::Choose(choice) = op {
                                return children
                                    .iter()
                                    .any(|child| child.variant() == *choice)
                                    .then_some(())
                                    .ok_or([<$union Rejection>]::MissingVariant);
                            }

                            let mut rejection = ::std::option::Option::None;
                            for child in children {
                                match (op, child) {
                                $(
                                    (
                                        $union::$variant(o),
                                        [<$union Child>]::$variant(log),
                                    ) => {
                                        let child_op: <$log as $crate::moirai_protocol::state::log::IsLog>::Update =
                                            <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(
                                                ::std::clone::Clone::clone(o),
                                            );
                                        match <$log as $crate::moirai_protocol::state::log::IsLog>::is_enabled(
                                            log,
                                            &child_op,
                                        ) {
                                            ::std::result::Result::Ok(()) => return ::std::result::Result::Ok(()),
                                            ::std::result::Result::Err(error) => {
                                                if rejection.is_none() {
                                                    rejection = ::std::option::Option::Some([<$union Rejection>]::$variant(::std::boxed::Box::new(error)));
                                                }
                                            }
                                        }
                                    }
                                )*
                                _ => {}
                                }
                            }
                            ::std::result::Result::Err(rejection.unwrap_or([<$union Rejection>]::WrongVariant))
                        }
                    }
                }

                fn effect(
                    &mut self,
                    event: $crate::moirai_protocol::event::Event<Self::Update>,
                    ctx: &mut $crate::moirai_protocol::state::effect_context::EffectContext<'_>)
                {
                    match ::std::clone::Clone::clone(event.op()) {
                        $(
                            $union::$variant(o) => {
                                self.record_routed([<$union Variant>]::$variant, event.id());
                                ctx.with_variant(::std::stringify!([<$variant:lower>]), |ctx| {
                                    let child_op: <$log as $crate::moirai_protocol::state::log::IsLog>::Update = <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o);
                                    let child_event = $crate::moirai_protocol::event::Event::unfold(event, child_op);
                                    if let ::std::option::Option::Some([<$union Child>]::$variant(log)) = self.present_mut([<$union Variant>]::$variant) {
                                        <$log as $crate::moirai_protocol::state::log::IsLog>::effect(log, child_event, ctx);
                                    } else {
                                        // An absent child is made present again with its log
                                        let mut child = self
                                            .take_removed([<$union Variant>]::$variant)
                                            .unwrap_or_else(|| [<$union Child>]::$variant(<$log as $crate::moirai_protocol::state::log::IsLog>::new()));
                                        if let [<$union Child>]::$variant(log) = &mut child {
                                            <$log as $crate::moirai_protocol::state::log::IsLog>::effect(log, child_event, ctx);
                                        }
                                        self.insert_present(child);
                                    }
                                });
                            }
                        )*
                        $union::Choose(choice) => {
                            self.reset_children(event.version(), true, ::std::option::Option::Some(choice));
                        }
                    }
                }

                fn stabilize(&mut self, frontier: &$crate::moirai_protocol::clock::causal_frontier::CausalFrontier) {
                    match &mut self.child {
                        [<$union Container>]::Unset => {}
                        [<$union Container>]::Value(union_child) => Self::stabilize_child(union_child, frontier),
                        [<$union Container>]::Conflicts(union_childs) => {
                            for union_child in union_childs {
                                Self::stabilize_child(union_child, frontier);
                            }
                        }
                    }
                    for union_child in &mut self.removed {
                        Self::stabilize_child(union_child, frontier);
                    }
                }

                fn redundant_by_parent(&mut self, version: &$crate::moirai_protocol::clock::version_vector::Version, conservative: ::std::primitive::bool) {
                    self.reset_children(version, conservative, ::std::option::Option::None);
                }

                // TODO: structurally its Unset, semantically not necessarily, so we may want to split this into two methods
                fn is_default(&self) -> ::std::primitive::bool {
                    let present_default = match &self.child {
                        [<$union Container>]::Unset => true,
                        [<$union Container>]::Value(child) => Self::child_is_default(child.as_ref()),
                        [<$union Container>]::Conflicts(children) => children
                            .iter()
                            .all(Self::child_is_default),
                    };
                    present_default && self.removed.iter().all(Self::child_is_default)
                }
            }

            /// Held by an optional, the union also drops each member the
            /// removal emptied, keeping those a concurrent write reached. A
            /// keyed collection keeps its emptied members, so this is not part
            /// of `redundant_by_parent`.
            impl $crate::union::held_by_optional::OptionalContent for [<$union Log>] {
                fn redundant_by_optional(&mut self, version: &$crate::moirai_protocol::clock::version_vector::Version, conservative: ::std::primitive::bool) {
                    <Self as $crate::moirai_protocol::state::log::IsLog>::redundant_by_parent(self, version, conservative);
                    self.child = match ::std::mem::take(&mut self.child) {
                        [<$union Container>]::Value(child) if !Self::child_is_default(&child) => {
                            [<$union Container>]::Value(child)
                        }
                        [<$union Container>]::Conflicts(children) => {
                            let mut kept: ::std::vec::Vec<[<$union Child>]> = children
                                .into_iter()
                                .filter(|child| !Self::child_is_default(child))
                                .collect();
                            match kept.len() {
                                0 => [<$union Container>]::Unset,
                                1 => [<$union Container>]::Value(::std::boxed::Box::new(kept.remove(0))),
                                _ => [<$union Container>]::Conflicts(kept),
                            }
                        }
                        _ => [<$union Container>]::Unset,
                    };
                    // A dropped child keeps no log, so its record of uncovered events goes with it.
                    let present: ::std::vec::Vec<[<$union Variant>]> = match &self.child {
                        [<$union Container>]::Unset => ::std::vec::Vec::new(),
                        [<$union Container>]::Value(child) => ::std::vec![child.variant()],
                        [<$union Container>]::Conflicts(children) => children.iter().map(|child| child.variant()).collect(),
                    };
                    self.uncovered.retain(|(variant, _)| present.contains(variant));
                }
            }

            impl $crate::moirai_protocol::crdt::eval::EvalNested<$crate::moirai_protocol::crdt::query::Read<[<$union Value>]>> for [<$union Log>] {
                fn execute_query(
                    &self,
                    _q: &$crate::moirai_protocol::crdt::query::Read<[<$union Value>]>,
                ) -> [<$union Value>] {
                    match &self.child {
                        [<$union Container>]::Unset => [<$union Value>]::Unset,
                        [<$union Container>]::Value(child) => {
                            match child.as_ref() {
                                $(
                                    [<$union Child>]::$variant(log) => {
                                        let value = $crate::moirai_protocol::crdt::eval::EvalNested::execute_query(
                                            log,
                                            &$crate::moirai_protocol::crdt::query::Read::<$value>::new(),
                                        );
                                        [<$union Value>]::Value(::std::boxed::Box::new([<$union ChildValue>]::$variant(value)))
                                    }
                                )*
                            }
                        },
                        [<$union Container>]::Conflicts(children) => {
                            let mut values = ::std::vec::Vec::new();
                            for child in children {
                                let value = match child {
                                    $(
                                        [<$union Child>]::$variant(log) => {
                                            let v = $crate::moirai_protocol::crdt::eval::EvalNested::execute_query(
                                                log,
                                                &$crate::moirai_protocol::crdt::query::Read::<$value>::new(),
                                            );
                                            [<$union ChildValue>]::$variant(v)
                                        }
                                    )*
                                };
                                values.push(value);
                            }
                            // TODO: in which case conflict can be empty?
                            match values.len() {
                                0 => [<$union Value>]::Unset,
                                1 => [<$union Value>]::Value(::std::boxed::Box::new(values.pop().unwrap())),
                                _ => {
                                    values.sort();
                                    [<$union Value>]::Conflict(values)
                                }
                            }
                        }
                    }
                }
            }

            //* Deterministic ordering of child values */

            #[repr(usize)]
            enum [<$union ChildValueRank>] {
                $(
                    $variant,
                )*
            }

            impl [<$union ChildValue>] {
                fn rank(&self) -> ::std::primitive::usize {
                    match self {
                        $(
                            Self::$variant(_) => [<$union ChildValueRank>]::$variant as ::std::primitive::usize,
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

/// What an optional's content does with a removal the optional hands down,
/// its own unset or a removal above it: the optional then drops the content
/// if it is left at its default. The trait has no default body, so a content
/// log (a user wrapper included) states what it does with the removal or is
/// refused as an optional's content at compile time.
pub mod held_by_optional {
    use moirai_protocol::{
        clock::version_vector::Version,
        crdt::replicated_data_type::ReplicatedDataType,
        state::{log::IsLog, po_log::POLog},
    };

    /// The content of an optional: see [the module](self).
    pub trait OptionalContent: IsLog {
        /// A removal handed down by the optional that holds this log.
        fn redundant_by_optional(&mut self, version: &Version, conservative: bool);
    }

    impl<L: OptionalContent> OptionalContent for Box<L> {
        fn redundant_by_optional(&mut self, version: &Version, conservative: bool) {
            (**self).redundant_by_optional(version, conservative);
        }
    }

    /// As from any other parent.
    impl<R, U> OptionalContent for POLog<R, U>
    where
        R: ReplicatedDataType,
        POLog<R, U>: IsLog,
    {
        fn redundant_by_optional(&mut self, version: &Version, conservative: bool) {
            IsLog::redundant_by_parent(self, version, conservative);
        }
    }
}
