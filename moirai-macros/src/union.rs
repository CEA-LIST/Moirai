#[macro_export]
macro_rules! union {
    (
        $union:ident = $($variant:ident ($ty:ty, $log:ty => $value:ty))|+ $(,)?
    ) => {
        $crate::paste::paste! {
            /// List of variant names, used in the `Choose` operation to select a variant
            #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
            pub enum [<$union Variant>] {
                $(
                    $variant,
                )*
            }

            /// Set of Union CRDT operations
            #[derive(Clone, Debug)]
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
            #[derive(Clone, Debug, Default)]
            #[cfg_attr(feature = "test_utils", derive(::deepsize::DeepSizeOf))]
            pub struct [<$union Log>] {
                pub child: [<$union Container>],
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
                ) {
                    match child {
                        $(
                            [<$union Child>]::$variant(log) => {
                                <$log as $crate::moirai_protocol::state::log::IsLog>::redundant_by_parent(log, version, true);
                            }
                        )*
                    }
                }
            }

            impl $crate::moirai_protocol::state::log::IsLog for [<$union Log>] {
                type Command = [<$union>];
                type Op = [<$union>];
                type Rejection = [<$union Rejection>];

                fn new() -> Self {
                    <Self as ::std::default::Default>::default()
                }

                fn prepare(&self, cmd: Self::Command) -> Self::Op {
                    cmd
                }

                fn is_enabled(&self, op: &Self::Op) -> ::std::result::Result<(), Self::Rejection> {
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
                                        let child_op: <$log as $crate::moirai_protocol::state::log::IsLog>::Op =
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
                                        let child_op: <$log as $crate::moirai_protocol::state::log::IsLog>::Op =
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
                    event: $crate::moirai_protocol::event::Event<Self::Op>,
                    ctx: &mut $crate::moirai_protocol::state::effect_context::EffectContext<'_>)
                {
                    match ::std::clone::Clone::clone(event.op()) {
                        $(
                            $union::$variant(o) => {
                                ctx.with_variant(::std::stringify!([<$variant:lower>]), |ctx| {
                                    match &mut self.child {
                                        [<$union Container>]::Unset => {
                                            let log = {
                                                let mut log = <$log as $crate::moirai_protocol::state::log::IsLog>::new();
                                                let child_op: <$log as $crate::moirai_protocol::state::log::IsLog>::Op = <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o);
                                                let child_event = $crate::moirai_protocol::event::Event::unfold(event, child_op);
                                                <$log as $crate::moirai_protocol::state::log::IsLog>::effect(&mut log, child_event, ctx);
                                                log
                                            };
                                            self.child = [<$union Container>]::Value(::std::boxed::Box::new([<$union Child>]::$variant(log)));
                                        }
                                        [<$union Container>]::Value(existing_child) => {
                                            if let [<$union Child>]::$variant(existing_log) = existing_child.as_mut() {
                                                let child_event = $crate::moirai_protocol::event::Event::unfold(event, <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o));
                                                <$log as $crate::moirai_protocol::state::log::IsLog>::effect(existing_log, child_event, ctx);
                                            } else {
                                                let mut new_children = ::std::vec::Vec::new();
                                                new_children.push(::std::clone::Clone::clone(&**existing_child));
                                                let log = {
                                                    let mut log = <$log as $crate::moirai_protocol::state::log::IsLog>::new();
                                                    let child_event = $crate::moirai_protocol::event::Event::unfold(event, <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o));
                                                    <$log as $crate::moirai_protocol::state::log::IsLog>::effect(&mut log, child_event, ctx);
                                                    log
                                                };
                                                new_children.push([<$union Child>]::$variant(log));
                                                self.child = [<$union Container>]::Conflicts(new_children);
                                            }
                                        }
                                        [<$union Container>]::Conflicts(children) => {
                                            if let ::std::option::Option::Some([<$union Child>]::$variant(log)) = children
                                                .iter_mut()
                                                .find(|c| ::std::matches!(c, [<$union Child>]::$variant(_)))
                                            {
                                                let child_event = $crate::moirai_protocol::event::Event::unfold(event, <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o));
                                                <$log as $crate::moirai_protocol::state::log::IsLog>::effect(log, child_event, ctx);
                                            } else {
                                                let log = {
                                                    let mut log = <$log as $crate::moirai_protocol::state::log::IsLog>::new();
                                                    let child_event = $crate::moirai_protocol::event::Event::unfold(event, <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o));
                                                    <$log as $crate::moirai_protocol::state::log::IsLog>::effect(&mut log, child_event, ctx);
                                                    log
                                                };
                                                children.push([<$union Child>]::$variant(log));
                                            }
                                        }
                                    }
                                });
                            }
                        )*
                        $union::Choose(choice) => {
                            match &mut self.child {
                                [<$union Container>]::Unset => {}
                                [<$union Container>]::Value(existing_child) => {
                                    if existing_child.variant() != choice {
                                        Self::reset_child(existing_child, event.version());
                                        if Self::child_is_default(&existing_child) {
                                            self.child = [<$union Container>]::Unset;
                                        }
                                    }
                                }
                                [<$union Container>]::Conflicts(children) => {
                                    for mut child in children.iter_mut() {
                                        if child.variant() != choice {
                                            Self::reset_child(child, event.version());
                                        }
                                    }
                                    let no_conflicts = children.iter().all(|child| child.variant() == choice || Self::child_is_default(child));
                                    if no_conflicts {
                                        let selected_child = children
                                            .iter()
                                            .find(|child| child.variant() == choice)
                                            .expect("there should be a child with the chosen variant");
                                        self.child = [<$union Container>]::Value(::std::boxed::Box::new(
                                            ::std::clone::Clone::clone(selected_child),
                                        ));
                                    } else {
                                        children.retain(|child| !Self::child_is_default(child));
                                    }
                                }
                            }
                        }
                    }
                }

                fn stabilize(&mut self, _version: &$crate::moirai_protocol::clock::version_vector::Version) {
                    match &mut self.child {
                        [<$union Container>]::Unset => {}
                        [<$union Container>]::Value(union_child) => {
                            match union_child.as_mut() {
                                $(
                                    [<$union Child>]::$variant(log) => {
                                        <$log as $crate::moirai_protocol::state::log::IsLog>::stabilize(
                                            log,
                                            _version,
                                        );
                                    }
                                )*
                            }
                        },
                        [<$union Container>]::Conflicts(union_childs) => {
                            for union_child in union_childs {
                                match union_child {
                                    $(
                                        [<$union Child>]::$variant(log) => {
                                            <$log as $crate::moirai_protocol::state::log::IsLog>::stabilize(
                                                log,
                                                _version,
                                            );
                                        }
                                    )*
                                }
                            }
                        }
                    }
                }

                fn redundant_by_parent(&mut self, version: &$crate::moirai_protocol::clock::version_vector::Version, conservative: ::std::primitive::bool) {
                    match &mut self.child {
                        [<$union Container>]::Unset => {}
                        [<$union Container>]::Value(union_child) => match union_child.as_mut() {
                            $(
                                [<$union Child>]::$variant(log) => {
                                    <$log as $crate::moirai_protocol::state::log::IsLog>::redundant_by_parent(
                                        log,
                                        version,
                                        conservative,
                                    );
                                }
                            )*
                        },
                        [<$union Container>]::Conflicts(union_childs) => {
                            for union_child in union_childs {
                                match union_child {
                                    $(
                                        [<$union Child>]::$variant(log) => {
                                            <$log as $crate::moirai_protocol::state::log::IsLog>::redundant_by_parent(
                                                log,
                                                version,
                                                conservative,
                                            );
                                        }
                                    )*
                                }
                            }
                        }
                    }
                }

                // TODO: structurally its Unset, semantically not necessarily, so we may want to split this into two methods
                fn is_default(&self) -> ::std::primitive::bool {
                    match &self.child {
                        [<$union Container>]::Unset => true,
                        [<$union Container>]::Value(child) => Self::child_is_default(child.as_ref()),
                        [<$union Container>]::Conflicts(children) => children
                            .iter()
                            .all(Self::child_is_default),
                    }
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
