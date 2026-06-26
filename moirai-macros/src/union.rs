#[macro_export]
macro_rules! union {
    (
        $union:ident = $($variant:ident ($ty:ty, $log:ty))|+ $(,)?
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
                fn is_match_log(&self, log: &[<$union Child>]) -> bool {
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

            impl [<$union Child>] {
                /// Returns the variant name that this child log corresponds to.
                fn __moirai_variant(&self) -> [<$union Variant>] {
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
                    $variant(<$log as $crate::moirai_protocol::state::log::IsLog>::Value),
                )*
            }

            /// Value returned by the union log, which may be a single value, a conflict of values, or unset.
            #[derive(Clone, Debug, Default, PartialEq)]
            pub enum [<$union Value>] {
                #[default]
                Unset,
                Value(Box<[<$union ChildValue>]>),
                Conflict(Vec<[<$union ChildValue>]>),
            }

            /// Internal Union log state
            #[derive(Clone, Debug, Default)]
            pub enum [<$union Container>] {
                #[default]
                Unset,
                Value(Box<[<$union Child>]>),
                Conflicts(Vec<[<$union Child>]>),
            }

            /// Union log
            #[derive(Clone, Debug, Default)]
            pub struct [<$union Log>] {
                pub child: [<$union Container>],
                __moirai_read_cache: $crate::moirai_protocol::state::cache::CacheCell<[<$union Value>]>,
            }

            /// Rejection reasons for union operations
            /// i.e., when an operation is not enabled in the current state
            #[derive(Debug)]
            pub enum [<$union Rejection>] {
                WrongVariant,
                MissingVariant,
                NotConflict,
                $(
                    $variant(Box<<$log as $crate::moirai_protocol::state::log::IsLog>::Rejection>),
                )*
            }

            impl [<$union Log>] {
                fn __moirai_child_is_default(child: &[<$union Child>]) -> bool {
                    match child {
                        $(
                            [<$union Child>]::$variant(log) => {
                                <$log as $crate::moirai_protocol::state::log::IsLog>::is_default(log)
                            }
                        )*
                    }
                }

                fn __moirai_reset_child(
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
                type Value = [<$union Value>];
                type Op = [<$union>];
                type Rejection = [<$union Rejection>];

                fn new() -> Self {
                    Self::default()
                }

                fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
                    match &self.child {
                        [<$union Container>]::Unset => match op {
                            $union::Choose(_) => Err([<$union Rejection>]::MissingVariant),
                            _ => Ok(()),
                        },
                        [<$union Container>]::Value(child) => match op {
                            $union::Choose(choice) => {
                                if child.__moirai_variant() == *choice {
                                    Err([<$union Rejection>]::NotConflict)
                                } else {
                                    Err([<$union Rejection>]::MissingVariant)
                                }
                            }
                            _ => match (op, child.as_ref()) {
                                $(
                                    (
                                        $union::$variant(o),
                                        [<$union Child>]::$variant(log),
                                    ) => {
                                        let child_op: <$log as $crate::moirai_protocol::state::log::IsLog>::Op =
                                            <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o.clone());
                                        log.is_enabled(&child_op)
                                            .map_err(|error| [<$union Rejection>]::$variant(Box::new(error)))
                                    }
                                )*
                                _ => Err([<$union Rejection>]::WrongVariant),
                            },
                        },
                        [<$union Container>]::Conflicts(children) => {
                            if let $union::Choose(choice) = op {
                                return children
                                    .iter()
                                    .any(|child| child.__moirai_variant() == *choice)
                                    .then_some(())
                                    .ok_or([<$union Rejection>]::MissingVariant);
                            }

                            let mut rejection = None;
                            for child in children {
                                match (op, child) {
                                $(
                                    (
                                        $union::$variant(o),
                                        [<$union Child>]::$variant(log),
                                    ) => {
                                        let child_op: <$log as $crate::moirai_protocol::state::log::IsLog>::Op =
                                            <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o.clone());
                                        match log.is_enabled(&child_op) {
                                            Ok(()) => return Ok(()),
                                            Err(error) => {
                                                if rejection.is_none() {
                                                    rejection = Some([<$union Rejection>]::$variant(Box::new(error)));
                                                }
                                            }
                                        }
                                    }
                                )*
                                _ => {}
                                }
                            }
                            Err(rejection.unwrap_or([<$union Rejection>]::WrongVariant))
                        }
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
                            $union::$variant(o) => {
                                ctx.with_variant(stringify!([<$variant:lower>]), |ctx| {
                                    match &mut self.child {
                                        [<$union Container>]::Unset => {
                                            let log = {
                                                let mut log = <$log as $crate::moirai_protocol::state::log::IsLog>::new();
                                                let child_op: <$log as $crate::moirai_protocol::state::log::IsLog>::Op = <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o);
                                                let child_event = $crate::moirai_protocol::event::Event::unfold(event, child_op);
                                                <$log as $crate::moirai_protocol::state::log::IsLog>::effect(&mut log, child_event, ctx);
                                                log
                                            };
                                            self.child = [<$union Container>]::Value(Box::new([<$union Child>]::$variant(log)));
                                        }
                                        [<$union Container>]::Value(existing_child) => {
                                            if let [<$union Child>]::$variant(existing_log) = existing_child.as_mut() {
                                                let child_event = $crate::moirai_protocol::event::Event::unfold(event, <$ty as $crate::moirai_protocol::utils::boxer::Boxer<_>>::boxer(o));
                                                <$log as $crate::moirai_protocol::state::log::IsLog>::effect(existing_log, child_event, ctx);
                                            } else {
                                                let mut new_children = vec![];
                                                new_children.push((**existing_child).clone());
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
                                            if let Some([<$union Child>]::$variant(log)) = children
                                                .iter_mut()
                                                .find(|c| matches!(c, [<$union Child>]::$variant(_)))
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
                                    if existing_child.__moirai_variant() != choice {
                                        Self::__moirai_reset_child(existing_child, event.version());
                                        if Self::__moirai_child_is_default(&existing_child) {
                                            self.child = [<$union Container>]::Unset;
                                        }
                                    }
                                }
                                [<$union Container>]::Conflicts(children) => {
                                    for mut child in children.iter_mut() {
                                        if child.__moirai_variant() != choice {
                                            Self::__moirai_reset_child(child, event.version());
                                        }
                                    }
                                    let no_conflicts = children.iter().all(|child| child.__moirai_variant() == choice || Self::__moirai_child_is_default(child));
                                    if no_conflicts {
                                        let selected_child = children
                                            .iter()
                                            .find(|child| child.__moirai_variant() == choice)
                                            .expect("there should be a child with the chosen variant");
                                        self.child = [<$union Container>]::Value(Box::new(selected_child.clone()));
                                    } else {
                                        children.retain(|child| !Self::__moirai_child_is_default(child));
                                    }
                                }
                            }
                        }
                    }
                }

                fn stabilize(&mut self, _version: &$crate::moirai_protocol::clock::version_vector::Version) {
                    self.__moirai_read_cache.invalidate();
                    match &mut self.child {
                        [<$union Container>]::Unset => {}
                        [<$union Container>]::Value(union_child) => {
                            match union_child.as_mut() {
                                $(
                                    [<$union Child>]::$variant(log) => {
                                        log.stabilize(_version);
                                    }
                                )*
                            }
                        },
                        [<$union Container>]::Conflicts(union_childs) => {
                            for union_child in union_childs {
                                match union_child {
                                    $(
                                        [<$union Child>]::$variant(log) => {
                                            log.stabilize(_version);
                                        }
                                    )*
                                }
                            }
                        }
                    }
                }

                fn redundant_by_parent(&mut self, version: &$crate::moirai_protocol::clock::version_vector::Version, conservative: bool) {
                    self.__moirai_read_cache.invalidate();
                    match &mut self.child {
                        [<$union Container>]::Unset => {}
                        [<$union Container>]::Value(union_child) => match union_child.as_mut() {
                            $(
                                [<$union Child>]::$variant(log) => {
                                    log.redundant_by_parent(version, conservative);
                                }
                            )*
                        },
                        [<$union Container>]::Conflicts(union_childs) => {
                            for union_child in union_childs {
                                match union_child {
                                    $(
                                        [<$union Child>]::$variant(log) => {
                                            log.redundant_by_parent(version, conservative);
                                        }
                                    )*
                                }
                            }
                        }
                    }
                }

                // TODO: structurally its Unset, semantically not necessarily, so we may want to split this into two methods
                fn is_default(&self) -> bool {
                    match &self.child {
                        [<$union Container>]::Unset => true,
                        [<$union Container>]::Value(child) => Self::__moirai_child_is_default(child.as_ref()),
                        [<$union Container>]::Conflicts(children) => children
                            .iter()
                            .all(Self::__moirai_child_is_default),
                    }
                }
            }

            impl $crate::moirai_protocol::crdt::eval::EvalNested<$crate::moirai_protocol::crdt::query::Read<<Self as $crate::moirai_protocol::state::log::IsLog>::Value>> for [<$union Log>] {
                fn execute_query(
                    &self,
                    _q: $crate::moirai_protocol::crdt::query::Read<Self::Value>,
                ) -> <$crate::moirai_protocol::crdt::query::Read<Self::Value> as $crate::moirai_protocol::crdt::query::QueryOperation>::Response {
                    self.read_uncached()
                }
            }

            impl $crate::moirai_protocol::crdt::eval::BorrowedRead for [<$union Log>] {
                fn read_ref(&self) -> &Self::Value {
                    self.__moirai_read_cache.get_or_compute(|| self.read_uncached())
                }
            }

            impl [<$union Log>] {
                fn read_uncached(&self) -> [<$union Value>] {
                    match &self.child {
                        [<$union Container>]::Unset => [<$union Value>]::Unset,
                        [<$union Container>]::Value(child) => {
                            match child.as_ref() {
                                $(
                                    [<$union Child>]::$variant(log) => {
                                        let value = $crate::moirai_protocol::crdt::eval::EvalNested::execute_query(
                                            log,
                                            $crate::moirai_protocol::crdt::query::Read::new(),
                                        );
                                        [<$union Value>]::Value(Box::new([<$union ChildValue>]::$variant(value)))
                                    }
                                )*
                            }
                        },
                        [<$union Container>]::Conflicts(children) => {
                            let mut values = vec![];
                            for child in children {
                                let value = match child {
                                    $(
                                        [<$union Child>]::$variant(log) => {
                                            let v = $crate::moirai_protocol::crdt::eval::EvalNested::execute_query(
                                                log,
                                                $crate::moirai_protocol::crdt::query::Read::new(),
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
                                1 => [<$union Value>]::Value(Box::new(values.pop().unwrap())),
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
                fn rank(&self) -> usize {
                    match self {
                        $(
                            Self::$variant(_) => [<$union ChildValueRank>]::$variant as usize,
                        )*
                    }
                }
            }

            impl PartialEq for [<$union ChildValue>] {
                fn eq(&self, other: &Self) -> bool {
                    match (self, other) {
                        $(
                            (Self::$variant(left), Self::$variant(right)) => left == right,
                        )*
                        _ => false,
                    }
                }
            }

            impl Eq for [<$union ChildValue>] {}

            impl PartialOrd for [<$union ChildValue>] {
                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                    Some(self.cmp(other))
                }
            }

            impl Ord for [<$union ChildValue>] {
                fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                    self.rank().cmp(&other.rank())
                }
            }

            //* Deepsize */

            #[cfg(feature = "test_utils")]
            impl ::deepsize::DeepSizeOf for [<$union Variant>] {
                fn deep_size_of_children(&self, _context: &mut ::deepsize::Context) -> usize {
                    0
                }
            }

            #[cfg(feature = "test_utils")]
            impl ::deepsize::DeepSizeOf for $union {
                fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> usize {
                    match self {
                        $(
                            Self::$variant(value) => value.deep_size_of_children(context),
                        )*
                        Self::Choose(_) => 0,
                    }
                }
            }

            impl $crate::moirai_protocol::utils::intern_str::InternalizeOp for $union {
                fn internalize(self, interner: &$crate::moirai_protocol::utils::intern_str::Interner) -> Self {
                    match self {
                        $(
                            Self::$variant(o) => Self::$variant(o.internalize(interner)),
                        )*
                        Self::Choose(variant) => Self::Choose(variant),
                    }
                }
            }

            impl std::fmt::Display for [<$union Rejection>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    match self {
                        Self::WrongVariant => write!(f, "operation does not match the active union variant"),
                        Self::MissingVariant => write!(f, "chosen union variant is not currently set"),
                        Self::NotConflict => write!(f, "choose is only enabled when the union is in conflict"),
                        $(
                            Self::$variant(error) => write!(f, "{}: {}", stringify!($variant), error),
                        )*
                    }
                }
            }
        }
    };
}
