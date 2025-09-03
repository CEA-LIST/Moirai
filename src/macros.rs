// A macro to define a record type with multiple fields, each field being a different log type.
// It generates the necessary structures and implements the Log trait for the record.

#[macro_export]
macro_rules! record {
    ($name:ident { $($field:ident : $T:path),* $(,)? }) => {
        paste::paste! {
            #[derive(Clone, Debug)]
            pub enum $name {
                $(
                    [<$field:camel>](< $T as $crate::protocol::state::log::IsLog>::Op),
                )*
            }

            #[derive(Debug, Default, PartialEq)]
            pub struct [<$name Value>] {
                $(
                    pub $field: <$T as $crate::protocol::state::log::IsLog>::Value,
                )*
            }

            #[derive(Debug, Default)]
            pub struct [<$name Log>] {
                $(
                    pub $field: $T,
                )*
            }

            impl $crate::protocol::state::log::IsLog for [<$name Log>] {
                type Op = $name;
                type Value = [<$name Value>];

                fn new() -> Self {
                    Self {
                        $(
                            $field: <$T as $crate::protocol::state::log::IsLog>::new(),
                        )*
                    }
                }

                fn effect(&mut self, event: $crate::protocol::event::Event<Self::Op>) {
                    match event.op().clone() {
                        $(
                            $name::[<$field:camel>](op) => {
                                let child_op = $crate::protocol::event::Event::unfold(event, op);
                                self.$field.effect(child_op);
                            }
                        )*
                    }
                }

                fn eval(&self) -> Self::Value {
                    [<$name Value>] {
                        $(
                            $field: self.$field.eval(),
                        )*
                    }
                }

                fn stabilize(&mut self, version: &$crate::protocol::clock::version_vector::Version) {
                    $(
                        self.$field.stabilize(version);
                    )*
                }

                fn redundant_by_parent(&mut self, version: &$crate::protocol::clock::version_vector::Version, conservative: bool) {
                    $(
                        self.$field.redundant_by_parent(version, conservative);
                    )*
                }
            }
        }
    };
}
