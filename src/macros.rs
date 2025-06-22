// #[macro_export]
// macro_rules! make_struct {
//     ($name: ident) => {
//         use paste::paste;
//         paste! {
//             #[derive(Debug, Default)]
//             struct [< $name:camel >] {

//             }
//         }
//     };
// }

// #[macro_export]
// macro_rules! crdt_object {
//     ( { $( $field:ident : $typ:ty ),* $(,)? } ) => {
//         use paste::paste;
//         paste! {
//             // #[derive(Debug, Default)]
//             struct CRDTLog<$( $field:camel),*> {
//             }
//         }
//     };
//( { $( $field:ident : $typ:ty ),* $(,)? } ) => {
//     use paste::paste;

//     #[derive(Clone, Debug)]
//     pub enum CRDTOp {
//         $(
//             paste::paste! {
//                 [<$field:camel>](<$typ as Log>::Op),
//             }
//         )*
//     }

//     #[derive(Clone, Debug, Default)]
//     pub struct CRDTLog<$( $field ),*>
//     where
//         $( $field: std::fmt::Debug ),*
//     {
//         $( pub $field: $field, )*
//     }
// }
// }
//     impl Log for CRDTLog {
//         type Value = ( $( <$typ as Log>::Value ),* );
//         type Op = CRDTOp;

//         fn new() -> Self {
//             Self {
//                 $( $field: <$typ as Log>::new(), )*
//             }
//         }

//         fn new_event(&mut self, event: &Event<Self::Op>) {
//             match &event.op {
//                 $(
//                     CRDTOp::$field(op) => {
//                         let e = Event::new(op.clone(), event.metadata().clone(), event.lamport());
//                         self.$field.new_event(&e);
//                     }
//                 )*
//             }
//         }

//         fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool, ltm: &MatrixClock) {
//             match &event.op {
//                 $(
//                     CRDTOp::$field(op) => {
//                         let e = Event::new(op.clone(), event.metadata().clone(), event.lamport());
//                         self.$field.prune_redundant_events(&e, is_r_0, ltm);
//                     }
//                 )*
//             }
//         }

//         fn purge_stable_metadata(&mut self, dot: &Dot) {
//             $( self.$field.purge_stable_metadata(dot); )*
//         }

//         fn stable_by_clock(&mut self, clock: &Clock<Full>) {
//             $( self.$field.stable_by_clock(clock); )*
//         }

//         fn collect_events_since(&self, since: &Since, ltm: &MatrixClock) -> Vec<Event<Self::Op>> {
//             let mut result = vec![];
//             $(
//                 result.extend(
//                     self.$field
//                         .collect_events_since(since, ltm)
//                         .into_iter()
//                         .map(|e| Event::new(CRDTOp::$field(e.op.clone()), e.metadata().clone(), e.lamport())),
//                 );
//             )*
//             result
//         }

//         fn clock_from_event(&self, event: &Event<Self::Op>) -> Clock<Full> {
//             match &event.op {
//                 $(
//                     CRDTOp::$field(op) => {
//                         let e = Event::new(op.clone(), event.metadata().clone(), event.lamport());
//                         self.$field.clock_from_event(&e)
//                     }
//                 )*
//             }
//         }

//         fn r_n(&mut self, metadata: &Clock<Full>, conservative: bool) {
//             $( self.$field.r_n(metadata, conservative); )*
//         }

//         fn redundant_itself(&self, event: &Event<Self::Op>) -> bool {
//             match &event.op {
//                 $(
//                     CRDTOp::$field(op) => {
//                         let e = Event::new(op.clone(), event.metadata().clone(), event.lamport());
//                         self.$field.redundant_itself(&e)
//                     }
//                 )*
//             }
//         }

//         fn eval(&self) -> Self::Value {
//             ( $( self.$field.eval() ),* )
//         }

//         fn stabilize(&mut self, dot: &Dot) {
//             $( self.$field.stabilize(dot); )*
//         }

//         fn is_empty(&self) -> bool {
//             true $( && self.$field.is_empty() )*
//         }

//         fn deps(
//             &mut self,
//             clocks: &mut VecDeque<Clock<Partial>>,
//             view: &Rc<ViewData>,
//             dot: &Dot,
//             op: &Self::Op,
//         ) {
//             match op {
//                 $(
//                     CRDTOp::$field(inner) => self.$field.deps(clocks, view, dot, inner),
//                 )*
//             }
//         }
//     }
// };
// }

// Example usage:
// crdt_object!({ name: LWWRegister<String>, bag_content: AWSet<Item> });
