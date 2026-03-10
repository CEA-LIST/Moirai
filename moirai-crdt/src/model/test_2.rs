use moirai_macros::{record, union};
use moirai_protocol::state::po_log::VecLog;

use crate::flag::ew_flag::EWFlag;

record!(Sequence {
    children: Box<TreeNodeLog>,
});

union!(TreeNode = Sequence(Sequence, SequenceLog) | Action(Action, ActionLog));

record!(BehaviorTree { child: TreeNodeLog });

record!(Action {
    is_success: VecLog<EWFlag>,
});
