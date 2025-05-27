#[cfg(feature = "utils")]
use deepsize::DeepSizeOf;
use log::info;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::rc::Rc;
#[cfg(feature = "serde")]
use tsify::Tsify;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum ViewStatus {
    /// The view has been installed. At least one view is always installed and the last installed view is the current view.
    Installed,
    /// The view is being installed. There is at most one view being installed at a time.
    Installing,
    /// The view is planned to be installed. It will be installed right after the current installing view.
    /// Only consecutive views can be planned and they must follow the installing one.
    Planned,
    /// The view is pending.
    Pending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum ViewInstallingStatus {
    NothingToInstall,
    AlreadyInstalling,
    Starting,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
#[cfg_attr(feature = "utils", derive(DeepSizeOf))]
pub struct ViewData {
    pub id: usize,
    pub members: Vec<String>,
}

impl ViewData {
    pub fn member_pos(&self, id: &str) -> Option<usize> {
        self.members.iter().position(|m| m == id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct View {
    pub data: Rc<ViewData>,
    pub status: ViewStatus,
}

impl View {
    pub fn new(id: usize, members: Vec<String>, status: ViewStatus) -> Self {
        let data = Rc::new(ViewData { id, members });
        Self { data, status }
    }

    pub fn init(tcsb_id: &str) -> Self {
        let data = Rc::new(ViewData {
            id: 0,
            members: vec![tcsb_id.to_string()],
        });
        Self {
            data,
            status: ViewStatus::Installed,
        }
    }
}

/// Invariants:
/// - There is always at least one view in the list
/// - There may be zero or one view being installed
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct Views {
    views: Vec<View>,
    current_view_id: usize,
}

impl Views {
    pub fn new(members: Vec<String>) -> Self {
        Self {
            views: vec![View::init(&members[0])],
            current_view_id: 0,
        }
    }

    /// Install the view given in parameter
    pub fn add_pending_view(&mut self, members: Vec<String>) {
        assert!(!members.is_empty());
        let view_id = self.views.len();
        let view = View::new(view_id, members, ViewStatus::Pending);
        assert!(self.views.last().unwrap().data.id < view_id);
        self.views.push(view);
    }

    /// Start installing the next view
    pub fn start_installing(&mut self) -> ViewInstallingStatus {
        let next_pending_view = self.views.get_mut(self.current_view_id + 1);
        if let Some(view) = next_pending_view {
            if view.status == ViewStatus::Pending || view.status == ViewStatus::Planned {
                view.status = ViewStatus::Installing;
                return ViewInstallingStatus::Starting;
            } else {
                return ViewInstallingStatus::AlreadyInstalling;
            }
        }
        ViewInstallingStatus::NothingToInstall
    }

    /// Mark as installed the last view
    pub fn mark_installed(&mut self) {
        let view = self.views.get_mut(self.current_view_id + 1).unwrap();
        assert_eq!(
            view.status,
            ViewStatus::Installing,
            "Trying to mark as installed a view that is not installing. Status of next view is {:?}",
            view.status,
        );
        view.status = ViewStatus::Installed;
        self.current_view_id += 1;
        info!("View {} installed", self.current_view_id);
    }

    /// Returns the last installed view (not the one being installed)
    pub fn installed_view(&self) -> &View {
        self.views.get(self.current_view_id).unwrap()
    }

    pub fn installing_view(&self) -> Option<&View> {
        if self.views.len() == 1 {
            return self.views.first();
        }
        self.views.get(self.current_view_id + 1)
    }

    /// The set of members that are in the installed view and in the installing view
    pub fn stable_members_in_transition(&self) -> Option<Vec<&String>> {
        if let Some(installing_view) = self.installing_view() {
            let members = self
                .installed_view()
                .data
                .members
                .iter()
                .filter(|id| installing_view.data.members.contains(id))
                .collect();
            return Some(members);
        }
        None
    }

    pub fn stable_across_views(&self) -> Vec<&String> {
        let mut stable_members: Vec<&String> = self.installed_view().data.members.iter().collect();
        for i in self.current_view_id + 1..self.views.len() {
            stable_members = stable_members
                .iter()
                .filter(|id| self.views[i].data.members.contains(*id))
                .copied()
                .collect();
        }
        stable_members
    }

    pub fn installing_members(&self) -> Option<Vec<&String>> {
        if let Some(installing_view) = self.installing_view() {
            let members = installing_view.data.members.iter().collect();
            return Some(members);
        }
        None
    }

    /// Returns the members that are in the installed view but not in the installing view
    pub fn leaving_members(&self, id: &str) -> Vec<&String> {
        if let Some(installing_view) = self.installing_view() {
            let members: Vec<&String> = self
                .installed_view()
                .data
                .members
                .iter()
                .filter(|m| !installing_view.data.members.contains(m))
                .collect();
            if members.contains(&&id.to_string()) {
                let old_view_members: Vec<&String> = self
                    .installed_view()
                    .data
                    .members
                    .iter()
                    .filter(|m| *m != id)
                    .collect();
                old_view_members
            } else {
                members
            }
        } else {
            Vec::new()
        }
    }

    /// Returns the members that are in the installing view but not in the installed view
    pub fn joining_members(&self) -> Vec<&String> {
        if let Some(installing_view) = self.installing_view() {
            installing_view
                .data
                .members
                .iter()
                .filter(|id| !self.installed_view().data.members.contains(id))
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn planning(&mut self, view_id: usize) {
        assert!(view_id < self.views.len());
        let installing_id = self.installing_view().unwrap().data.id;
        for v in &mut self.views[installing_id..=view_id] {
            if v.status == ViewStatus::Pending {
                v.status = ViewStatus::Planned;
            }
        }
    }

    pub fn last_planned_id(&self) -> Option<usize> {
        let mut last_planned_id = None;
        for view in &self.views[self.current_view_id..] {
            if view.status == ViewStatus::Planned || view.status == ViewStatus::Installing {
                last_planned_id = Some(view.data.id);
            }
        }
        last_planned_id
    }

    pub fn last_view(&self) -> &View {
        self.views.last().unwrap()
    }

    pub fn views(&self) -> &[View] {
        &self.views
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::membership::{View, ViewStatus};

    #[test_log::test]
    fn test_stable_across_views() {
        let views = vec![
            View::new(
                0,
                vec!["a".to_string(), "b".to_string()],
                ViewStatus::Installed,
            ),
            View::new(
                1,
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                ViewStatus::Installing,
            ),
            View::new(
                2,
                vec!["a".to_string(), "c".to_string(), "d".to_string()],
                ViewStatus::Pending,
            ),
        ];
        let views = super::Views {
            views,
            current_view_id: 0,
        };
        let stable_members = views.stable_across_views();
        assert_eq!(stable_members, vec![&"a".to_string()]);
    }
}
