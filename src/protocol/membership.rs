use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ViewStatus {
    Installing,
    Installed,
    Pending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct View {
    pub id: usize,
    pub members: Vec<String>,
    pub status: ViewStatus,
}

impl View {
    pub fn new(id: usize, members: Vec<String>, status: ViewStatus) -> Self {
        Self {
            id,
            members,
            status,
        }
    }

    pub fn init(tcsb_id: &str) -> Self {
        Self {
            id: 0,
            members: vec![tcsb_id.to_string()],
            status: ViewStatus::Installed,
        }
    }
}

/// Invariants:
/// - There is always at least one view in the list
/// - There may be zero or one view being installed
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
        self.views.push(view);
    }

    pub fn start_installing(&mut self) -> bool {
        let next_pending_view = self.views.get_mut(self.current_view_id + 1);
        if let Some(view) = next_pending_view {
            if view.status == ViewStatus::Pending {
                view.status = ViewStatus::Installing;
                return true;
            }
        }
        false
    }

    /// Mark as installed the last view
    pub fn mark_installed(&mut self) {
        assert!(
            self.views.get_mut(self.current_view_id + 1).unwrap().status == ViewStatus::Installing
        );
        self.views.get_mut(self.current_view_id + 1).unwrap().status = ViewStatus::Installed;
        self.current_view_id += 1;
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
                .members
                .iter()
                .filter(|id| installing_view.members.contains(id))
                .collect();
            return Some(members);
        }
        None
    }

    pub fn installing_members(&self) -> Option<Vec<&String>> {
        if let Some(installing_view) = self.installing_view() {
            let members = installing_view.members.iter().collect();
            return Some(members);
        }
        None
    }

    /// Returns the members that are in the installed view but not in the installing view
    pub fn leaving_members(&self) -> Vec<&String> {
        if let Some(installing_view) = self.installing_view() {
            self.installed_view()
                .members
                .iter()
                .filter(|id| !installing_view.members.contains(id))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Returns the members that are in the installing view but not in the installed view
    pub fn joining_members(&self) -> Vec<&String> {
        if let Some(installing_view) = self.installing_view() {
            installing_view
                .members
                .iter()
                .filter(|id| !self.installed_view().members.contains(id))
                .collect()
        } else {
            Vec::new()
        }
    }
}
