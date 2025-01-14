use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ViewStatus {
    Installing,
    Installed,
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
/// - A view marked as installing is always the last one
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Views {
    views: Vec<View>,
}

impl Views {
    pub fn new() -> Self {
        Self { views: Vec::new() }
    }

    /// Create and install a new view with the given members and status
    pub fn install(&mut self, members: Vec<String>, status: ViewStatus) {
        let id = self.views.len();
        self.views.push(View::new(id, members, status));
    }

    /// Install the view given in parameter
    pub fn install_view(&mut self, view: View) {
        self.views.push(view);
    }

    /// Mark as installed the last view
    pub fn mark_installed(&mut self) {
        self.views.last_mut().unwrap().status = ViewStatus::Installed;
    }

    /// Returns the last installed view (not the one being installed)
    pub fn current_installed_view(&self) -> &View {
        if matches!(self.views.last().unwrap().status, ViewStatus::Installed) {
            self.views.last().unwrap()
        } else {
            self.views.get(self.views.len() - 2).unwrap()
        }
    }

    pub fn last_view(&self) -> &View {
        self.views.last().unwrap()
    }

    /// Returns the members of the last view (installed or installing)
    pub fn members(&self) -> &Vec<String> {
        &self.views.last().unwrap().members
    }

    pub fn leaving_members(&self) -> Vec<&String> {
        if matches!(self.views.last().unwrap().status, ViewStatus::Installing) {
            let last = self.views.last().unwrap();
            let previous = self.views.get(self.views.len() - 2).unwrap();
            previous
                .members
                .iter()
                .filter(|id| !last.members.contains(id))
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn joining_members(&self) -> Vec<&String> {
        if matches!(self.views.last().unwrap().status, ViewStatus::Installing) {
            let last = self.views.last().unwrap();
            let previous = self.views.get(self.views.len() - 2).unwrap();
            last.members
                .iter()
                .filter(|id| !previous.members.contains(id))
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn is_member(&self, id: &String) -> bool {
        !self.views.last().unwrap().members.contains(id)
    }
}

impl Default for Views {
    fn default() -> Self {
        Self::new()
    }
}
