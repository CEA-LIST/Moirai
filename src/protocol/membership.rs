use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ViewStatus {
    Installing,
    Installed,
}

#[derive(Debug, Clone)]
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

    pub fn init(tcsb_id: &String) -> Self {
        Self {
            id: 0,
            members: vec![tcsb_id.clone()],
            status: ViewStatus::Installed,
        }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Views {
    views: Vec<View>,
}

impl Views {
    pub fn new() -> Self {
        Self { views: Vec::new() }
    }

    pub fn install(&mut self, members: Vec<String>, status: ViewStatus) {
        let id = self.views.len();
        self.views.push(View::new(id, members, status));
    }

    pub fn install_view(&mut self, view: View) {
        self.views.push(view);
    }

    pub fn get_current_view(&self) -> &View {
        if matches!(self.views.last().unwrap().status, ViewStatus::Installed) {
            self.views.last().unwrap()
        } else {
            self.views.get(self.views.len() - 2).unwrap()
        }
    }

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

    pub fn installed(&mut self) {
        self.views.last_mut().unwrap().status = ViewStatus::Installed;
    }

    pub fn is_member(&self, id: &String) -> bool {
        !self.views.last().unwrap().members.contains(id)
    }
}
