pub mod aw_set;
pub mod ewflag_set;
pub mod rw_set;

pub struct SetConfig {
    pub max_elements: usize,
}

impl Default for SetConfig {
    fn default() -> Self {
        Self { max_elements: 100 }
    }
}
