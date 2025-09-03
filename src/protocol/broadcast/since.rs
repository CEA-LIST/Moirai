use crate::protocol::{clock::version_vector::Version, event::id::EventId};

#[allow(dead_code)]
pub struct Since {
    version: Version,
    except: Vec<EventId>,
}
