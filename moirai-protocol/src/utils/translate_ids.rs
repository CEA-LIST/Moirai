use crate::{
    event::id::EventId,
    replica::ReplicaIdx,
    state::sink::{ObjectPath, PathSegment},
    utils::intern_str::Interner,
};

pub trait TranslateIds: Clone {
    fn translate_ids(&self, from: ReplicaIdx, interner: &Interner) -> Self;
}

impl TranslateIds for EventId {
    fn translate_ids(&self, _from: ReplicaIdx, interner: &Interner) -> Self {
        let idx = interner.get(self.origin_id()).unwrap_or_else(|| {
            panic!(
                "Cannot translate embedded EventId for unknown replica origin {}",
                self.origin_id()
            )
        });
        EventId::new(idx, self.seq(), interner.resolver().clone())
    }
}

impl TranslateIds for PathSegment {
    fn translate_ids(&self, from: ReplicaIdx, interner: &Interner) -> Self {
        match self {
            PathSegment::Field(name) => PathSegment::Field(name),
            PathSegment::ListElement(id) => {
                PathSegment::ListElement(id.translate_ids(from, interner))
            }
            PathSegment::MapEntry(key) => PathSegment::MapEntry(key.clone()),
            PathSegment::Variant(name) => PathSegment::Variant(name),
        }
    }
}

impl TranslateIds for ObjectPath {
    fn translate_ids(&self, from: ReplicaIdx, interner: &Interner) -> Self {
        self.segments()
            .iter()
            .fold(ObjectPath::new(self.root()), |path, segment| match segment
                .translate_ids(from, interner)
            {
                PathSegment::Field(name) => path.field(name),
                PathSegment::ListElement(id) => path.list_element(id),
                PathSegment::MapEntry(key) => path.map_entry(key),
                PathSegment::Variant(name) => path.variant(name),
            })
    }
}

impl<T> TranslateIds for Box<T>
where
    T: TranslateIds,
{
    fn translate_ids(&self, from: ReplicaIdx, interner: &Interner) -> Self {
        Box::new((**self).translate_ids(from, interner))
    }
}

impl<T> TranslateIds for Vec<T>
where
    T: TranslateIds,
{
    fn translate_ids(&self, from: ReplicaIdx, interner: &Interner) -> Self {
        self.iter()
            .map(|item| item.translate_ids(from, interner))
            .collect()
    }
}

impl<T> TranslateIds for Option<T>
where
    T: TranslateIds,
{
    fn translate_ids(&self, from: ReplicaIdx, interner: &Interner) -> Self {
        self.as_ref().map(|item| item.translate_ids(from, interner))
    }
}
