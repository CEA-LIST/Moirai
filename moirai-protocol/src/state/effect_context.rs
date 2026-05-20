#[cfg(not(feature = "sink"))]
use std::marker::PhantomData;

use crate::{event::id::EventId, state::sink::SinkCollector};

#[cfg(feature = "sink")]
use crate::state::{object_path::ObjectPath, sink::Sink};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectPathMode {
    Owned,
    Delegated,
}

pub struct EffectContext<'a> {
    #[cfg(feature = "sink")]
    path: ObjectPath,
    #[cfg(feature = "sink")]
    sink: Option<&'a mut SinkCollector>,
    #[cfg(feature = "sink")]
    mode: EffectPathMode,
    #[cfg(not(feature = "sink"))]
    _marker: PhantomData<&'a mut SinkCollector>,
}

impl<'a> EffectContext<'a> {
    pub fn root(root: &'static str, sink: Option<&'a mut SinkCollector>) -> Self {
        #[cfg(feature = "sink")]
        {
            Self {
                path: ObjectPath::new(root),
                sink,
                mode: EffectPathMode::Owned,
            }
        }

        #[cfg(not(feature = "sink"))]
        {
            let _ = root;
            let _ = sink;
            Self {
                _marker: PhantomData,
            }
        }
    }

    pub fn silent() -> Self {
        Self::root("silent", None)
    }

    pub fn is_owned(&self) -> bool {
        #[cfg(feature = "sink")]
        {
            self.mode == EffectPathMode::Owned
        }

        #[cfg(not(feature = "sink"))]
        {
            true
        }
    }

    pub fn create(&mut self) {
        #[cfg(feature = "sink")]
        self.collect(Sink::create(self.path.clone()));
    }

    pub fn update(&mut self) {
        #[cfg(feature = "sink")]
        self.collect(Sink::update(self.path.clone()));
    }

    pub fn delete(&mut self) {
        #[cfg(feature = "sink")]
        self.collect(Sink::delete(self.path.clone()));
    }

    pub fn with_owned<R>(&mut self, f: impl FnOnce(&mut EffectContext<'_>) -> R) -> R {
        #[cfg(feature = "sink")]
        {
            let path = self.path.clone();
            let mut child = self.child(path, EffectPathMode::Owned);
            f(&mut child)
        }

        #[cfg(not(feature = "sink"))]
        {
            f(self)
        }
    }

    pub fn with_delegated<R>(&mut self, f: impl FnOnce(&mut EffectContext<'_>) -> R) -> R {
        #[cfg(feature = "sink")]
        {
            let path = self.path.clone();
            let mut child = self.child(path, EffectPathMode::Delegated);
            f(&mut child)
        }

        #[cfg(not(feature = "sink"))]
        {
            f(self)
        }
    }

    pub fn with_field<R>(
        &mut self,
        name: &'static str,
        f: impl FnOnce(&mut EffectContext<'_>) -> R,
    ) -> R {
        #[cfg(feature = "sink")]
        {
            let path = self.path.clone().field(name);
            let mut child = self.child(path, EffectPathMode::Owned);
            f(&mut child)
        }

        #[cfg(not(feature = "sink"))]
        {
            let _ = name;
            f(self)
        }
    }

    pub fn with_list_element<R>(
        &mut self,
        id: impl FnOnce() -> EventId,
        f: impl FnOnce(&mut EffectContext<'_>) -> R,
    ) -> R {
        #[cfg(feature = "sink")]
        {
            let path = self.path.clone().list_element(id());
            let mut child = self.child(path, EffectPathMode::Owned);
            f(&mut child)
        }

        #[cfg(not(feature = "sink"))]
        {
            let _ = id;
            f(self)
        }
    }

    pub fn with_map_entry<R>(
        &mut self,
        key: impl FnOnce() -> String,
        f: impl FnOnce(&mut EffectContext<'_>) -> R,
    ) -> R {
        #[cfg(feature = "sink")]
        {
            let path = self.path.clone().map_entry(key());
            let mut child = self.child(path, EffectPathMode::Owned);
            f(&mut child)
        }

        #[cfg(not(feature = "sink"))]
        {
            let _ = key;
            f(self)
        }
    }

    pub fn with_variant<R>(
        &mut self,
        name: &'static str,
        f: impl FnOnce(&mut EffectContext<'_>) -> R,
    ) -> R {
        #[cfg(feature = "sink")]
        {
            let path = self.path.clone().variant(name);
            let mut child = self.child(path, EffectPathMode::Owned);
            f(&mut child)
        }

        #[cfg(not(feature = "sink"))]
        {
            let _ = name;
            f(self)
        }
    }

    #[cfg(feature = "sink")]
    fn collect(&mut self, sink: Sink) {
        if let Some(collector) = self.sink.as_deref_mut() {
            collector.collect(sink);
        }
    }

    #[cfg(feature = "sink")]
    fn child(&mut self, path: ObjectPath, mode: EffectPathMode) -> EffectContext<'_> {
        EffectContext {
            path,
            sink: self.sink.as_deref_mut(),
            mode,
        }
    }
}
