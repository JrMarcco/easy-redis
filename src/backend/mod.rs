use crate::RespFrame;
use dashmap::DashMap;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Backend(Arc<BackendInner>);

#[derive(Debug, Default)]
pub struct BackendInner {
    pub(crate) map: DashMap<String, RespFrame>,
    pub(crate) hash_map: DashMap<String, DashMap<String, RespFrame>>,
}

impl Backend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, key: String, value: RespFrame) {
        self.map.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<RespFrame> {
        self.map.get(key).map(|v| v.value().clone())
    }

    pub fn hash_set(&self, key: String, field: String, value: RespFrame) {
        let map = self.hash_map.entry(key).or_default();
        map.insert(field, value);
    }

    pub fn hash_get(&self, key: &str, field: &str) -> Option<RespFrame> {
        self.hash_map
            .get(key)
            .and_then(|v| v.get(field).map(|v| v.value().clone()))
    }

    pub fn hash_get_all(&self, key: &str) -> Option<DashMap<String, RespFrame>> {
        self.hash_map.get(key).map(|v| v.clone())
    }
}

impl Deref for Backend {
    type Target = BackendInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for Backend {
    fn default() -> Self {
        Self(Arc::new(BackendInner::default()))
    }
}
