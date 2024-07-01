use crate::RespFrame;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Backend(Arc<BackendInner>);

#[derive(Debug, Default)]
pub struct BackendInner {
    pub(crate) map: DashMap<String, RespFrame>,
    pub(crate) hash_map: DashMap<String, DashMap<String, RespFrame>>,
    pub(crate) list_map: DashMap<String, VecDeque<RespFrame>>,
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

    pub fn hset(&self, key: String, field: String, value: RespFrame) {
        let map = self.hash_map.entry(key).or_default();
        map.insert(field, value);
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<RespFrame> {
        self.hash_map
            .get(key)
            .and_then(|ret| ret.get(field).map(|v| v.value().clone()))
    }

    pub fn hgetall(&self, key: &str) -> Option<DashMap<String, RespFrame>> {
        self.hash_map.get(key).map(|v| v.clone())
    }

    pub fn lpush(&self, key: String, value: RespFrame) {
        self.list_map.entry(key).or_default().push_front(value);
    }

    pub fn lpop(&self, key: &str) -> Option<RespFrame> {
        let mut ret = self.list_map.get_mut(key)?;
        ret.value_mut().pop_front()
    }

    pub fn rpush(&self, key: String, value: RespFrame) {
        self.list_map.entry(key).or_default().push_back(value);
    }

    pub fn rpop(&self, key: &str) -> Option<RespFrame> {
        let mut ret = self.list_map.get_mut(key)?;
        ret.value_mut().pop_back()
    }

    pub fn llen(&self, key: &str) -> Option<usize> {
        self.list_map.get(key).map(|ret| ret.value().len())
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
