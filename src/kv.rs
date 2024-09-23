use std::collections::HashMap;

/// this doc test will NOT run unless under lib directory
/// the kv store
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// set a key value pair
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// get value for a key
    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    /// remove value of a key
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
