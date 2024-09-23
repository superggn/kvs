use kvs::KvStore;

/// 进行如下测试
/// set key value
/// get key
/// overwrite
/// remove

#[test]
fn get_stored_value() {
    let mut store = KvStore::new();
    store.set("key1".to_owned(), "value1".to_owned());
    store.set("key2".to_owned(), "value2".to_owned());

    assert_eq!(store.get("key1".to_owned()).unwrap(), "value1".to_owned());
    assert_eq!(store.get("key2".to_owned()).unwrap(), "value2".to_owned());
}

#[test]
fn overwrite_value() {
    let mut store = KvStore::new();
    store.set("key1".to_owned(), "value1".to_owned());
    assert_eq!(store.get("key1".to_owned()).unwrap(), "value1".to_owned());

    store.set("key1".to_owned(), "value2".to_owned());
    assert_eq!(store.get("key1".to_owned()).unwrap(), "value2".to_owned());
}

#[test]
fn get_non_existent_value() {
    let mut store = KvStore::new();
    store.set("key1".to_owned(), "value1".to_owned());
    assert_eq!(store.get("key2".to_owned()), None);
}

#[test]
fn remove_key() {
    let mut store = KvStore::new();
    store.set("key1".to_owned(), "value1".to_owned());
    assert_eq!(store.get("key1".to_owned()).unwrap(), "value1".to_owned());

    store.remove("key1".to_owned());
    assert_eq!(store.get("key1".to_owned()), None);
}
