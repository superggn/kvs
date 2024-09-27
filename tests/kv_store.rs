use kvs::{KvStore, Result};
use tempfile::TempDir;
use walkdir::WalkDir;

#[test]
fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temp work dir!");
    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));
    drop(store);
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));
    Ok(())
}

#[test]
fn overwrite_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temp work dir!");
    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    store.set("key1".to_owned(), "value2".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));

    drop(store);
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));
    store.set("key1".to_owned(), "value3".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value3".to_owned()));
    Ok(())
}

#[test]
fn get_non_existent_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temp work dir!");
    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key2".to_owned())?, None);

    drop(store);
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key2".to_owned())?, None);
    Ok(())
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temp work dir!");
    let mut store = KvStore::open(temp_dir.path())?;
    assert!(store.remove("key1".to_owned()).is_err());
    Ok(())
}

#[test]
fn remove_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temp work dir!");
    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    assert!(store.remove("key1".to_owned()).is_ok());
    assert_eq!(store.get("key1".to_owned())?, None);
    Ok(())
}

#[test]
fn compaction() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temp work dir!");
    let mut store = KvStore::open(temp_dir.path())?;
    let dir_size = || {
        let entries = WalkDir::new(temp_dir.path()).into_iter();
        // 这里是文件的长度
        let len: walkdir::Result<u64> = entries
            .map(|res| {
                res.and_then(|entry| {
                    println!("entry: {:?}", &entry);
                    entry.metadata()
                })
                .map(|metadata| {
                    println!("metadata: {:?}", &metadata);
                    println!("====================");
                    println!("metadata.len(): {:?}", &metadata.len());
                    println!("====================");
                    metadata.len()
                })
            })
            .sum();
        len.expect("fail to get dir size")
    };
    let mut cur_size = dir_size();
    for iter in 0..1000 {
        // 若成功触发压缩， 在压缩后校验数据， 若数据正确， 可以 return
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            let value = format!("{}", iter);
            store.set(key, value)?;
        }
        let new_size = dir_size();
        if new_size > cur_size {
            cur_size = new_size;
            continue;
        }
        // compaction triggered
        drop(store);
        let mut store = KvStore::open(temp_dir.path())?;
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            assert_eq!(store.get(key)?, Some(format!("{}", iter)));
        }
        return Ok(());
    }
    panic!("no compaction detected");
}
