use crossbeam_skiplist::SkipMap;
use log::error;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::KvsEngine;
use crate::{KvsError, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A skip list in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// use kvs::KvsEngine;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```

#[derive(Clone)]
pub struct KvStore {
    path: Arc<PathBuf>,
    // key_2_set_command_position, does not store "remove" command position
    key_2_cmd_pos: Arc<SkipMap<String, CommandPos>>,
    reader: KvStoreReader,
    writer: Arc<Mutex<KvStoreWriter>>,
}

/// A single thread reader.
///
/// Each `KvStore` instance has its own `KvStoreReader` and
/// `KvStoreReader`s open the same files separately. So the user
/// can read concurrently through multiple `KvStore`s in different
/// threads.
struct KvStoreReader {
    path: Arc<PathBuf>,
    // last compaction gen
    safe_point: Arc<AtomicU64>,
    gen_2_reader: RefCell<BTreeMap<u64, BufReaderWithPos<File>>>,
}

impl Clone for KvStoreReader {
    fn clone(&self) -> Self {
        KvStoreReader {
            path: self.path.clone(),
            safe_point: self.safe_point.clone(),
            gen_2_reader: RefCell::new(BTreeMap::new()),
        }
    }
}

impl KvStoreReader {
    /// 过一遍当前线程的 gen_2_reader, 和全局 safe_point 比一下
    /// 如果当前线程本地的所有 old gen >= 全局的 safe_point, 那就啥也不动
    /// 如果当前线程本地的所有 old gen < 全局的 safe_point
    /// => 说明 compaction gen 已经更新了版本， 当前线程的 gen_2_reader 没用了， 可以全部清理掉
    fn close_stale_handles(&self) {
        let mut gen_2_reader = self.gen_2_reader.borrow_mut();
        while !gen_2_reader.is_empty() {
            let first_gen = *gen_2_reader.keys().next().unwrap();
            if first_gen >= self.safe_point.load(Ordering::SeqCst) {
                break;
            }
            gen_2_reader.remove(&first_gen);
        }
    }

    /// read and then do something
    /// cmd_pos => reader
    /// f 定制 reader => ?
    fn read_and<F, R>(&self, cmd_pos: CommandPos, f: F) -> Result<R>
    where
        F: FnOnce(io::Take<&mut BufReaderWithPos<File>>) -> Result<R>,
    {
        self.close_stale_handles();
        let mut gen_2_reader = self.gen_2_reader.borrow_mut();
        // if it's a new gen: init the corresponding reader
        if !gen_2_reader.contains_key(&cmd_pos.gen) {
            let reader = BufReaderWithPos::new(File::open(log_path(&self.path, cmd_pos.gen))?)?;
            gen_2_reader.insert(cmd_pos.gen, reader);
        }
        let reader = gen_2_reader.get_mut(&cmd_pos.gen).unwrap();
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let cmd_reader = reader.take(cmd_pos.len);
        f(cmd_reader)
    }
    fn read_command(&self, cmd_pos: CommandPos) -> Result<Command> {
        self.read_and(cmd_pos, |cmd_reader| {
            Ok(serde_json::from_reader(cmd_reader)?)
        })
    }
}

struct KvStoreWriter {
    reader: KvStoreReader,
    writer: BufWriterWithPos<File>,
    cur_gen: u64,
    uncompacted: u64,
    path: Arc<PathBuf>,
    key_2_cmd_pos: Arc<SkipMap<String, CommandPos>>,
}

impl KvStoreWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        // write file
        // update key_2_cmd_pos + uncompacted
        // try compact
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            match self.key_2_cmd_pos.get(&key) {
                Some(old_cmd) => {
                    // println!("value: {:?}", old_cmd);
                    self.uncompacted += old_cmd.value().len;
                }
                None => {
                    // println!("None");
                }
            }
            let neo_pos: CommandPos = (self.cur_gen, pos..self.writer.pos).into();
            self.key_2_cmd_pos.insert(key, neo_pos);
        }
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }
    fn remove(&mut self, key: String) -> Result<()> {
        // write file
        // update key_2_cmd_pos + uncompacted
        // try compact
        if self.key_2_cmd_pos.contains_key(&key) {
            let cmd = Command::remove(key);
            let pos = self.writer.pos;
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Command::Remove { key } = cmd {
                let old_cmd = self.key_2_cmd_pos.remove(&key).expect("key not found");
                self.uncompacted += old_cmd.value().len;
                let remove_cmd_len = self.writer.pos - pos;
                self.uncompacted += remove_cmd_len;
            }
            if self.uncompacted > COMPACTION_THRESHOLD {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
    fn compact(&mut self) -> Result<()> {
        let compaction_gen = self.cur_gen + 1;
        let mut compaction_writer = new_log_writer(&self.path, compaction_gen)?;
        self.cur_gen += 2;
        self.writer = new_log_writer(&self.path, self.cur_gen)?;
        let mut new_pos = 0;
        for entry in self.key_2_cmd_pos.iter() {
            // 写到 compaction gen 里， 返回 len
            let cmd_len = self.reader.read_and(*entry.value(), |mut cmd_reader| {
                Ok(io::copy(&mut cmd_reader, &mut compaction_writer)?)
            })?;
            // 更新 self.key_2_cmd_pos
            self.key_2_cmd_pos.insert(
                entry.key().clone(),
                (compaction_gen, new_pos..new_pos + cmd_len).into(),
            );
            // 更新最新 pos
            new_pos += cmd_len;
        }
        // 更新一波 compaction_gen
        compaction_writer.flush()?;
        self.reader
            .safe_point
            .store(compaction_gen, Ordering::SeqCst);
        // 清理一波 stale_handle
        // 先清理 file handle (readers)
        self.reader.close_stale_handles();
        //
        let stale_gens = sorted_gen_list(&self.path)?
            .into_iter()
            .filter(|&gen| gen < compaction_gen);
        for stale_gen in stale_gens {
            let file_path = log_path(&self.path, stale_gen);
            if let Err(e) = fs::remove_file(&file_path) {
                error!("{:?} cannot be deleted: {}", file_path, e);
            }
        }
        self.uncompacted = 0;
        Ok(())
    }
}

fn new_log_writer(path: &Path, gen: u64) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    Ok(writer)
}

#[derive(Debug, Copy, Clone)]
struct CommandPos {
    // file gen
    gen: u64,
    // cursor position
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

#[derive(Debug)]
struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos: pos,
        })
    }
}
impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}
impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl KvStore {
    /// init an instance by opening a new path
    /// This will create a new directory if the given one does not exist.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        // traverse dir path, load every file into memory
        let data = path.into();
        let path = Arc::new(data);
        fs::create_dir_all(&*path)?;
        let mut gen_2_reader: BTreeMap<u64, BufReaderWithPos<File>> = BTreeMap::new();
        let key_2_cmd_pos: Arc<SkipMap<String, CommandPos>> = Arc::new(SkipMap::new());
        let gen_list = sorted_gen_list(&path)?;
        let mut uncompacted = 0;
        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            uncompacted += load_file_into_hashmap(gen, &mut reader, &*key_2_cmd_pos)?;
            gen_2_reader.insert(gen, reader);
        }
        // init cur_gen, writer, reader, safe_point, everything
        let cur_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, cur_gen)?;
        let safe_point = Arc::new(AtomicU64::new(0));
        let reader = KvStoreReader {
            path: Arc::clone(&path),
            safe_point,
            gen_2_reader: RefCell::new(gen_2_reader),
        };
        let writer = KvStoreWriter {
            reader: reader.clone(),
            writer,
            cur_gen,
            uncompacted,
            path: Arc::clone(&path),
            key_2_cmd_pos: Arc::clone(&key_2_cmd_pos),
        };
        Ok(KvStore {
            path,
            reader,
            key_2_cmd_pos,
            writer: Arc::new(Mutex::new(writer)),
        })
    }
}

/// Gets the string value of a given string key.
///
/// Returns `None` if the given key does not exist.
impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }
    fn get(&self, key: String) -> Result<Option<String>> {
        // 有 cmd_pos 就试试能不能取出来， 取不出来就是文件有问题
        // 这里如果 remove 了还能不能取出来？
        //   如果最新命令是 remove, 那么 key_2_cmd_pos 里不会有这个 key, 但文件里仍会存 remove 命令
        if let Some(cmd_pos) = self.key_2_cmd_pos.get(&key) {
            if let Command::Set { value, .. } = self.reader.read_command(*cmd_pos.value())? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        // 没 cmd_pos 就是新值拿不到
        } else {
            Ok(None)
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

fn new_log_file(path: &Path, gen: u64) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, gen);
    let writer = BufWriterWithPos::new(OpenOptions::new().create(true).write(true).open(&path)?)?;
    Ok(writer)
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

fn load_file_into_hashmap(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    key_2_cmd_pos: &SkipMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd_entry) = key_2_cmd_pos.get(&key) {
                    uncompacted += old_cmd_entry.value().len;
                }
                key_2_cmd_pos.insert(key, (gen, pos..new_pos).into());
            }
            Command::Remove { key } => {
                if let Some(old_cmd_entry) = key_2_cmd_pos.remove(&key) {
                    uncompacted += old_cmd_entry.value().len;
                }
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }
    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort();
    Ok(gen_list)
}
