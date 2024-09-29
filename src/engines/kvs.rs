use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{KvsError, Result};
use std::ffi::OsStr;

use super::KvsEngine;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `BTreeMap` in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```

// #[default]
pub struct KvStore {
    path: PathBuf,
    gen_2_reader: HashMap<u64, BufReaderWithPos<File>>,
    writer: BufWriterWithPos<File>, // cur gen writer
    cur_gen: u64,
    key_2_cmd_pos: BTreeMap<String, CommandPos>,
    uncompacted: u64,
}

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
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        fs::create_dir_all(&path).unwrap();
        let mut gen_2_reader: HashMap<u64, BufReaderWithPos<File>> = HashMap::new();
        let mut key_2_cmd_pos: BTreeMap<String, CommandPos> = BTreeMap::new();
        let gen_list = sorted_gen_list(&path)?;
        let mut uncompacted = 0;
        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(get_log_path(&path, gen))?)?;
            uncompacted += load_file_into_hashmap(gen, &mut reader, &mut key_2_cmd_pos)?;
            gen_2_reader.insert(gen, reader);
        }
        let cur_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, cur_gen, &mut gen_2_reader)?;
        Ok(KvStore {
            path,
            gen_2_reader,
            writer,
            cur_gen,
            key_2_cmd_pos,
            uncompacted,
        })
    }

    /// set a kv pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // todo 加 return type
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd) = self
                .key_2_cmd_pos
                .insert(key, (self.cur_gen, pos..self.writer.pos).into())
            {
                self.uncompacted += old_cmd.len
            }
        }
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?
        };
        Ok(())
    }

    /// result 是给 file 中的字符串格式的， 识别不了的字符串标一下 Err
    /// Option 是给 value 的， 存过这个 key 就是 Some, 没存过这个 key 那就是 None
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.key_2_cmd_pos.get(&key) {
            let reader = self
                .gen_2_reader
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// remove a key from storage
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.key_2_cmd_pos.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            // 这里包一层 if let 不直接用 key 是因为刚才 key 已经被 move 消耗掉了， 现在重新取出来
            if let Command::Remove { key } = cmd {
                let old_cmd = self.key_2_cmd_pos.remove(&key).expect("key not found");
                self.uncompacted += old_cmd.len;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// compact the storage files into a single compaction file
    pub fn compact(&mut self) -> Result<()> {
        let compaction_gen = self.cur_gen + 1;
        let mut compaction_writer = self.new_log_file(compaction_gen)?;
        self.cur_gen += 2;
        self.writer = self.new_log_file(self.cur_gen)?;
        let mut new_pos = 0;
        for cmd_pos in &mut self.key_2_cmd_pos.values_mut() {
            let reader = self
                .gen_2_reader
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }
            let mut entry_reader = reader.take(cmd_pos.len);
            let len = io::copy(&mut entry_reader, &mut compaction_writer)?;
            *cmd_pos = (compaction_gen, new_pos..new_pos + len).into();
            new_pos += len;
        }
        compaction_writer.flush()?;
        let stale_gens: Vec<_> = self
            .gen_2_reader
            .keys()
            .filter(|&&gen| gen < compaction_gen)
            .cloned()
            .collect();
        for gen in stale_gens {
            self.gen_2_reader.remove(&gen);
            fs::remove_file(get_log_path(&self.path, gen))?;
        }
        self.uncompacted = 0;
        Ok(())
    }

    fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.path, gen, &mut self.gen_2_reader)
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd) = self
                .key_2_cmd_pos
                .insert(key, (self.cur_gen, pos..self.writer.pos).into())
            {
                self.uncompacted += old_cmd.len;
            }
        }
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.key_2_cmd_pos.get(&key) {
            let reader = self
                .gen_2_reader
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.key_2_cmd_pos.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Command::Remove { key } = cmd {
                let old_cmd = self.key_2_cmd_pos.remove(&key).expect("key not found");
                self.uncompacted += old_cmd.len;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

fn new_log_file(
    path: &Path,
    gen: u64,
    gen_2_reader: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let log_path = get_log_path(path, gen);
    let inner = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&log_path)?;
    let writer = BufWriterWithPos::new(inner)?;
    gen_2_reader.insert(gen, BufReaderWithPos::new(File::open(&log_path)?)?);
    Ok(writer)
}

fn get_log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

fn load_file_into_hashmap(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    key_2_cmd_pos: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                let cmd_pos = (gen, pos..new_pos).into();
                if let Some(old_cmd) = key_2_cmd_pos.insert(key, cmd_pos) {
                    uncompacted += old_cmd.len;
                }
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = key_2_cmd_pos.remove(&key) {
                    uncompacted += old_cmd.len;
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
