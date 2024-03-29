use crate::{context::Context, minhash::MinHash};

use anyhow::Result;
use cityhasher::hash;
use log::debug;
use speedy::{IsEof, Readable, Writable};
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    fs::{read_dir, File},
    io::{BufReader, Write},
    mem,
    path::Path,
    slice,
};
use uuid::Uuid;
use zstd::stream;

// Settings to find duplicates with Jaccard similarity 0.8
const LSH_RANGE: usize = 15;
const LSH_BUCKETS: usize = 17;
const LSH_LAST: usize = LSH_RANGE * LSH_BUCKETS;

#[derive(Eq, PartialEq, Ord, PartialOrd)]
pub struct LshBucket {
    index: u8,
    hash: u64,
}

impl LshBucket {
    pub fn index(&self) -> u8 {
        self.index
    }

    pub fn hash(&self) -> u64 {
        self.hash
    }
}

pub fn create_lsh_buckets(minhash: &MinHash) -> Vec<LshBucket> {
    let mut result = Vec::new();
    for (index, start) in (0..LSH_LAST).step_by(LSH_RANGE).enumerate() {
        let slice = &minhash[start..(start + LSH_RANGE)];
        let bytes: &[u8] = unsafe {
            slice::from_raw_parts(
                slice.as_ptr() as *const u8,
                slice.len() * mem::size_of_val(&slice[0]),
            )
        };
        assert!(start <= 255 && index <= LSH_BUCKETS);
        result.push(LshBucket {
            index: index as u8,
            hash: hash::<u64>(bytes),
        });
    }
    result
}

#[derive(Readable, Writable)]
pub struct LshBucketsMeta {
    files: Vec<String>,
    column_name: String,
    file_prefix: String,
}

impl LshBucketsMeta {
    pub fn files(&self) -> &Vec<String> {
        &self.files
    }

    pub fn file_prefix(&self) -> &String {
        &self.file_prefix
    }

    pub fn column_name(&self) -> &String {
        &self.column_name
    }
}

#[repr(packed)]
#[derive(Eq, PartialEq, Ord, PartialOrd, Readable, Writable, Copy, Clone)]
pub struct LshBucketRow {
    bucket_index: u8,
    bucket_hash: u64,
    // path hash is too small, but it is needed to show diff between texts,
    // it speed up searching content by content_hash
    path_hash: u16,
    content_hash: u64,
}

impl LshBucketRow {
    pub fn new(bucket_index: u8, bucket_hash: u64, path_hash: u16, content_hash: u64) -> Self {
        Self {
            bucket_index,
            bucket_hash,
            path_hash,
            content_hash,
        }
    }

    pub fn bucket_index(&self) -> u8 {
        self.bucket_index
    }

    pub fn bucket_hash(&self) -> u64 {
        self.bucket_hash
    }

    pub fn path_hash(&self) -> u16 {
        self.path_hash
    }

    pub fn content_hash(&self) -> u64 {
        self.content_hash
    }
}

pub struct LshBucketRowsFilesWriter {
    folder: String,
    meta: LshBucketsMeta,
    rows: Vec<LshBucketRow>,
    buckets_size_limit: u64,
}

impl LshBucketRowsFilesWriter {
    pub fn new(folder: String, buckets_size_limit: u64) -> Self {
        Self {
            folder,
            meta: LshBucketsMeta {
                files: Vec::new(),
                column_name: String::new(),
                file_prefix: String::new(),
            },
            rows: Vec::new(),
            buckets_size_limit,
        }
    }

    pub fn write_rows(
        &mut self,
        source_file: &String,
        column_name: &String,
        rows: Vec<LshBucketRow>,
    ) -> Result<()> {
        self.rows.extend(rows);
        self.meta.files.push(source_file.clone());

        assert!(self.meta.column_name.is_empty() || self.meta.column_name == *column_name);
        self.meta.column_name = column_name.clone();

        if !self.rows.is_empty()
            && (mem::size_of_val(&self.rows[0]) as u64) * (self.rows.len() as u64)
                >= self.buckets_size_limit
        {
            self.flush()?;
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }

        let file_prefix = format!("{}", Uuid::new_v4());

        let file_name = Context::canonicalize(&format!("{}/{}.lsh_rows", self.folder, file_prefix));
        assert!(!Path::new(&file_name).exists());

        debug!(
            "Started writing lsh rows file: {}, processed files num: {}, rows num: {}",
            file_name,
            self.meta.files.len(),
            self.rows.len()
        );

        let mut file = stream::write::Encoder::new(File::create(file_name.clone())?, 1)?;

        self.meta.files.sort();
        self.meta.file_prefix = file_prefix.clone();
        self.rows.sort();

        for row in &self.rows {
            row.write_to_stream(&mut file)?;
        }

        file.flush()?;

        debug!("Stopped writing lsh rows file: {}", file_name);

        let meta_file_name =
            Context::canonicalize(&format!("{}/{}.lsh_meta", self.folder, file_prefix));
        debug!("Started writing lsh meta file: {}", meta_file_name);

        let mut meta_file = stream::write::Encoder::new(File::create(meta_file_name.clone())?, 1)?;
        self.meta.write_to_stream(&mut meta_file)?;

        meta_file.flush()?;

        debug!("Stopped writing lsh meta file: {}", meta_file_name);

        self.meta.files.clear();
        self.rows.clear();

        Ok(())
    }
}

pub struct LshBucketRowsFileReader<'a> {
    reader: stream::read::Decoder<'a, BufReader<File>>,
    prev: Option<LshBucketRow>,
}

impl LshBucketRowsFileReader<'_> {
    pub fn new(path: &String) -> Result<Self> {
        let file = File::open(&path)?;
        let reader = stream::read::Decoder::new(file)?;
        Ok(Self { reader, prev: None })
    }

    pub fn next(&mut self) -> Result<Option<LshBucketRow>> {
        let result = LshBucketRow::read_from_stream_unbuffered(&mut self.reader);
        if result.as_ref().is_err_and(|e| e.is_eof()) {
            return Ok(None);
        }
        let result = result?;

        if let Some(prev) = self.prev {
            assert!(
                prev.bucket_index < result.bucket_index
                    || (prev.bucket_index == result.bucket_index
                        && prev.bucket_hash <= result.bucket_hash)
            );
        }
        self.prev = Some(result.clone());
        Ok(Some(result))
    }
}

#[derive(Eq, PartialEq)]
struct ReverseOrderedLshBucketRow {
    data: LshBucketRow,
}

impl Ord for ReverseOrderedLshBucketRow {
    fn cmp(&self, other: &Self) -> Ordering {
        // Notice that the we flip the ordering on LshBucketRows.
        // to make implementations of `PartialEq` and `Ord` consistent.
        other.data.cmp(&self.data)
    }
}

// `PartialOrd` needs to be implemented as well.
impl PartialOrd for ReverseOrderedLshBucketRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct LshBucketRowsFilesMerger<'a> {
    readers: Vec<LshBucketRowsFileReader<'a>>,
    heap: BinaryHeap<(ReverseOrderedLshBucketRow, usize)>,
    prev: Option<LshBucketRow>,
}

impl LshBucketRowsFilesMerger<'_> {
    pub fn new(folder: &String) -> Result<Self> {
        let list = read_dir(folder)?;
        let mut readers = Vec::new();
        for path in list {
            let path = path.unwrap().path().display().to_string();
            if !path.ends_with(".lsh_rows") {
                continue;
            }
            readers.push(LshBucketRowsFileReader::new(&path)?);
        }

        let mut heap = BinaryHeap::new();
        for (index, reader) in readers.iter_mut().enumerate() {
            let next = reader.next()?;
            if next.is_some() {
                heap.push((
                    ReverseOrderedLshBucketRow {
                        data: next.unwrap(),
                    },
                    index,
                ));
            }
        }

        Ok(LshBucketRowsFilesMerger {
            readers,
            heap,
            prev: None,
        })
    }

    pub fn has_data_left(&mut self) -> bool {
        !self.heap.is_empty()
    }

    pub fn next(&mut self) -> Result<LshBucketRow> {
        let top = self.heap.pop().unwrap();
        let reader = &mut self.readers[top.1];
        let next = reader.next()?;
        if next.is_some() {
            self.heap.push((
                ReverseOrderedLshBucketRow {
                    data: next.unwrap(),
                },
                top.1,
            ))
        }
        let result = top.0.data;
        assert!(self.prev.is_none() || self.prev.unwrap() <= result);
        self.prev = Some(result.clone());
        Ok(result)
    }
}
