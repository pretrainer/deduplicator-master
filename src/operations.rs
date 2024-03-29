use crate::{context::Context, diff, lsh::LshBucketsMeta};
use anyhow::Result;
use cityhasher::hash;
use indicatif::ProgressBar;
use log::{debug, error, info, warn};
use rand::{prelude::SliceRandom, thread_rng};
use speedy::{IsEof, Readable, Writable};
use std::{
    cmp,
    collections::{HashMap, HashSet},
    fs::{canonicalize, create_dir_all, read_dir, remove_file, File},
    io::{BufWriter, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use threadpool::ThreadPool;
use zstd::stream;

use crate::lsh::{
    create_lsh_buckets, LshBucketRow, LshBucketRowsFilesMerger, LshBucketRowsFilesWriter,
};
use crate::{
    minhash::hash_text,
    parquet_io::{ParquetReader, ParquetWriter},
};

fn parquet_file_to_lsh_rows(path: &String, column_name: &String) -> Result<Vec<LshBucketRow>> {
    let path = canonicalize(path)?.display().to_string();
    debug!("Started processing file to lsh rows, file: {}", path);

    let path_hash = Context::hash_path(&path);

    let mut result = Vec::new();

    let mut parquet_reader = ParquetReader::try_new(&path, column_name)?;
    while parquet_reader.has_data_left()? {
        let text = parquet_reader.next()?;
        let content_hash = hash::<u64>(&text);
        let minhash = hash_text(&text);
        let lsh_buckets = create_lsh_buckets(&minhash);
        let rows = lsh_buckets
            .iter()
            .map(|x| LshBucketRow::new(x.index(), x.hash(), path_hash, content_hash));
        result.extend(rows);
    }

    debug!("Stopped processing file to lsh rows, file: {}", path);

    Ok(result)
}

fn process_parquet_files_to_lsh_bucket_rows_files(
    paths: Vec<String>,
    column_name: String,
    output_folder: String,
    lsh_buckets_size_limit: u64,
    progress_bar: &ProgressBar,
) -> Result<()> {
    let mut writer = LshBucketRowsFilesWriter::new(output_folder.clone(), lsh_buckets_size_limit);

    for path in paths {
        let rows = parquet_file_to_lsh_rows(&path, &column_name)?;
        writer.write_rows(&path, &column_name, rows)?;
        progress_bar.inc(1);
    }

    writer.flush()?;

    Ok(())
}

pub fn process_parquet_files_from_folder_to_lsh_buckets_files(
    context: &Context,
    column_name: &String,
    lsh_buckets_size_limit: u64,
    n_workers: usize,
) -> Result<()> {
    let input_files = context.input_files();

    let output_folder = context.raw_lsh_buckets_folder_path();
    info!(
        "Starting building lsh rows files from dir {}, total files: {}",
        context.input_folder(),
        input_files.len()
    );

    create_dir_all(&output_folder)?;

    let mut processed_input_files = HashSet::new();
    let mut known_lsh_rows_files = HashSet::new();

    let list_output_files: Vec<String> = read_dir(&output_folder)?
        .map(|path| path.unwrap().path().display().to_string())
        .map(|path| Context::canonicalize(&path))
        .collect();
    for path in &list_output_files {
        if !path.ends_with(".lsh_meta") {
            continue;
        }
        let decoder = stream::Decoder::new(File::open(path)?)?;
        let meta = LshBucketsMeta::read_from_stream_unbuffered(decoder)?;
        if meta.column_name() != column_name {
            warn!(
                "Column name {} is different with {}, so {} is removed",
                meta.column_name(),
                column_name,
                path
            );
            remove_file(path)?;
            continue;
        }
        for file in meta.files() {
            debug!("{} is already processed to lsh_rows, will be skipped", file);
            processed_input_files.insert(file.clone());
        }
        known_lsh_rows_files.insert(format!("{}.lsh_rows", meta.file_prefix()));
    }

    for path in &list_output_files {
        if !path.ends_with(".lsh_rows") {
            continue;
        }
        let file_name = Path::new(path).file_name().unwrap();
        if !known_lsh_rows_files.contains(file_name.to_str().unwrap()) {
            warn!("Cannot find {} in known files, so remove it", path);
            std::fs::remove_file(path)?;
        }
    }

    let mut input_files: Vec<String> = input_files
        .iter()
        .filter(|v| !processed_input_files.contains(*v))
        .map(|v| v.clone())
        .collect();
    input_files.shuffle(&mut thread_rng());

    let progress_bar = Arc::new(ProgressBar::new(input_files.len() as u64));
    let pool = ThreadPool::new(n_workers);

    let num_files_per_worker = cmp::max(input_files.len() / n_workers, 1);
    for start in (0..input_files.len()).step_by(num_files_per_worker) {
        let worker_files = Vec::from(
            &input_files[start..cmp::min(input_files.len(), start + num_files_per_worker)],
        );
        let column_name = column_name.clone();
        let output_folder = output_folder.clone();
        let progress_bar = progress_bar.clone();
        let worker = move || {
            let result = process_parquet_files_to_lsh_bucket_rows_files(
                worker_files,
                column_name,
                output_folder,
                lsh_buckets_size_limit,
                &*progress_bar,
            );
            if result.is_err() {
                error!("{}", result.err().unwrap());
                panic!();
            }
        };
        pool.execute(worker);
    }
    pool.join();

    progress_bar.finish();

    info!(
        "Stopped building lsh rows files from dir {}",
        context.input_folder()
    );

    Ok(())
}

#[derive(Readable, Writable, Debug)]
struct DuplicatesGroupItem {
    path_hash: u16,
    content_hash: u64,
}

#[derive(Readable, Writable, Debug)]
struct DuplicatesGroup {
    group: Vec<DuplicatesGroupItem>,
}

pub fn find_duplicates_in_lsh_buckets_files(
    input_folder: &String,
    output_file: &String,
) -> Result<()> {
    info!("Starting finding duplicates in folder {}", input_folder);

    let mut merger = LshBucketRowsFilesMerger::new(input_folder)?;
    let mut output_writer = stream::Encoder::new(BufWriter::new(File::create(output_file)?), 1)?;

    let mut flush = |group: &mut Vec<LshBucketRow>| -> Result<()> {
        group.sort_by(|a, b| a.content_hash().cmp(&b.content_hash()));

        let duplicates_group = DuplicatesGroup {
            group: group
                .iter()
                .map(|x| DuplicatesGroupItem {
                    path_hash: x.path_hash(),
                    content_hash: x.content_hash(),
                })
                .collect(),
        };
        //println!("{:?}", duplicates_group);
        duplicates_group.write_to_stream(&mut output_writer)?;
        group.clear();

        Ok(())
    };

    let mut group = Vec::new();
    while merger.has_data_left() {
        let next = merger.next()?;

        if group.is_empty() {
            group.push(next);
            continue;
        }

        if group[0].bucket_index() == next.bucket_index()
            && group[0].bucket_hash() == next.bucket_hash()
        {
            group.push(next);
            continue;
        }

        if group.len() > 1 {
            flush(&mut group)?;
        }

        group.clear();
        group.push(next);
    }

    if group.len() > 1 {
        flush(&mut group)?;
    }

    output_writer.flush()?;

    info!("Stopped finding duplicates in folder {}", input_folder);

    Ok(())
}

#[derive(Readable, Writable)]
struct Filter {
    content_hash: u64,
}

pub fn build_filters(context: &Context) -> Result<()> {
    info!("Started building filters");

    let mut writers = HashMap::new();

    let mut reader = stream::Decoder::new(File::open(context.duplicats_groups_path())?)?;
    loop {
        let group = DuplicatesGroup::read_from_stream_unbuffered(&mut reader);
        if group.as_ref().is_err_and(|e| e.is_eof()) {
            break;
        }

        let rows = &group?.group;
        for i in 1..rows.len() {
            let row = &rows[i];
            if !writers.contains_key(&row.path_hash) {
                writers.insert(
                    row.path_hash,
                    stream::Encoder::new(
                        BufWriter::new(File::create(context.filter_file_path(row.path_hash))?),
                        1,
                    )?,
                );
            }

            let stream = writers.get_mut(&row.path_hash).unwrap();
            let filter = Filter {
                content_hash: row.content_hash,
            };
            filter.write_to_stream(stream)?;
        }
    }

    for (_, val) in &mut writers {
        val.flush()?;
    }

    info!("Stopped building filters");

    Ok(())
}

fn apply_filter_to_files(
    context: &Context,
    files: &[String],
    column: &String,
    output_folder: &String,
    progress_bar: &ProgressBar,
    total_rows: &mut Arc<AtomicU64>,
    filtered_rows: &mut Arc<AtomicU64>,
) -> Result<()> {
    for file in files {
        let filter_file = context.filter_file_path(Context::hash_path(&file));
        if !Path::new(&filter_file).exists() {
            debug!("There is no filter file for {}, nothing to filter", file);
            progress_bar.inc(1);
            continue;
        }
        debug!("Starting filter {} with filter file {}", file, filter_file);

        let mut filters_set = HashSet::new();
        let mut filter_reader = stream::Decoder::new(File::open(filter_file)?)?;
        loop {
            let filter = Filter::read_from_stream_unbuffered(&mut filter_reader);
            if filter.as_ref().is_err_and(|e| e.is_eof()) {
                break;
            }
            filters_set.insert(filter?.content_hash);
        }

        let output_file_path = format!("{}/{:x}.parquet.zst", output_folder, md5::compute(&file));
        debug!("Writing {}", output_file_path);
        let mut writer = ParquetWriter::new(&output_file_path, column)?;

        let mut reader = ParquetReader::try_new(&file, &column)?;

        let mut num_total = 0u64;
        let mut num_filtered = 0u64;

        while reader.has_data_left()? {
            let text = reader.next()?;
            num_total += 1;
            if filters_set.contains(&hash::<u64>(&text)) {
                num_filtered += 1;
                continue;
            }
            writer.write(text)?;
        }

        writer.close()?;
        debug!(
            "Stopped filter {}, num_total: {}, num_filtered: {}",
            file, num_total, num_filtered
        );

        total_rows.fetch_add(num_total, Ordering::Relaxed);
        filtered_rows.fetch_add(num_filtered, Ordering::Relaxed);

        progress_bar.inc(1);
    }

    Ok(())
}

pub fn apply_filters(
    context: &Context,
    column_name: &String,
    output_folder: &String,
    n_workers: usize,
) -> Result<()> {
    info!("Started applying filters");

    let mut input_files = context.input_files().clone();
    input_files.shuffle(&mut thread_rng());

    let total_rows = Arc::new(AtomicU64::new(0));
    let filtered_rows = Arc::new(AtomicU64::new(0));

    let progress_bar = Arc::new(ProgressBar::new(input_files.len() as u64));
    let pool = ThreadPool::new(n_workers);

    let num_files_per_worker = cmp::max(input_files.len() / n_workers, 1);
    for start in (0..input_files.len()).step_by(num_files_per_worker) {
        let worker_files = Vec::from(
            &input_files[start..cmp::min(input_files.len(), start + num_files_per_worker)],
        );
        let context = context.clone();
        let column_name = column_name.clone();
        let output_folder = output_folder.clone();
        let progress_bar = progress_bar.clone();
        let mut total_rows = total_rows.clone();
        let mut filtered_rows = filtered_rows.clone();
        let worker = move || {
            let result = apply_filter_to_files(
                &context,
                &worker_files,
                &column_name,
                &output_folder,
                &*progress_bar,
                &mut total_rows,
                &mut filtered_rows,
            );
            if result.is_err() {
                error!("{}", result.err().unwrap());
                panic!();
            }
        };
        pool.execute(worker);
    }
    pool.join();

    info!(
        "Total rows processed: {}, total filtered: {}",
        total_rows.load(Ordering::Relaxed),
        filtered_rows.load(Ordering::Relaxed),
    );

    info!("Stopped applying filters");

    Ok(())
}

pub fn show_diff(context: &Context, column: &String, limit: usize) -> Result<()> {
    let mut reader = stream::Decoder::new(File::open(context.duplicats_groups_path())?)?;

    let mut groups = Vec::new();
    for _ in 0..limit {
        let group = DuplicatesGroup::read_from_stream_unbuffered(&mut reader);
        if group.as_ref().is_err_and(|e| e.is_eof()) {
            break;
        }
        groups.push(group?);
    }

    let mut content_hashes = HashSet::new();
    let mut files = HashSet::new();

    for group in &groups {
        for item in &group.group {
            content_hashes.insert(item.content_hash);
            files.extend(context.hash_to_input_files(item.path_hash));
        }
    }

    let mut content = HashMap::new();
    for file in files {
        let mut reader = ParquetReader::try_new(&file, &column)?;
        while reader.has_data_left()? {
            let text = reader.next()?;
            content.insert(hash::<u64>(&text), text);
        }
    }

    for group in &groups {
        let (item0, item1) = (&group.group[0], &group.group[1]);
        diff::print_diff(
            content.get(&item0.content_hash).unwrap(),
            content.get(&item1.content_hash).unwrap(),
        );
    }

    Ok(())
}
