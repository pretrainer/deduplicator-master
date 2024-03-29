use std::{
    fs::{create_dir_all, remove_dir_all},
    path::Path,
};

use anyhow::Result;
use clap::Parser;
use context::Context;
use env_logger::Env;
use log::info;

mod context;
mod diff;
mod lsh;
mod minhash;
mod operations;
mod parquet_io;

#[derive(Parser)]
#[command(name = "deduplicate")]
enum Cli {
    Deduplicate(DeduplicateArgs),
    Diff(DiffArgs),
}

#[derive(clap::Args)]
#[command(version, about, long_about = None)]
struct DeduplicateArgs {
    #[arg(long, value_name = "INPUT")]
    input: String,

    #[arg(long, value_name = "INPUT_PATTERN", default_value = "*.parquet.zst")]
    input_pattern: String,

    #[arg(long, value_name = "TMP")]
    tmp: String,

    #[arg(long, value_name = "OUT")]
    out: String,

    #[arg(long, value_name = "N_WORKERS", default_value = "1")]
    n_workers: usize,

    #[arg(long, value_name = "COLUMN", default_value = "content")]
    column: String,

    #[arg(long, value_name = "CLEAR", default_value = "false")]
    clear: bool,

    //#[arg(long, value_name = "CLEAR", default_value = "false")]
    //clear: bool,
    #[arg(long, default_value = "1073741824")]
    lsh_buckets_size_limit: u64,
}

fn clear(cli: &DeduplicateArgs) -> Result<()> {
    assert!(cli.clear);
    info!(
        "clear == true, so remove dirs output = {}, tmp = {}",
        cli.out, cli.tmp
    );
    remove_dir_all(&cli.out)?;
    remove_dir_all(&cli.tmp)?;

    Ok(())
}

fn deduplicate_main(cli: DeduplicateArgs) -> Result<()> {
    if cli.clear {
        clear(&cli)?;
    }

    let context = Context::new(cli.input, cli.input_pattern, cli.tmp)?;

    if !Path::new(&context.duplicats_groups_path()).exists() {
        operations::process_parquet_files_from_folder_to_lsh_buckets_files(
            &context,
            &cli.column,
            cli.lsh_buckets_size_limit,
            cli.n_workers,
        )?;

        operations::find_duplicates_in_lsh_buckets_files(
            &context.raw_lsh_buckets_folder_path(),
            &context.duplicats_groups_path(),
        )?;
    } else {
        info!("Found duplicates.groups file, so initial processing lsh index files is skipped");
    }

    operations::build_filters(&context)?;

    create_dir_all(&cli.out)?;
    operations::apply_filters(&context, &cli.column, &cli.out, cli.n_workers)?;

    Ok(())
}

#[derive(clap::Args)]
#[command(version, about, long_about = None)]
struct DiffArgs {
    #[arg(long, value_name = "INPUT")]
    input: String,

    #[arg(long, value_name = "INPUT_PATTERN", default_value = "*.parquet.zst")]
    input_pattern: String,

    #[arg(long, value_name = "TMP")]
    tmp: String,

    #[arg(long, value_name = "COLUMN", default_value = "content")]
    column: String,

    #[arg(long, value_name = "LIMIT", default_value = "100")]
    limit: usize,
}

fn diff_main(cli: DiffArgs) -> Result<()> {
    let context = Context::new(cli.input, cli.input_pattern, cli.tmp)?;
    operations::show_diff(&context, &cli.column, cli.limit)
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();
    match cli {
        Cli::Deduplicate(args) => deduplicate_main(args),
        Cli::Diff(args) => diff_main(args),
    }
}
