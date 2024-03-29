To install rust follow this guide:
https://doc.rust-lang.org/cargo/getting-started/installation.html


To build the system:
```
cargo build -r
```

To deduplicate files from folder INPUT_FOLDER, results will be in OUTPUT_FOLDER, working dir with tmp will be in TMP:
```
./target/release/deduplicator deduplicate --input INPUT_FOLDER --input-pattern "*.parquet.zst" --out OUTPUT_FOLDER --tmp TMP --column content --n-workers 12
```

To show diff after deduplication:
```
./target/release/deduplicator diff --input INPUT_FOLDER --tmp TMP --column content --limit 100
```
