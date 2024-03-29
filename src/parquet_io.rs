use anyhow::{anyhow, Result};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

pub struct ParquetReader {
    path: String,
    column: String,
    batch_reader: ParquetRecordBatchReader,
    texts: Vec<String>,
}

impl ParquetReader {
    pub fn try_new(path: &String, column: &String) -> Result<Self> {
        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        Ok(Self {
            path: path.to_string(),
            column: column.clone(),
            batch_reader: builder.build()?,
            texts: Vec::new(),
        })
    }

    fn read_texts(&mut self) -> Result<()> {
        while self.texts.is_empty() {
            let record_batch = self.batch_reader.next();
            if record_batch.is_none() {
                break;
            }
            let record_batch = record_batch.unwrap()?;

            let rows = record_batch
                .column_by_name(&self.column)
                .ok_or(anyhow!(
                    "Cannot find column {} in file {}",
                    self.column,
                    self.path
                ))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(anyhow!("Cannot downcast column to StringArray"))?;

            for row in rows {
                self.texts.push(
                    row.ok_or(anyhow!(
                        "Cannot find text in column {} in file {}",
                        self.column,
                        self.path
                    ))?
                    .to_string(),
                );
            }
            self.texts.reverse();
        }

        Ok(())
    }

    pub fn has_data_left(&mut self) -> Result<bool> {
        if self.texts.is_empty() {
            self.read_texts()?;
        }
        Ok(!self.texts.is_empty())
    }

    pub fn next(&mut self) -> Result<String> {
        if self.texts.is_empty() {
            self.read_texts()?;
        }
        Ok(self.texts.pop().unwrap())
    }
}

pub struct ParquetWriter {
    writer: ArrowWriter<BufWriter<File>>,
    column: String,
    buffer: Vec<String>,
}

impl ParquetWriter {
    pub fn new(path: &String, column: &String) -> Result<Self> {
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(5)?))
            .build();

        let writer = ArrowWriter::try_new(
            BufWriter::new(File::create(path)?),
            Arc::new(Schema::new(vec![Field::new(column, DataType::Utf8, false)])),
            Some(props),
        )?;
        Ok(Self {
            writer,
            column: column.clone(),
            buffer: Vec::new(),
        })
    }

    pub fn write(&mut self, text: String) -> Result<()> {
        const BATCH_SIZE: usize = 1024;

        self.buffer.push(text);
        if self.buffer.len() >= BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let col = Arc::new(StringArray::from_iter_values(self.buffer.iter())) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([(self.column.clone(), col)])?;
        self.writer.write(&to_write)?;
        self.buffer.clear();
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.writer.close()?;
        Ok(())
    }
}
