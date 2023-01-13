use std::path::Path;

use arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;

pub struct ParquetWriterHelper;

impl ParquetWriterHelper {
    pub fn from_record_batch(path: &Path, record_batch: &RecordBatch) -> Result<(), ParquetError> {
        use datafusion::parquet::arrow::ArrowWriter;

        let mut arrow_writer = ArrowWriter::try_new(
            std::fs::File::create(path).unwrap(),
            record_batch.schema(),
            None,
        )?;

        arrow_writer.write(&record_batch)?;
        arrow_writer.close()?;

        Ok(())
    }
}
