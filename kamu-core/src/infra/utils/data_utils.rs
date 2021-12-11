use std::{path::Path, sync::Arc};

use arrow_digest::{RecordDigest, RecordDigestV0};
use datafusion::parquet::arrow::ArrowReader;
use datafusion::parquet::arrow::ParquetFileArrowReader;
use datafusion::parquet::file::reader::SerializedFileReader;
use opendatafabric::{MulticodecCode, Multihash};

/// Computes a stable hash based on the content (not binary layout) of the Parquet file.
pub fn get_parquet_logical_hash(
    data_path: &Path,
) -> Result<Multihash, datafusion::parquet::errors::ParquetError> {
    let file = std::fs::File::open(&data_path)?;
    let parquet_reader = SerializedFileReader::new(file)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));

    let mut hasher = RecordDigestV0::<sha3::Sha3_256>::new(&arrow_reader.get_schema()?);

    for res_batch in arrow_reader.get_record_reader(1024)? {
        let batch = res_batch?;
        hasher.update(&batch);
    }

    let digest = hasher.finalize();
    Ok(Multihash::new(MulticodecCode::Sha3_256, &digest))
}

pub fn get_parquet_physical_hash(
    data_path: &Path,
) -> Result<Multihash, datafusion::parquet::errors::ParquetError> {
    unimplemented!()
}
