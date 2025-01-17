// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatchReader;
use odf_metadata::{Multicodec, Multihash};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const LOGICAL_HASH_BATCH_SIZE: usize = 10_000;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "debug")]
pub fn get_batches_logical_hash(schema: &Schema, record_batches: &[RecordBatch]) -> Multihash {
    use arrow_digest::{RecordDigest, RecordDigestV0};

    let mut hasher = RecordDigestV0::<sha3::Sha3_256>::new(schema);
    for batch in record_batches {
        hasher.update(batch);
    }

    let digest = hasher.finalize();
    Multihash::new(Multicodec::Arrow0_Sha3_256, &digest).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Computes a stable hash based on a content (as opposed to a binary layout) of
/// a Parquet file.
#[tracing::instrument(level = "info")]
pub fn get_parquet_logical_hash(
    data_path: &Path,
) -> Result<Multihash, datafusion::parquet::errors::ParquetError> {
    use arrow_digest::{RecordDigest, RecordDigestV0};
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(data_path)?;

    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(LOGICAL_HASH_BATCH_SIZE)
        .build()?;

    let mut hasher = RecordDigestV0::<sha3::Sha3_256>::new(&parquet_reader.schema());

    for res_batch in parquet_reader {
        let record_batch = res_batch?;
        hasher.update(&record_batch);
    }

    let digest = hasher.finalize();
    Ok(Multihash::new(Multicodec::Arrow0_Sha3_256, &digest).unwrap())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "info")]
pub fn get_file_physical_hash(file_path: &Path) -> Result<Multihash, std::io::Error> {
    use std::io::Read;

    use digest::Digest;

    let mut file = std::fs::File::open(file_path)?;
    let mut buffer = [0; 2048];
    let mut hasher = sha3::Sha3_256::new();

    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }

    Ok(Multihash::new(Multicodec::Sha3_256, &hasher.finalize()).unwrap())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
