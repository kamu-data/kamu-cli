// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{path::Path, sync::Arc};

use opendatafabric::{Multicodec, Multihash};

const ARROW_BATCH_SIZE: usize = 10_000;

/// Computes a stable hash based on the content (not binary layout) of the Parquet file.
pub fn get_parquet_logical_hash(
    data_path: &Path,
) -> Result<Multihash, datafusion::parquet::errors::ParquetError> {
    use arrow_digest::{RecordDigest, RecordDigestV0};
    use datafusion::parquet::arrow::ArrowReader;
    use datafusion::parquet::arrow::ParquetFileArrowReader;
    use datafusion::parquet::file::reader::SerializedFileReader;

    let file = std::fs::File::open(&data_path)?;
    let parquet_reader = SerializedFileReader::new(file)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));

    let mut hasher = RecordDigestV0::<sha3::Sha3_256>::new(&arrow_reader.get_schema()?);

    for res_batch in arrow_reader.get_record_reader(ARROW_BATCH_SIZE)? {
        let batch = res_batch?;
        hasher.update(&batch);
    }

    let digest = hasher.finalize();
    Ok(Multihash::new(Multicodec::Arrow0_Sha3_256, &digest))
}

pub fn get_parquet_physical_hash(data_path: &Path) -> Result<Multihash, std::io::Error> {
    use digest::Digest;
    use std::io::Read;

    let mut file = std::fs::File::open(data_path)?;
    let mut buffer = [0; 2048];
    let mut hasher = sha3::Sha3_256::new();

    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }

    Ok(Multihash::new(Multicodec::Sha3_256, &hasher.finalize()))
}
