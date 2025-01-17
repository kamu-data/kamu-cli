// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use datafusion::parquet::file::reader::FileReader;
use datafusion::parquet::file::serialized_reader::SerializedFileReader;
use datafusion::parquet::record::reader::RowIter;

pub struct ParquetReaderHelper {
    pub reader: SerializedFileReader<std::fs::File>,
}

impl ParquetReaderHelper {
    pub fn new(reader: SerializedFileReader<std::fs::File>) -> Self {
        Self { reader }
    }

    pub fn open(path: &Path) -> Self {
        Self::new(SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap())
    }

    pub fn get_schema(&self) -> &datafusion::parquet::schema::types::Type {
        self.reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .root_schema()
    }

    #[deprecated(note = "Use [`Self::get_schema`] in combination with \
                         [`odf::utils::testing::assert_parquet_schema_eq`]")]
    pub fn get_column_names(&self) -> Vec<String> {
        self.reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .map(|cd| cd.path().string())
            .collect()
    }

    #[deprecated(note = "use [`Self::get_batches`] in combination with \
                         [`odf::utils::testing::assert_data_batches_eq`]")]
    pub fn get_row_iter(&self) -> RowIter {
        self.reader.get_row_iter(None).unwrap()
    }
}
