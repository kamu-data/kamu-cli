use std::path::Path;

use datafusion::parquet::{
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
    record::reader::RowIter,
};

pub struct ParquetReaderHelper {
    pub reader: SerializedFileReader<std::fs::File>,
}

impl ParquetReaderHelper {
    pub fn new(reader: SerializedFileReader<std::fs::File>) -> Self {
        Self { reader }
    }

    pub fn open(path: &Path) -> Self {
        Self::new(SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap())
    }

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

    pub fn get_row_iter(&self) -> RowIter {
        self.reader.get_row_iter(None).unwrap()
    }
}
