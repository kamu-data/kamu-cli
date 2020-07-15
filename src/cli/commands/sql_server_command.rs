use super::Command;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;

pub struct SqlServerCommand {
    address: String,
    port: u16,
}

impl SqlServerCommand {
    pub fn new(address: &str, port: u16) -> SqlServerCommand {
        SqlServerCommand {
            address: address.to_owned(),
            port: port,
        }
    }
}

impl Command for SqlServerCommand {
    fn run(&mut self) {
        let file = File::open(&Path::new("data/decimal/1.snappy.parquet")).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let mut iter = reader.get_row_iter(None).unwrap();
        while let Some(record) = iter.next() {
            println!("{}", record);
        }
    }
}
