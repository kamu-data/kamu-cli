// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{self, Display};
use std::io::{ErrorKind, Write};

use arrow::error::ArrowError;
pub use datafusion::arrow::csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
pub use datafusion::arrow::json::{
    ArrayWriter as JsonArrayWriter,
    LineDelimitedWriter as JsonLineDelimitedWriter,
};
use datafusion::arrow::record_batch::RecordBatch;
use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

pub trait RecordsWriter {
    fn write_batch(&mut self, records: &RecordBatch) -> Result<(), WriterError>;

    fn write_batches(&mut self, record_batches: &[RecordBatch]) -> Result<(), WriterError> {
        for records in record_batches {
            self.write_batch(records)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        Ok(())
    }

    fn handle_writer_result(
        &self,
        writer_result: Result<(), ArrowError>,
    ) -> Result<(), WriterError> {
        if let Err(err) = writer_result {
            match err {
                ArrowError::IoError(_, io_err) => match io_err.kind() {
                    ErrorKind::BrokenPipe => (),
                    _ => return Err(WriterError::IoError(io_err)),
                },
                err => return Err(WriterError::ArrowError(err)),
            };
        }
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// CSV
/////////////////////////////////////////////////////////////////////////////////////////

impl<W: Write> RecordsWriter for CsvWriter<W> {
    fn write_batch(&mut self, records: &RecordBatch) -> Result<(), WriterError> {
        let writer_result = CsvWriter::write(self, records);
        self.handle_writer_result(writer_result)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// JSON
/////////////////////////////////////////////////////////////////////////////////////////

impl<W: Write> RecordsWriter for JsonArrayWriter<W> {
    fn write_batch(&mut self, records: &RecordBatch) -> Result<(), WriterError> {
        let writer_result = JsonArrayWriter::write(self, records);
        self.handle_writer_result(writer_result)
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        JsonArrayWriter::finish(self).unwrap();
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// JSON Line-Delimited
/////////////////////////////////////////////////////////////////////////////////////////

impl<W: Write> RecordsWriter for JsonLineDelimitedWriter<W> {
    fn write_batch(&mut self, records: &RecordBatch) -> Result<(), WriterError> {
        let writer_result = JsonLineDelimitedWriter::write(self, records);
        self.handle_writer_result(writer_result)
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        JsonLineDelimitedWriter::finish(self).unwrap();
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum WriterError {
    IoError(std::io::Error),
    ArrowError(ArrowError),
}

impl Display for WriterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError(io_err) => write!(f, "Cannot write output, io error occurred : {io_err}"),
            Self::ArrowError(arrow_err) => {
                write!(f, "Cannot write output, arrow error occurred : {arrow_err}")
            }
        }
    }
}
