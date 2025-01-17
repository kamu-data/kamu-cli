// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::ErrorKind;

use arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RecordsWriter
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

#[derive(Debug, Error)]
pub enum WriterError {
    #[error(transparent)]
    DataTooLarge(#[from] DataTooLarge),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    ArrowError(#[from] ArrowError),
}

#[derive(Debug, Error)]
#[error("Data is too large to fit in the defined serialization buffer")]
pub struct DataTooLarge;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Row-wise encoders
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait Encoder {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError>;
}

impl<T> Encoder for Box<T>
where
    T: Encoder,
    T: ?Sized,
{
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        self.as_mut().encode(idx, buf)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
