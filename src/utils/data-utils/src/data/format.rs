// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::{BufWriter, ErrorKind, Write};

use arrow::array::{AsArray, OffsetSizeTrait};
use arrow::datatypes::{ArrowPrimitiveType, DataType, SchemaRef};
use arrow::error::ArrowError;
pub use datafusion::arrow::csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use datafusion::arrow::json::ArrayWriter;
pub use datafusion::arrow::json::LineDelimitedWriter as JsonLineDelimitedWriter;
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
// JSON Array-of-Structures
/////////////////////////////////////////////////////////////////////////////////////////

/// This type exists purely as a workaround to an issue where empty batches
/// produce no output (which is an invalid JSON) instead of an empty array `[]`.
pub struct JsonArrayOfStructsWriter<W: std::io::Write> {
    inner: Option<ArrayWriter<W>>,
    empty: bool,
}

impl<W: std::io::Write> JsonArrayOfStructsWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: Some(ArrayWriter::new(writer)),
            empty: true,
        }
    }
}

impl<W: Write> RecordsWriter for JsonArrayOfStructsWriter<W> {
    fn write_batch(&mut self, records: &RecordBatch) -> Result<(), WriterError> {
        if self.empty && records.num_rows() != 0 {
            self.empty = false;
        }
        let writer_result = self.inner.as_mut().unwrap().write(records);
        self.handle_writer_result(writer_result)
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        let mut inner = self.inner.take().unwrap();
        inner.finish()?;

        // FIXME: Workaround for upstream bug
        if self.empty {
            write!(inner.into_inner(), "[]")?;
        }

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

/////////////////////////////////////////////////////////////////////////////////////////
// JSON Array-of-Arrays
/////////////////////////////////////////////////////////////////////////////////////////

pub struct JsonArrayOfArraysWriter<W> {
    writer: W,
    started: bool,
}

impl<W: std::io::Write> JsonArrayOfArraysWriter<W> {
    pub fn new(mut writer: W) -> Self {
        writer.write_all(b"[").unwrap();
        Self {
            writer,
            started: false,
        }
    }
}

impl<W: std::io::Write> RecordsWriter for JsonArrayOfArraysWriter<W> {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), WriterError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let mut buf: BufWriter<&mut dyn std::io::Write> =
            BufWriter::with_capacity(16 * 1024, &mut self.writer);

        let mut encoders = Vec::new();
        for (f, c) in batch.schema().fields().iter().zip(batch.columns()) {
            encoders.push(encoder_for(f.data_type(), c));
        }

        for idx in 0..batch.num_rows() {
            if self.started {
                write!(&mut buf, ",")?;
            }
            self.started = true;

            write!(&mut buf, "[")?;
            let mut first = true;
            for e in &mut encoders {
                if !first {
                    write!(&mut buf, ",")?;
                }
                e.encode(idx, &mut buf)?;
                first = false;
            }
            write!(&mut buf, "]")?;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        self.writer.write_all(b"]")?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Row-wise encoders
/////////////////////////////////////////////////////////////////////////////////////////

trait Encoder {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError>;
}

struct NullEncoder;
impl Encoder for NullEncoder {
    fn encode(&mut self, _idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "null")?;
        Ok(())
    }
}

struct NullableEncoder<'a, E: Encoder + 'a>(&'a dyn arrow::array::Array, E);
impl<'a, E: Encoder + 'a> Encoder for NullableEncoder<'a, E> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        if self.0.is_null(idx) {
            NullEncoder.encode(idx, buf)
        } else {
            self.1.encode(idx, buf)
        }
    }
}

struct BooleanEncoder<'a>(&'a arrow::array::BooleanArray);
impl<'a> Encoder for BooleanEncoder<'a> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        if self.0.value(idx) {
            write!(buf, "true")?;
        } else {
            write!(buf, "false")?;
        }
        Ok(())
    }
}

struct IntegerEncoder<'a, T: ArrowPrimitiveType>(&'a arrow::array::PrimitiveArray<T>);
impl<'a, T: ArrowPrimitiveType> Encoder for IntegerEncoder<'a, T>
where
    <T as ArrowPrimitiveType>::Native: std::fmt::Display,
{
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "{}", self.0.value(idx))?;
        Ok(())
    }
}

struct Float16Encoder<'a>(&'a arrow::array::Float16Array);
impl<'a> Encoder for Float16Encoder<'a> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        use serde::ser::Serializer;
        let mut serializer = serde_json::Serializer::new(buf);
        serializer
            .serialize_f32(self.0.value(idx).to_f32())
            .unwrap();
        Ok(())
    }
}

struct Float32Encoder<'a>(&'a arrow::array::Float32Array);
impl<'a> Encoder for Float32Encoder<'a> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        use serde::ser::Serializer;
        let mut serializer = serde_json::Serializer::new(buf);
        serializer.serialize_f32(self.0.value(idx)).unwrap();
        Ok(())
    }
}

struct Float64Encoder<'a>(&'a arrow::array::Float64Array);
impl<'a> Encoder for Float64Encoder<'a> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        use serde::ser::Serializer;
        let mut serializer = serde_json::Serializer::new(buf);
        serializer.serialize_f64(self.0.value(idx)).unwrap();
        Ok(())
    }
}

struct Decimal128Encoder<'a>(&'a arrow::array::Decimal128Array);
impl<'a> Encoder for Decimal128Encoder<'a> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        // TODO: PERF: Avoid allocation
        // TODO: Support decimal-as-string setting?
        write!(buf, "{}", self.0.value_as_string(idx))?;
        Ok(())
    }
}

struct Decimal256Encoder<'a>(&'a arrow::array::Decimal256Array);
impl<'a> Encoder for Decimal256Encoder<'a> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        // TODO: PERF: Avoid allocation
        // TODO: Support decimal-as-string setting?
        write!(buf, "{}", self.0.value_as_string(idx))?;
        Ok(())
    }
}

struct StringEncoder<'a, OffsetSize: OffsetSizeTrait>(
    &'a arrow::array::GenericStringArray<OffsetSize>,
);
impl<'a, OffsetSize: OffsetSizeTrait> Encoder for StringEncoder<'a, OffsetSize> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        use serde::ser::Serializer;
        let mut serializer = serde_json::Serializer::new(buf);
        serializer.serialize_str(self.0.value(idx)).unwrap();
        Ok(())
    }
}

/// This encoder uses default arrow representation as determined by
/// [`arrow::util::display::ArrayFormatter`]. When using this encoder you have
/// to be absolutely sure that result does not include symbols that have special
/// meaning within a JSON string.
struct ArrowEncoder<'a>(arrow::util::display::ArrayFormatter<'a>);
impl<'a> Encoder for ArrowEncoder<'a> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "\"{}\"", self.0.value(idx))?;
        Ok(())
    }
}

fn encoder_for<'a>(dt: &DataType, c: &'a dyn arrow::array::Array) -> Box<dyn Encoder + 'a> {
    use arrow::datatypes as dts;
    match dt {
        DataType::Null => Box::new(NullEncoder),
        DataType::Boolean => Box::new(NullableEncoder(c, BooleanEncoder(c.as_boolean()))),
        DataType::Int8 => Box::new(NullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::Int8Type>()),
        )),
        DataType::Int16 => Box::new(NullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::Int16Type>()),
        )),
        DataType::Int32 => Box::new(NullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::Int32Type>()),
        )),
        DataType::Int64 => Box::new(NullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::Int64Type>()),
        )),
        DataType::UInt8 => Box::new(NullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::UInt8Type>()),
        )),
        DataType::UInt16 => Box::new(NullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::UInt16Type>()),
        )),
        DataType::UInt32 => Box::new(NullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::UInt32Type>()),
        )),
        DataType::UInt64 => Box::new(NullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::UInt64Type>()),
        )),
        DataType::Float16 => Box::new(NullableEncoder(c, Float16Encoder(c.as_primitive()))),
        DataType::Float32 => Box::new(NullableEncoder(c, Float32Encoder(c.as_primitive()))),
        DataType::Float64 => Box::new(NullableEncoder(c, Float64Encoder(c.as_primitive()))),
        DataType::Decimal128(_, _) => {
            Box::new(NullableEncoder(c, Decimal128Encoder(c.as_primitive())))
        }
        DataType::Decimal256(_, _) => {
            Box::new(NullableEncoder(c, Decimal256Encoder(c.as_primitive())))
        }
        DataType::Utf8 => Box::new(NullableEncoder(c, StringEncoder(c.as_string::<i32>()))),
        DataType::LargeUtf8 => Box::new(NullableEncoder(c, StringEncoder(c.as_string::<i64>()))),
        DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => {
            let options = arrow::util::display::FormatOptions::new().with_display_error(true);
            let formatter = arrow::util::display::ArrayFormatter::try_new(c, &options).unwrap();
            Box::new(NullableEncoder(c, ArrowEncoder(formatter)))
        }
        DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8View
        | DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::Map(_, _)
        | DataType::RunEndEncoded(_, _) => {
            unimplemented!("Array-of-Arrays encoding is not yet supported for {}", dt)
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// JSON Structure-of-Arrays
/////////////////////////////////////////////////////////////////////////////////////////

/// This is the least efficient format as it cannot write data in a streaming
/// fashion and has to buffer all of it in memory.
pub struct JsonStructOfArraysWriter<W> {
    writer: W,
    schema: Option<SchemaRef>,
    column_buffers: Vec<Vec<u8>>,
    max_size: usize,
}

impl<W: std::io::Write> JsonStructOfArraysWriter<W> {
    pub fn new(mut writer: W, max_size: usize) -> Self {
        writer.write_all(b"{").unwrap();
        Self {
            writer,
            schema: None,
            column_buffers: Vec::new(),
            max_size,
        }
    }
}

impl<W: std::io::Write> RecordsWriter for JsonStructOfArraysWriter<W> {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), WriterError> {
        if self.schema.is_none() {
            // Init schema and column buffers
            let schema = batch.schema();
            self.column_buffers.resize(schema.fields.len(), Vec::new());
            self.schema = Some(schema);
            for buf in &mut self.column_buffers {
                buf.push(b'[');
            }
        }

        let mut size: usize = self.column_buffers.iter().map(Vec::len).sum();

        for ((f, c), buf) in batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .zip(&mut self.column_buffers)
        {
            let buf_start_len = buf.len();

            let mut encoder = encoder_for(f.data_type(), c);
            for idx in 0..c.len() {
                if buf.len() > 1 {
                    buf.push(b',');
                }
                encoder.encode(idx, buf)?;
            }

            size = size + buf.len() - buf_start_len;

            if size >= self.max_size {
                return Err(DataTooLarge.into());
            }
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        use serde::ser::Serializer;

        let mut first = true;
        if let Some(schema) = &self.schema {
            for (f, buf) in schema.fields().iter().zip(&mut self.column_buffers) {
                buf.push(b']');

                if !first {
                    self.writer.write_all(b",")?;
                }
                first = false;

                let mut serializer = serde_json::Serializer::new(&mut self.writer);
                serializer.serialize_str(f.name()).unwrap();

                self.writer.write_all(b":")?;
                self.writer.write_all(buf)?;
            }
        }
        self.writer.write_all(b"}")?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Tests
/////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::*;
    use arrow_json::ArrayWriter;
    use serde_json::json;

    use super::*;

    /// This test reproduces the issue that when writing empty batch arrow
    /// writer does not produce any output while it should produce empty array.
    /// If this test fails - it's a good thing, meaning we can remove workaround
    /// on our side.
    #[test]
    fn test_arrow_writer_empty_batch_issue() {
        let batch = get_empty_batch();

        let mut buf = Vec::new();
        {
            let mut writer = ArrayWriter::new(&mut buf);
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        assert_eq!(std::str::from_utf8(&buf).unwrap(), "");
    }

    #[test]
    fn test_arrow_writer_empty_batch_fix() {
        let batch = get_empty_batch();
        assert_aos_output(batch, json!([]));
    }

    #[test]
    fn test_json_aoa_empty() {
        let batch = get_empty_batch();
        assert_aoa_output(batch, json!([]));
    }

    #[test]
    fn test_json_aoa_simple() {
        let batch = get_sample_batch();
        assert_aoa_output(
            batch,
            json!([[1, "a"], [2, "b"], [3, "c"], [null, "d"], [5, null]]),
        );
    }

    #[test_log::test]
    fn test_json_aoa_temporal() {
        let schema = Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            true,
        )]);

        let a = TimestampMillisecondArray::from(vec![
            // 2020-01-01T12:00:00Z
            Some(1_577_880_000_000),
            // 2020-01-01T12:01:00Z
            Some(1_577_880_060_000),
            None,
        ])
        .with_timezone("UTC");

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        assert_aoa_output(
            batch,
            serde_json::json!([["2020-01-01T12:00:00Z"], ["2020-01-01T12:01:00Z"], [null]]),
        );
    }

    #[test]
    fn test_json_soa_empty() {
        let batch = get_empty_batch();
        assert_soa_output(
            batch,
            json!({
                "c1": [],
                "c2": [],
            }),
        );
    }

    #[test]
    fn test_json_soa_simple() {
        let batch = get_sample_batch();
        assert_soa_output(
            batch,
            json!({
                "c1": [1, 2, 3, null, 5],
                "c2": ["a", "b", "c", "d", null],
            }),
        );
    }

    #[test]
    fn test_json_soa_max_size() {
        let batch = get_sample_batch();

        let mut buf = Vec::new();
        let mut writer = JsonStructOfArraysWriter::new(&mut buf, 10);
        assert_matches!(
            writer.write_batch(&batch),
            Err(WriterError::DataTooLarge(_))
        );
    }

    fn get_sample_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
        ]);

        let a = Int32Array::from(vec![Some(1), Some(2), Some(3), None, Some(5)]);
        let b = StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("d"), None]);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap()
    }

    fn get_empty_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
        ]);

        let a = Int32Array::new_null(0);
        let b = StringArray::new_null(0);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap()
    }

    #[allow(clippy::needless_pass_by_value)]
    fn assert_aos_output(batch: RecordBatch, expected: serde_json::Value) {
        let mut buf = Vec::new();
        {
            let mut writer = JsonArrayOfStructsWriter::new(&mut buf);
            writer.write_batch(&batch).unwrap();
            writer.finish().unwrap();
        }
        tracing::debug!("Raw result:\n{}", std::str::from_utf8(&buf).unwrap());

        let actual: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(actual, expected);
    }

    #[allow(clippy::needless_pass_by_value)]
    fn assert_soa_output(batch: RecordBatch, expected: serde_json::Value) {
        let mut buf = Vec::new();
        {
            let mut writer = JsonStructOfArraysWriter::new(&mut buf, usize::MAX);
            writer.write_batch(&batch).unwrap();
            writer.finish().unwrap();
        }
        tracing::debug!("Raw result:\n{}", std::str::from_utf8(&buf).unwrap());

        let actual: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(actual, expected);
    }

    #[allow(clippy::needless_pass_by_value)]
    fn assert_aoa_output(batch: RecordBatch, expected: serde_json::Value) {
        let mut buf = Vec::new();
        {
            let mut writer = JsonArrayOfArraysWriter::new(&mut buf);
            writer.write_batch(&batch).unwrap();
            writer.finish().unwrap();
        }
        tracing::debug!("Raw result:\n{}", std::str::from_utf8(&buf).unwrap());

        let actual: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(actual, expected);
    }
}
