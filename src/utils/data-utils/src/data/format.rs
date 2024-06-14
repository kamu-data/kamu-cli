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
use datafusion::arrow::record_batch::RecordBatch;
use serde::ser::Serializer;
use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////
// RecordsWriter
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
// CSV
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CsvWriterOptions {
    /// Whether to write column names as file headers. Defaults to `true`
    pub header: bool,
    /// Optional column delimiter. Defaults to `b','`
    pub delimiter: u8,
    /// Optional quote character. Defaults to `b'"'`
    pub quote: u8,
}

impl Default for CsvWriterOptions {
    fn default() -> Self {
        Self {
            header: true,
            delimiter: b',',
            quote: b'"',
        }
    }
}

pub struct CsvWriter<W> {
    writer: W,
    options: CsvWriterOptions,
    rows_written: usize,
}

impl<W> CsvWriter<W> {
    pub fn new(writer: W, options: CsvWriterOptions) -> Self {
        Self {
            writer,
            options,
            rows_written: 0,
        }
    }
}

impl<W: Write> RecordsWriter for CsvWriter<W> {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), WriterError> {
        if self.rows_written == 0 && self.options.header {
            for (i, field) in batch.schema_ref().fields().iter().enumerate() {
                if i != 0 {
                    self.writer.write_all(&[self.options.delimiter])?;
                }
                // TODO: quote / escape
                CsvEscapeStringEncoder::write_escaped(
                    &mut self.writer,
                    field.name(),
                    &self.options,
                )?;
            }
            self.rows_written += 1;
        }

        let mut encoders = Vec::new();
        for (f, c) in batch.schema_ref().fields().iter().zip(batch.columns()) {
            encoders.push(encoder_for_csv(f.data_type(), c, &self.options));
        }

        for idx in 0..batch.num_rows() {
            if self.rows_written != 0 {
                self.writer.write_all(b"\n")?;
            }

            for (i, e) in encoders.iter_mut().enumerate() {
                if i != 0 {
                    self.writer.write_all(&[self.options.delimiter])?;
                }
                e.encode(idx, &mut self.writer)?;
            }

            self.rows_written += 1;
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// JSON Array-of-Structures
/////////////////////////////////////////////////////////////////////////////////////////

pub struct JsonArrayOfStructsWriter<W> {
    writer: W,
    empty: bool,
}

impl<W: std::io::Write> JsonArrayOfStructsWriter<W> {
    pub fn new(mut writer: W) -> Self {
        writer.write_all(b"[").unwrap();
        Self {
            writer,
            empty: true,
        }
    }
}

impl<W: Write> RecordsWriter for JsonArrayOfStructsWriter<W> {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), WriterError> {
        let mut encoders = Vec::new();
        for (f, c) in batch.schema_ref().fields().iter().zip(batch.columns()) {
            encoders.push(encoder_for_json(f.data_type(), c));
        }

        for idx in 0..batch.num_rows() {
            if !self.empty {
                self.writer.write_all(b",")?;
            }

            self.writer.write_all(b"{")?;

            for (i, (e, f)) in encoders
                .iter_mut()
                .zip(batch.schema_ref().fields().iter())
                .enumerate()
            {
                if i != 0 {
                    self.writer.write_all(b",")?;
                }

                {
                    let mut serializer = serde_json::Serializer::new(&mut self.writer);
                    serializer.serialize_str(f.name()).unwrap();
                }
                self.writer.write_all(b":")?;
                e.encode(idx, &mut self.writer)?;
            }

            self.writer.write_all(b"}")?;
            self.empty = false;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        self.writer.write_all(b"]")?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// JSON Line-Delimited
/////////////////////////////////////////////////////////////////////////////////////////

pub struct JsonLineDelimitedWriter<W> {
    writer: W,
    empty: bool,
}

impl<W: std::io::Write> JsonLineDelimitedWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            empty: true,
        }
    }
}

impl<W: Write> RecordsWriter for JsonLineDelimitedWriter<W> {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), WriterError> {
        let mut encoders = Vec::new();
        for (f, c) in batch.schema_ref().fields().iter().zip(batch.columns()) {
            encoders.push(encoder_for_json(f.data_type(), c));
        }

        for idx in 0..batch.num_rows() {
            if !self.empty {
                self.writer.write_all(b"\n")?;
            }

            self.writer.write_all(b"{")?;

            for (i, (e, f)) in encoders
                .iter_mut()
                .zip(batch.schema_ref().fields().iter())
                .enumerate()
            {
                if i != 0 {
                    self.writer.write_all(b",")?;
                }

                {
                    let mut serializer = serde_json::Serializer::new(&mut self.writer);
                    serializer.serialize_str(f.name()).unwrap();
                }
                self.writer.write_all(b":")?;
                e.encode(idx, &mut self.writer)?;
            }

            self.writer.write_all(b"}")?;
            self.empty = false;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        Ok(())
    }
}

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

    pub fn into_inner(self) -> W {
        self.writer
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
            encoders.push(encoder_for_json(f.data_type(), c));
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

            let mut encoder = encoder_for_json(f.data_type(), c);
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
// Row-wise encoders
/////////////////////////////////////////////////////////////////////////////////////////

trait Encoder {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

fn encoder_for_json<'a>(dt: &DataType, c: &'a dyn arrow::array::Array) -> Box<dyn Encoder + 'a> {
    use arrow::datatypes as dts;
    match dt {
        DataType::Null => Box::new(JsonNullEncoder),
        DataType::Boolean => Box::new(JsonNullableEncoder(c, BooleanEncoder(c.as_boolean()))),
        DataType::Int8 => Box::new(JsonNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::Int8Type>()),
        )),
        DataType::Int16 => Box::new(JsonNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::Int16Type>()),
        )),
        DataType::Int32 => Box::new(JsonNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::Int32Type>()),
        )),
        DataType::Int64 => Box::new(JsonNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::Int64Type>()),
        )),
        DataType::UInt8 => Box::new(JsonNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::UInt8Type>()),
        )),
        DataType::UInt16 => Box::new(JsonNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::UInt16Type>()),
        )),
        DataType::UInt32 => Box::new(JsonNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::UInt32Type>()),
        )),
        DataType::UInt64 => Box::new(JsonNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::UInt64Type>()),
        )),
        DataType::Float16 => Box::new(JsonNullableEncoder(c, Float16Encoder(c.as_primitive()))),
        DataType::Float32 => Box::new(JsonNullableEncoder(c, Float32Encoder(c.as_primitive()))),
        DataType::Float64 => Box::new(JsonNullableEncoder(c, Float64Encoder(c.as_primitive()))),
        DataType::Decimal128(_, _) => Box::new(JsonNullableEncoder(
            c,
            JsonSafeStringEncoder(Decimal128Encoder(c.as_primitive())),
        )),
        DataType::Decimal256(_, _) => Box::new(JsonNullableEncoder(
            c,
            JsonSafeStringEncoder(Decimal256Encoder(c.as_primitive())),
        )),
        DataType::Utf8 => Box::new(JsonNullableEncoder(
            c,
            JsonEscapeStringEncoder::new(StringEncoder(c.as_string::<i32>())),
        )),
        DataType::LargeUtf8 => Box::new(JsonNullableEncoder(
            c,
            JsonEscapeStringEncoder::new(StringEncoder(c.as_string::<i64>())),
        )),
        DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => {
            let options = arrow::util::display::FormatOptions::new().with_display_error(true);
            let formatter = arrow::util::display::ArrayFormatter::try_new(c, &options).unwrap();
            Box::new(JsonNullableEncoder(
                c,
                JsonSafeStringEncoder(ArrowEncoder(formatter)),
            ))
        }
        DataType::Binary => Box::new(JsonNullableEncoder(
            c,
            JsonSafeStringEncoder(BinaryHexEncoder(c.as_binary::<i32>())),
        )),
        DataType::LargeBinary => Box::new(JsonNullableEncoder(
            c,
            JsonSafeStringEncoder(BinaryHexEncoder(c.as_binary::<i64>())),
        )),
        DataType::FixedSizeBinary(_) => Box::new(JsonNullableEncoder(
            c,
            JsonSafeStringEncoder(BinaryFixedHexEncoder(c.as_fixed_size_binary())),
        )),
        DataType::BinaryView
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

fn encoder_for_csv<'a>(
    dt: &DataType,
    c: &'a dyn arrow::array::Array,
    opts: &'a CsvWriterOptions,
) -> Box<dyn Encoder + 'a> {
    use arrow::datatypes as dts;
    match dt {
        DataType::Null => Box::new(CsvNullEncoder),
        DataType::Boolean => Box::new(CsvNullableEncoder(c, BooleanEncoder(c.as_boolean()))),
        DataType::Int8 => Box::new(CsvNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::Int8Type>()),
        )),
        DataType::Int16 => Box::new(CsvNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::Int16Type>()),
        )),
        DataType::Int32 => Box::new(CsvNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::Int32Type>()),
        )),
        DataType::Int64 => Box::new(CsvNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::Int64Type>()),
        )),
        DataType::UInt8 => Box::new(CsvNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::UInt8Type>()),
        )),
        DataType::UInt16 => Box::new(CsvNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::UInt16Type>()),
        )),
        DataType::UInt32 => Box::new(CsvNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::UInt32Type>()),
        )),
        DataType::UInt64 => Box::new(CsvNullableEncoder(
            c,
            IntegerEncoder(c.as_primitive::<dts::UInt64Type>()),
        )),
        DataType::Float16 => Box::new(CsvNullableEncoder(c, Float16Encoder(c.as_primitive()))),
        DataType::Float32 => Box::new(CsvNullableEncoder(c, Float32Encoder(c.as_primitive()))),
        DataType::Float64 => Box::new(CsvNullableEncoder(c, Float64Encoder(c.as_primitive()))),
        DataType::Decimal128(_, _) => {
            Box::new(CsvNullableEncoder(c, Decimal128Encoder(c.as_primitive())))
        }
        DataType::Decimal256(_, _) => {
            Box::new(CsvNullableEncoder(c, Decimal256Encoder(c.as_primitive())))
        }
        DataType::Utf8 => Box::new(CsvNullableEncoder(
            c,
            CsvEscapeStringEncoder::new(StringEncoder(c.as_string::<i32>()), opts),
        )),
        DataType::LargeUtf8 => Box::new(CsvNullableEncoder(
            c,
            CsvEscapeStringEncoder::new(StringEncoder(c.as_string::<i64>()), opts),
        )),
        DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => {
            let options = arrow::util::display::FormatOptions::new().with_display_error(true);
            let formatter = arrow::util::display::ArrayFormatter::try_new(c, &options).unwrap();
            Box::new(CsvNullableEncoder(c, ArrowEncoder(formatter)))
        }
        DataType::Binary => Box::new(CsvNullableEncoder(
            c,
            BinaryHexEncoder(c.as_binary::<i32>()),
        )),
        DataType::LargeBinary => Box::new(CsvNullableEncoder(
            c,
            BinaryHexEncoder(c.as_binary::<i64>()),
        )),
        DataType::FixedSizeBinary(_) => Box::new(CsvNullableEncoder(
            c,
            BinaryFixedHexEncoder(c.as_fixed_size_binary()),
        )),
        DataType::BinaryView
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

struct JsonEscapeStringEncoder<E: Encoder>(E, Vec<u8>);
impl<E: Encoder> JsonEscapeStringEncoder<E> {
    fn new(e: E) -> Self {
        Self(e, Vec::new())
    }
}
impl<E: Encoder> Encoder for JsonEscapeStringEncoder<E> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        self.0.encode(idx, &mut self.1)?;
        let mut serializer = serde_json::Serializer::new(buf);
        serializer
            .serialize_str(std::str::from_utf8(&self.1).unwrap())
            .unwrap();

        self.1.clear();
        Ok(())
    }
}

struct JsonSafeStringEncoder<E: Encoder>(E);
impl<E: Encoder> Encoder for JsonSafeStringEncoder<E> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "\"")?;
        self.0.encode(idx, buf)?;
        write!(buf, "\"")?;
        Ok(())
    }
}

struct CsvEscapeStringEncoder<'a, E: Encoder>(E, &'a CsvWriterOptions, Vec<u8>);
impl<'a, E: Encoder> CsvEscapeStringEncoder<'a, E> {
    fn new(e: E, opts: &'a CsvWriterOptions) -> Self {
        Self(e, opts, Vec::new())
    }
}

impl CsvEscapeStringEncoder<'static, CsvNullEncoder> {
    fn write_escaped(
        buf: &mut dyn std::io::Write,
        s: &str,
        opts: &CsvWriterOptions,
    ) -> Result<(), WriterError> {
        if !s.contains([
            char::from(opts.delimiter),
            char::from(opts.quote),
            '\n',
            '\r',
        ]) {
            buf.write_all(s.as_bytes())?;
        } else {
            buf.write_all(&[opts.quote])?;
            let double_quote = [opts.quote, opts.quote];
            let s = s.replace(
                char::from(opts.quote),
                std::str::from_utf8(&double_quote).unwrap(),
            );
            buf.write_all(s.as_bytes())?;
            buf.write_all(&[opts.quote])?;
        }
        Ok(())
    }
}

impl<'a, E: Encoder> Encoder for CsvEscapeStringEncoder<'a, E> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        self.0.encode(idx, &mut self.2)?;
        CsvEscapeStringEncoder::write_escaped(buf, std::str::from_utf8(&self.2).unwrap(), self.1)?;
        self.2.clear();
        Ok(())
    }
}

struct JsonNullEncoder;
impl Encoder for JsonNullEncoder {
    fn encode(&mut self, _idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "null")?;
        Ok(())
    }
}

struct JsonNullableEncoder<'a, E: Encoder + 'a>(&'a dyn arrow::array::Array, E);
impl<'a, E: Encoder + 'a> Encoder for JsonNullableEncoder<'a, E> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        if self.0.is_null(idx) {
            JsonNullEncoder.encode(idx, buf)
        } else {
            self.1.encode(idx, buf)
        }
    }
}

struct CsvNullEncoder;
impl Encoder for CsvNullEncoder {
    fn encode(&mut self, _idx: usize, _buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        Ok(())
    }
}

struct CsvNullableEncoder<'a, E: Encoder + 'a>(&'a dyn arrow::array::Array, E);
impl<'a, E: Encoder + 'a> Encoder for CsvNullableEncoder<'a, E> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        if self.0.is_null(idx) {
            CsvNullEncoder.encode(idx, buf)
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
        let mut serializer = serde_json::Serializer::new(buf);
        serializer.serialize_f32(self.0.value(idx)).unwrap();
        Ok(())
    }
}

struct Float64Encoder<'a>(&'a arrow::array::Float64Array);
impl<'a> Encoder for Float64Encoder<'a> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        let mut serializer = serde_json::Serializer::new(buf);
        serializer.serialize_f64(self.0.value(idx)).unwrap();
        Ok(())
    }
}

struct Decimal128Encoder<'a>(&'a arrow::array::Decimal128Array);
impl<'a> Encoder for Decimal128Encoder<'a> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        // TODO: PERF: Avoid allocation
        write!(buf, "{}", self.0.value_as_string(idx))?;
        Ok(())
    }
}

struct Decimal256Encoder<'a>(&'a arrow::array::Decimal256Array);
impl<'a> Encoder for Decimal256Encoder<'a> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        // TODO: PERF: Avoid allocation
        write!(buf, "{}", self.0.value_as_string(idx))?;
        Ok(())
    }
}

struct StringEncoder<'a, OffsetSize: OffsetSizeTrait>(
    &'a arrow::array::GenericStringArray<OffsetSize>,
);
impl<'a, OffsetSize: OffsetSizeTrait> Encoder for StringEncoder<'a, OffsetSize> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "{}", self.0.value(idx))?;
        Ok(())
    }
}

struct BinaryHexEncoder<'a, OffsetSize: OffsetSizeTrait>(
    &'a arrow::array::GenericBinaryArray<OffsetSize>,
);
impl<'a, OffsetSize: OffsetSizeTrait> Encoder for BinaryHexEncoder<'a, OffsetSize> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        let hex_str = hex::encode(self.0.value(idx));
        write!(buf, "{hex_str}")?;
        Ok(())
    }
}

struct BinaryFixedHexEncoder<'a>(&'a arrow::array::FixedSizeBinaryArray);
impl<'a> Encoder for BinaryFixedHexEncoder<'a> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        let hex_str = hex::encode(self.0.value(idx));
        write!(buf, "{hex_str}")?;
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
        write!(buf, "{}", self.0.value(idx))?;
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
    #[test_log::test]
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

    // AoS
    #[test_log::test]
    fn test_json_aos_empty() {
        let batch = get_empty_batch();
        assert_aos_output(batch, json!([]));
    }

    #[test_log::test]
    fn test_json_aos_simple() {
        let batch = get_sample_batch();
        assert_aos_output(
            batch,
            json!([
                {
                    "bin": "666f6f",
                    "bin_fixed": "0102",
                    "decimal": "5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "nullable": 1,
                    "ts_milli": "2020-01-01T12:00:00Z",
                    "uint64": 100,
                    "utf8": "foo",
                    "utf8_special": "sep,quotes'\",newline\nfin",
                },
                {
                    "bin": "626172",
                    "bin_fixed": "0304",
                    "decimal": "-5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "nullable": null,
                    "ts_milli": "2020-01-01T12:01:00Z",
                    "uint64": 200,
                    "utf8": "bar",
                    "utf8_special": null,
                }
            ]),
        );
    }

    // AoA
    #[test_log::test]
    fn test_json_aoa_empty() {
        let batch = get_empty_batch();
        assert_aoa_output(batch, json!([]));
    }

    #[test_log::test]
    fn test_json_aoa_simple() {
        let batch = get_sample_batch();
        assert_aoa_output(
            batch,
            json!([
                [
                    1,
                    100,
                    "5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "foo",
                    "sep,quotes'\",newline\nfin",
                    "666f6f",
                    "0102",
                    "2020-01-01T12:00:00Z",
                ],
                [
                    null,
                    200,
                    "-5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "bar",
                    null,
                    "626172",
                    "0304",
                    "2020-01-01T12:01:00Z",
                ],
            ]),
        );
    }

    // SoA
    #[test_log::test]
    fn test_json_soa_empty() {
        let batch = get_empty_batch();
        assert_soa_output(
            batch,
            json!({
                "bin": [],
                "bin_fixed": [],
                "decimal": [],
                "nullable": [],
                "ts_milli": [],
                "uint64": [],
                "utf8": [],
                "utf8_special": [],
            }),
        );
    }

    #[test_log::test]
    fn test_json_soa_simple() {
        let batch = get_sample_batch();
        assert_soa_output(
            batch,
            json!({
                "bin": ["666f6f", "626172"],
                "bin_fixed": ["0102", "0304"],
                "decimal": [
                    "5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "-5789604461865809771178549250434395392663499233282028201972879200395656481996",
                ],
                "nullable": [1, null],
                "ts_milli": ["2020-01-01T12:00:00Z", "2020-01-01T12:01:00Z"],
                "uint64": [100, 200],
                "utf8": ["foo", "bar"],
                "utf8_special": ["sep,quotes'\",newline\nfin", null],
            }),
        );
    }

    #[test_log::test]
    fn test_json_soa_max_size() {
        let batch = get_sample_batch();

        let mut buf = Vec::new();
        let mut writer = JsonStructOfArraysWriter::new(&mut buf, 10);
        assert_matches!(
            writer.write_batch(&batch),
            Err(WriterError::DataTooLarge(_))
        );
    }

    // LD
    #[test_log::test]
    fn test_json_ld_empty() {
        let batch = get_empty_batch();
        assert_ld_output(batch, vec![]);
    }

    #[test_log::test]
    fn test_json_ld_simple() {
        let batch = get_sample_batch();
        assert_ld_output(
            batch,
            vec![
                json!({
                    "bin": "666f6f",
                    "bin_fixed": "0102",
                    "decimal": "5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "nullable": 1,
                    "ts_milli": "2020-01-01T12:00:00Z",
                    "uint64": 100,
                    "utf8": "foo",
                    "utf8_special": "sep,quotes'\",newline\nfin",
                }),
                json!({
                    "bin": "626172",
                    "bin_fixed": "0304",
                    "decimal": "-5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "nullable": null,
                    "ts_milli": "2020-01-01T12:01:00Z",
                    "uint64": 200,
                    "utf8": "bar",
                    "utf8_special": null,
                }),
            ],
        );
    }

    // CSV
    #[test_log::test]
    fn test_csv_empty() {
        let batch = get_empty_batch();
        assert_csv_output(
            batch.clone(),
            CsvWriterOptions::default(),
            "nullable,uint64,decimal,utf8,utf8_special,bin,bin_fixed,ts_milli",
        );
        assert_csv_output(
            batch,
            CsvWriterOptions {
                header: false,
                ..Default::default()
            },
            "",
        );
    }

    #[test_log::test]
    fn test_csv_simple() {
        let batch = get_sample_batch();
        assert_csv_output(
            batch,
            CsvWriterOptions::default(),
            indoc::indoc!(
                r#"
                nullable,uint64,decimal,utf8,utf8_special,bin,bin_fixed,ts_milli
                1,100,5789604461865809771178549250434395392663499233282028201972879200395656481996,foo,"sep,quotes'"",newline
                fin",666f6f,0102,2020-01-01T12:00:00Z
                ,200,-5789604461865809771178549250434395392663499233282028201972879200395656481996,bar,,626172,0304,2020-01-01T12:01:00Z
                "#
            )
            .trim(),
        );
    }

    fn get_empty_batch() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::new(vec![
            Field::new("nullable", DataType::UInt64, true),
            Field::new("uint64", DataType::UInt64, false),
            Field::new("decimal", DataType::Decimal256(76, 0), false),
            Field::new("utf8", DataType::Utf8, false),
            Field::new("utf8_special", DataType::Utf8, true),
            Field::new("bin", DataType::Binary, false),
            Field::new("bin_fixed", DataType::FixedSizeBinary(2), false),
            Field::new(
                "ts_milli",
                DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
                false,
            ),
        ])))
    }

    fn get_sample_batch() -> RecordBatch {
        RecordBatch::try_new(
            get_empty_batch().schema(),
            vec![
                Arc::new(UInt64Array::from(vec![Some(1), None])),
                Arc::new(UInt64Array::from(vec![100, 200])),
                Arc::new(
                    Decimal256Array::from(vec![i256::MAX, i256::MIN])
                        .with_precision_and_scale(76, 0)
                        .unwrap(),
                ),
                Arc::new(StringArray::from(vec![
                    "foo".to_string(),
                    "bar".to_string(),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("sep,quotes'\",newline\nfin".to_string()),
                    None,
                ])),
                Arc::new(BinaryArray::from(vec![&b"foo"[..], &b"bar"[..]])),
                Arc::new({
                    let v = vec![vec![1, 2], vec![3, 4]];
                    FixedSizeBinaryArray::try_from_iter(v.into_iter()).unwrap()
                }),
                Arc::new(
                    TimestampMillisecondArray::from(vec![
                        // 2020-01-01T12:00:00Z
                        1_577_880_000_000,
                        // 2020-01-01T12:01:00Z
                        1_577_880_060_000,
                    ])
                    .with_timezone(Arc::from("UTC")),
                ),
            ],
        )
        .unwrap()
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

    #[allow(clippy::needless_pass_by_value)]
    fn assert_ld_output(batch: RecordBatch, expected: Vec<serde_json::Value>) {
        let mut buf = Vec::new();
        {
            let mut writer = JsonLineDelimitedWriter::new(&mut buf);
            writer.write_batch(&batch).unwrap();
            writer.finish().unwrap();
        }
        let actual_str = std::str::from_utf8(&buf).unwrap();
        tracing::debug!("Raw result:\n{actual_str}");
        let actual: Vec<serde_json::Value> = if actual_str.is_empty() {
            Vec::new()
        } else {
            actual_str
                .split('\n')
                .map(|s| serde_json::from_slice(s.as_bytes()).unwrap())
                .collect()
        };

        assert_eq!(actual, expected);
    }

    #[allow(clippy::needless_pass_by_value)]
    fn assert_csv_output(batch: RecordBatch, opts: CsvWriterOptions, expected: &str) {
        let mut buf = Vec::new();
        {
            let mut writer = CsvWriter::new(&mut buf, opts);
            writer.write_batch(&batch).unwrap();
            writer.finish().unwrap();
        }
        let actual = std::str::from_utf8(&buf).unwrap();
        pretty_assertions::assert_eq!(actual, expected);
    }
}
