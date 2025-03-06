// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;

use arrow::array::{AsArray, OffsetSizeTrait};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use serde::ser::Serializer;

use super::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// JSON Array-of-Structures
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let struct_array: arrow::array::StructArray = batch.clone().into();
        let struct_array_data_type = DataType::Struct(struct_array.fields().clone());
        let mut encoder = encoder_for(&struct_array_data_type, &struct_array, &|f, e| {
            Box::new(JsonStructAoSEncoder(f, e))
        });

        for idx in 0..batch.num_rows() {
            if !self.empty {
                self.writer.write_all(b",")?;
            }

            encoder.encode(idx, &mut self.writer)?;
            self.empty = false;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        self.writer.write_all(b"]")?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// JSON Line-Delimited
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let struct_array: arrow::array::StructArray = batch.clone().into();
        let struct_array_data_type = DataType::Struct(struct_array.fields().clone());
        let mut encoder = encoder_for(&struct_array_data_type, &struct_array, &|f, e| {
            Box::new(JsonStructAoSEncoder(f, e))
        });

        for idx in 0..batch.num_rows() {
            if !self.empty {
                self.writer.write_all(b"\n")?;
            }

            encoder.encode(idx, &mut self.writer)?;
            self.empty = false;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// JSON Array-of-Arrays
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct JsonArrayOfArraysWriter<W> {
    writer: W,
    empty: bool,
}

impl<W: std::io::Write> JsonArrayOfArraysWriter<W> {
    pub fn new(mut writer: W) -> Self {
        writer.write_all(b"[").unwrap();
        Self {
            writer,
            empty: true,
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

        let struct_array: arrow::array::StructArray = batch.clone().into();
        let struct_array_data_type = DataType::Struct(struct_array.fields().clone());
        let mut encoder = encoder_for(&struct_array_data_type, &struct_array, &|f, e| {
            Box::new(JsonStructAoAEncoder(f, e))
        });

        for idx in 0..batch.num_rows() {
            if !self.empty {
                self.writer.write_all(b",")?;
            }
            self.empty = false;

            encoder.encode(idx, &mut self.writer)?;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        self.writer.write_all(b"]")?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// JSON Structure-of-Arrays
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

            let mut encoder = encoder_for(f.data_type(), c, &|_, _| {
                unimplemented!("SoA encoding for struct types is not yet supported")
            });
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn encoder_for<'a>(
    dt: &'a DataType,
    c: &'a dyn arrow::array::Array,
    nested: &dyn Fn(
        &'a arrow::datatypes::Fields,
        Vec<Box<dyn Encoder + 'a>>,
    ) -> Box<dyn Encoder + 'a>,
) -> Box<dyn Encoder + 'a> {
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
        DataType::Utf8View => Box::new(JsonNullableEncoder(
            c,
            JsonEscapeStringEncoder::new(StringViewEncoder(c.as_string_view())),
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
        DataType::BinaryView => Box::new(JsonNullableEncoder(
            c,
            JsonSafeStringEncoder(BinaryViewHexEncoder(c.as_binary_view())),
        )),
        DataType::FixedSizeBinary(_) => Box::new(JsonNullableEncoder(
            c,
            JsonSafeStringEncoder(BinaryFixedHexEncoder(c.as_fixed_size_binary())),
        )),
        DataType::List(field) => {
            let list = c.as_list::<i32>();
            let item_encoder = encoder_for(field.data_type(), list.values(), nested);
            Box::new(JsonNullableEncoder(c, JsonListEncoder(list, item_encoder)))
        }
        DataType::LargeList(field) => {
            let list = c.as_list::<i64>();
            let item_encoder = encoder_for(field.data_type(), list.values(), nested);
            Box::new(JsonNullableEncoder(c, JsonListEncoder(list, item_encoder)))
        }
        DataType::FixedSizeList(field, len) => {
            let list = c.as_fixed_size_list();
            let item_encoder = encoder_for(field.data_type(), list.values(), nested);
            Box::new(JsonNullableEncoder(
                c,
                JsonFixedSizeListEncoder(usize::try_from(*len).unwrap(), item_encoder),
            ))
        }
        DataType::Struct(fields) => {
            let arr = c.as_struct();
            let encoders = fields
                .iter()
                .zip(arr.columns())
                .map(|(f, a)| encoder_for(f.data_type(), a, nested))
                .collect();
            Box::new(JsonNullableEncoder(c, nested(fields, encoders)))
        }
        DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::Map(_, _)
        | DataType::RunEndEncoded(_, _) => {
            unimplemented!("JSON encoding is not yet supported for {}", dt)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::unnecessary_wraps)]
fn write_string_quoted_escaped(s: &str, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
    let mut serializer = serde_json::Serializer::new(buf);
    serializer.serialize_str(s).unwrap();
    Ok(())
}

struct JsonEscapeStringEncoder<E: Encoder>(E, Vec<u8>);
impl<E: Encoder> JsonEscapeStringEncoder<E> {
    fn new(e: E) -> Self {
        Self(e, Vec::new())
    }
}
impl<E: Encoder> Encoder for JsonEscapeStringEncoder<E> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        self.0.encode(idx, &mut self.1)?;
        write_string_quoted_escaped(std::str::from_utf8(&self.1).unwrap(), buf)?;
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct JsonListEncoder<'a, OffsetSize: OffsetSizeTrait, E: Encoder>(
    &'a arrow::array::GenericListArray<OffsetSize>,
    E,
);
impl<OffsetSize: OffsetSizeTrait, E: Encoder> Encoder for JsonListEncoder<'_, OffsetSize, E> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "[")?;
        let start = self.0.value_offsets()[idx].as_usize();
        let end = self.0.value_offsets()[idx + 1].as_usize();
        for slice_idx in start..end {
            if slice_idx != start {
                write!(buf, ",")?;
            }
            self.1.encode(slice_idx, buf)?;
        }
        write!(buf, "]")?;
        Ok(())
    }
}

struct JsonFixedSizeListEncoder<E: Encoder>(usize, E);
impl<E: Encoder> Encoder for JsonFixedSizeListEncoder<E> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "[")?;
        let start = idx * self.0;
        let end = start + self.0;
        for slice_idx in start..end {
            if slice_idx != start {
                write!(buf, ",")?;
            }
            self.1.encode(slice_idx, buf)?;
        }
        write!(buf, "]")?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct JsonStructAoSEncoder<'a>(
    pub &'a arrow::datatypes::Fields,
    pub Vec<Box<dyn Encoder + 'a>>,
);
impl Encoder for JsonStructAoSEncoder<'_> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        buf.write_all(b"{")?;

        for (i, (e, f)) in self.1.iter_mut().zip(self.0.iter()).enumerate() {
            if i != 0 {
                buf.write_all(b",")?;
            }

            write_string_quoted_escaped(f.name(), buf)?;
            buf.write_all(b":")?;
            e.encode(idx, buf)?;
        }

        buf.write_all(b"}")?;

        Ok(())
    }
}

pub struct JsonStructAoAEncoder<'a>(
    pub &'a arrow::datatypes::Fields,
    pub Vec<Box<dyn Encoder + 'a>>,
);
impl Encoder for JsonStructAoAEncoder<'_> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "[")?;

        let mut first = true;
        for e in &mut self.1 {
            if !first {
                write!(buf, ",")?;
            }
            first = false;

            e.encode(idx, buf)?;
        }
        write!(buf, "]")?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::*;
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
            let mut writer = arrow_json::ArrayWriter::new(&mut buf);
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        assert_eq!(std::str::from_utf8(&buf).unwrap(), "[]");
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
                    "bin_large": "6c666f6f",
                    "bin_view": "76666f6f",
                    "decimal": "5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "nullable": 1,
                    "ts_milli": "2020-01-01T12:00:00Z",
                    "uint64": 100,
                    "utf8": "foo",
                    "utf8_special": "sep,quotes'\",newline\nfin",
                    "utf8_large": "lfoo",
                    "utf8_view": "vfoo",
                },
                {
                    "bin": "626172",
                    "bin_fixed": "0304",
                    "bin_large": "6c626172",
                    "bin_view": "76626172",
                    "decimal": "-5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "nullable": null,
                    "ts_milli": "2020-01-01T12:01:00Z",
                    "uint64": 200,
                    "utf8": "bar",
                    "utf8_special": null,
                    "utf8_large": "lbar",
                    "utf8_view": "vbar",
                }
            ]),
        );
    }

    #[test_log::test]
    fn test_json_aos_lists() {
        let batch = get_batch_lists();
        assert_aos_output(
            batch,
            json!([
                {
                    "list_fixed_i32": null,
                    "list_str": null,
                },
                {
                    "list_fixed_i32": [1, 2],
                    "list_str": [],
                },
                {
                    "list_fixed_i32": null,
                    "list_str": ["foo", "bar"],
                },
                {
                    "list_fixed_i32": null,
                    "list_str": ["baz", null],
                },
            ]),
        );
    }

    #[test_log::test]
    fn test_json_aos_structs() {
        let batch = get_batch_structs();
        assert_aos_output(
            batch,
            json!([
                {
                    "struct": {"a": "foo", "b": "baz"},
                },
                {
                    "struct": {"a": "bar", "b": null},
                },
                {
                    "struct": null,
                },
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
                    "lfoo",
                    "vfoo",
                    "666f6f",
                    "0102",
                    "6c666f6f",
                    "76666f6f",
                    "2020-01-01T12:00:00Z",
                ],
                [
                    null,
                    200,
                    "-5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "bar",
                    null,
                    "lbar",
                    "vbar",
                    "626172",
                    "0304",
                    "6c626172",
                    "76626172",
                    "2020-01-01T12:01:00Z",
                ],
            ]),
        );
    }

    #[test_log::test]
    fn test_json_aoa_lists() {
        let batch = get_batch_lists();
        assert_aoa_output(
            batch,
            json!([
                [null, null],
                [[1, 2], []],
                [null, ["foo", "bar"]],
                [null, ["baz", null]]
            ]),
        );
    }

    #[test_log::test]
    fn test_json_aoa_structs() {
        let batch = get_batch_structs();
        assert_aoa_output(batch, json!([[["foo", "baz"]], [["bar", null]], [null]]));
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
                "bin_large": [],
                "bin_view": [],
                "decimal": [],
                "nullable": [],
                "ts_milli": [],
                "uint64": [],
                "utf8": [],
                "utf8_special": [],
                "utf8_large": [],
                "utf8_view": [],
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
                "bin_large": ["6c666f6f", "6c626172"],
                "bin_view":  ["76666f6f", "76626172"],
                "decimal": [
                    "5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "-5789604461865809771178549250434395392663499233282028201972879200395656481996",
                ],
                "nullable": [1, null],
                "ts_milli": ["2020-01-01T12:00:00Z", "2020-01-01T12:01:00Z"],
                "uint64": [100, 200],
                "utf8": ["foo", "bar"],
                "utf8_special": ["sep,quotes'\",newline\nfin", null],
                "utf8_large": ["lfoo", "lbar"],
                "utf8_view": ["vfoo", "vbar"],
            }),
        );
    }

    #[test_log::test]
    fn test_json_soa_lists() {
        let batch = get_batch_lists();
        assert_soa_output(
            batch,
            json!({
                "list_fixed_i32": [null, [1, 2], null, null],
                "list_str": [null, [], ["foo", "bar"], ["baz", null]],
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
                    "bin_large": "6c666f6f",
                    "bin_view": "76666f6f",
                    "decimal": "5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "nullable": 1,
                    "ts_milli": "2020-01-01T12:00:00Z",
                    "uint64": 100,
                    "utf8": "foo",
                    "utf8_special": "sep,quotes'\",newline\nfin",
                    "utf8_large": "lfoo",
                    "utf8_view": "vfoo",
                }),
                json!({
                    "bin": "626172",
                    "bin_fixed": "0304",
                    "bin_large": "6c626172",
                    "bin_view": "76626172",
                    "decimal": "-5789604461865809771178549250434395392663499233282028201972879200395656481996",
                    "nullable": null,
                    "ts_milli": "2020-01-01T12:01:00Z",
                    "uint64": 200,
                    "utf8": "bar",
                    "utf8_special": null,
                    "utf8_large": "lbar",
                    "utf8_view": "vbar",
                }),
            ],
        );
    }

    fn get_empty_batch() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::new(vec![
            Field::new("nullable", DataType::UInt64, true),
            Field::new("uint64", DataType::UInt64, false),
            Field::new("decimal", DataType::Decimal256(76, 0), false),
            Field::new("utf8", DataType::Utf8, false),
            Field::new("utf8_special", DataType::Utf8, true),
            Field::new("utf8_large", DataType::LargeUtf8, false),
            Field::new("utf8_view", DataType::Utf8View, false),
            Field::new("bin", DataType::Binary, false),
            Field::new("bin_fixed", DataType::FixedSizeBinary(2), false),
            Field::new("bin_large", DataType::LargeBinary, false),
            Field::new("bin_view", DataType::BinaryView, false),
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
                Arc::new(StringArray::from(vec!["foo", "bar"])),
                Arc::new(StringArray::from(vec![
                    Some("sep,quotes'\",newline\nfin"),
                    None,
                ])),
                Arc::new(LargeStringArray::from(vec!["lfoo", "lbar"])),
                Arc::new(StringViewArray::from(vec!["vfoo", "vbar"])),
                Arc::new(BinaryArray::from(vec![&b"foo"[..], &b"bar"[..]])),
                Arc::new({
                    let v = vec![vec![1, 2], vec![3, 4]];
                    FixedSizeBinaryArray::try_from_iter(v.into_iter()).unwrap()
                }),
                Arc::new(LargeBinaryArray::from(vec![&b"lfoo"[..], &b"lbar"[..]])),
                Arc::new(BinaryViewArray::from(vec![&b"vfoo"[..], &b"vbar"[..]])),
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

    fn get_batch_lists() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "list_fixed_i32",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 2),
                true,
            ),
            Field::new(
                "list_str",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));

        let mut list_fixed_i32 = FixedSizeListBuilder::new(Int32Builder::new(), 2);
        list_fixed_i32.values().append_null();
        list_fixed_i32.values().append_null();
        list_fixed_i32.append(false); // null
        list_fixed_i32.values().append_value(1);
        list_fixed_i32.values().append_value(2);
        list_fixed_i32.append(true); // [1, 2]
        list_fixed_i32.values().append_null();
        list_fixed_i32.values().append_null();
        list_fixed_i32.append(false); // null
        list_fixed_i32.values().append_null();
        list_fixed_i32.values().append_null();
        list_fixed_i32.append(false); // null

        let mut list_str = ListBuilder::new(StringBuilder::new());
        list_str.append(false); // null
        list_str.append(true); // []
        list_str.values().append_value("foo");
        list_str.values().append_value("bar");
        list_str.append(true); // ["foo", "bar"]
        list_str.values().append_value("baz");
        list_str.values().append_null();
        list_str.append(true); // [null]

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(list_fixed_i32.finish()),
                Arc::new(list_str.finish()),
            ],
        )
        .unwrap()
    }

    fn get_batch_structs() -> RecordBatch {
        let struct_fields = Fields::from(vec![
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "struct",
            DataType::Struct(struct_fields.clone()),
            true,
        )]));

        RecordBatch::try_new(
            schema,
            vec![Arc::new(
                StructArray::try_new(
                    struct_fields,
                    vec![
                        Arc::new(StringArray::from(vec![Some("foo"), Some("bar"), None])),
                        Arc::new(StringArray::from(vec![Some("baz"), None, None])),
                    ],
                    Some(vec![true, true, false].into()),
                )
                .unwrap(),
            )],
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
        pretty_assertions::assert_eq!(actual, expected);
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
        pretty_assertions::assert_eq!(actual, expected);
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
        pretty_assertions::assert_eq!(actual, expected);
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

        pretty_assertions::assert_eq!(actual, expected);
    }
}
