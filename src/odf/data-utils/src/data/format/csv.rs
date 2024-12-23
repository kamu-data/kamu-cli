// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;

use arrow::array::AsArray;
use arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;

use super::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
            encoders.push(encoder_for(f.data_type(), c, &self.options));
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn encoder_for<'a>(
    dt: &'a DataType,
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
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_) => Box::new(CsvNullableEncoder(
            c,
            CsvEscapeStringEncoder::new(
                super::json::encoder_for(dt, c, &|f, e| {
                    Box::new(super::json::JsonStructAoSEncoder(f, e))
                }),
                opts,
            ),
        )),
        DataType::BinaryView
        | DataType::Utf8View
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::Map(_, _)
        | DataType::RunEndEncoded(_, _) => {
            unimplemented!("CSV encoding is not yet supported for {}", dt)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

impl<E: Encoder> Encoder for CsvEscapeStringEncoder<'_, E> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        self.0.encode(idx, &mut self.2)?;
        CsvEscapeStringEncoder::write_escaped(buf, std::str::from_utf8(&self.2).unwrap(), self.1)?;
        self.2.clear();
        Ok(())
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::*;

    use super::*;

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

    #[test_log::test]
    fn test_csv_nested() {
        let batch = get_nested_batch();
        assert_csv_output(
            batch,
            CsvWriterOptions::default(),
            indoc::indoc!(
                r#"
                list_fixed_i32,list_str
                ,
                "[1,2]",[]
                ,"[""foo"",""bar""]"
                ,"[""baz"",null]"
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

    fn get_nested_batch() -> RecordBatch {
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
