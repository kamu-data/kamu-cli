// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use odf::utils::data::format::WriterError;
pub use odf::utils::data::format::{
    CsvWriter,
    CsvWriterOptions,
    JsonArrayOfArraysWriter,
    JsonArrayOfStructsWriter,
    JsonLineDelimitedWriter,
    JsonStructOfArraysWriter,
    RecordsWriter,
};
use prettytable::{Cell, Row, Table};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! format_typed {
    ($array_type:ty, $item_type: ty, $actual_type: expr, $array: ident, $value_fmt: ident, $row: ident) => {{
        let t_array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        let t_value_fmt = $value_fmt
            .downcast_ref::<fn($item_type) -> String>()
            .unwrap_or_else(|| {
                panic!(
                    "Expected formatter for type {} but got {} instead",
                    stringify!($item_type),
                    $actual_type,
                )
            });
        t_value_fmt(t_array.value($row))
    }};
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RecordsFormat {
    column_formats: Vec<ColumnFormat>,
    default_column_format: ColumnFormat,
}

impl RecordsFormat {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_column_formats(self, column_formats: Vec<ColumnFormat>) -> Self {
        Self {
            column_formats,
            ..self
        }
    }

    pub fn with_default_column_format(self, default_column_format: ColumnFormat) -> Self {
        Self {
            default_column_format,
            ..self
        }
    }

    pub fn get_column_format(&self, col: usize) -> ColumnFormatRef {
        let cfmt = self.column_formats.get(col);

        ColumnFormatRef {
            style_spec: cfmt
                .and_then(|f| f.style_spec.as_ref())
                .or(self.default_column_format.style_spec.as_ref()),
            null_value: cfmt
                .and_then(|f| f.null_value.as_ref())
                .or(self.default_column_format.null_value.as_ref()),
            max_len: cfmt
                .and_then(|f| f.max_len)
                .or(self.default_column_format.max_len),
            max_bin_len: cfmt
                .and_then(|f| f.max_bin_len)
                .or(self.default_column_format.max_bin_len),
            value_fmt: cfmt
                .and_then(|f| f.value_fmt.as_ref())
                .or(self.default_column_format.value_fmt.as_ref()),
        }
    }

    // TODO: PERF: Rethink into a columnar approach
    pub fn format(&self, row: usize, col: usize, array: &ArrayRef) -> String {
        use datafusion::arrow::array::*;

        let column_fmt = self.get_column_format(col);

        // Check for null
        if array.is_null(row) {
            return column_fmt.null_value.unwrap().clone();
        }

        // Format value
        let mut value = if let Some(value_fmt) = column_fmt.value_fmt {
            (*value_fmt)(array, row, &column_fmt)
        } else {
            match array.data_type() {
                DataType::Utf8 => {
                    Self::format_str(downcast_array::<StringArray>(array).value(row), &column_fmt)
                }
                DataType::LargeUtf8 => Self::format_str(
                    downcast_array::<LargeStringArray>(array).value(row),
                    &column_fmt,
                ),
                DataType::Binary => Self::format_binary(
                    downcast_array::<BinaryArray>(array).value(row),
                    &column_fmt,
                ),
                DataType::LargeBinary => Self::format_binary(
                    downcast_array::<LargeBinaryArray>(array).value(row),
                    &column_fmt,
                ),
                _ => array_value_to_string(array, row).unwrap(),
            }
        };

        // Truncate to limit
        // TODO: Avoid formatting very long values in the first place
        if let Some(max_len) = self
            .column_formats
            .get(col)
            .and_then(|cf| cf.max_len)
            .or(self.default_column_format.max_len)
        {
            // Quick bytes check
            if value.len() > max_len
                && let Some((byte_index, _)) = value.char_indices().nth(max_len)
            {
                value.truncate(byte_index);
                value.push('…');
            }
        }

        value
    }

    fn format_str(v: &str, fmt: &ColumnFormatRef) -> String {
        let max_len = fmt.max_len.unwrap_or(usize::MAX);

        if v.len() <= max_len {
            v.to_string()
        } else {
            // TODO: Account for UTF
            let half_len = max_len / 2;
            let head = &v[..half_len];
            let tail = &v[usize::max(half_len, v.len() - half_len)..];
            format!("{head}…{tail}")
        }
    }

    fn format_binary(v: &[u8], fmt: &ColumnFormatRef) -> String {
        let max_len = fmt.max_bin_len.or(fmt.max_len).unwrap_or(usize::MAX);
        let max_len_bin = max_len / 2;
        if v.len() <= max_len_bin {
            hex::encode(v)
        } else {
            let half_len_bin = max_len_bin / 2;
            let head = &v[..half_len_bin];
            let tail = &v[usize::max(half_len_bin, v.len() - half_len_bin)..];
            format!("{}…{}", hex::encode(head), hex::encode(tail))
        }
    }
}

impl Default for RecordsFormat {
    fn default() -> Self {
        Self {
            column_formats: Vec::new(),
            default_column_format: ColumnFormat::new()
                .with_style_spec("r")
                .with_null_value("")
                .with_max_len(90)
                .with_max_binary_len(13),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type ValueFormatterCallback = Box<dyn Fn(&ArrayRef, usize, &ColumnFormatRef) -> String + Send>;

#[derive(Default)]
pub struct ColumnFormat {
    style_spec: Option<String>,
    null_value: Option<String>,
    max_len: Option<usize>,
    max_bin_len: Option<usize>,
    value_fmt: Option<ValueFormatterCallback>,
}

impl ColumnFormat {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_style_spec(self, style_spec: impl Into<String>) -> Self {
        Self {
            style_spec: Some(style_spec.into()),
            ..self
        }
    }

    pub fn with_null_value(self, null_value: impl Into<String>) -> Self {
        Self {
            null_value: Some(null_value.into()),
            ..self
        }
    }

    /// Determines maximum length of the formatted value in its textual
    /// representation
    pub fn with_max_len(self, max_len: usize) -> Self {
        Self {
            max_len: Some(max_len),
            ..self
        }
    }

    /// Specialization of max length for binary fields (length given is still in
    /// textual hex representation)
    pub fn with_max_binary_len(self, max_bin_len: usize) -> Self {
        Self {
            max_bin_len: Some(max_bin_len),
            ..self
        }
    }

    pub fn with_value_fmt<F>(self, value_fmt: F) -> Self
    where
        F: Fn(&ArrayRef, usize, &ColumnFormatRef) -> String + Send + 'static,
    {
        Self {
            value_fmt: Some(Box::new(value_fmt)),
            ..self
        }
    }

    pub fn with_value_fmt_t<T: 'static>(self, value_fmt_t: fn(T) -> String) -> Self {
        let value_fmt_t: Box<dyn Any + Send> = Box::new(value_fmt_t);
        Self {
            value_fmt: Some(Box::new(move |array, row, _| {
                Self::value_fmt_t(array, row, value_fmt_t.as_ref(), std::any::type_name::<T>())
            })),
            ..self
        }
    }

    fn value_fmt_t(
        array: &ArrayRef,
        row: usize,
        value_fmt: &dyn Any,
        type_name: &'static str,
    ) -> String {
        use datafusion::arrow::array::*;
        use datafusion::arrow::datatypes::TimeUnit;

        match array.data_type() {
            DataType::Int8 => format_typed!(Int8Array, i8, type_name, array, value_fmt, row),
            DataType::Int16 => format_typed!(Int16Array, i16, type_name, array, value_fmt, row),
            DataType::Int32 => format_typed!(Int32Array, i32, type_name, array, value_fmt, row),
            DataType::Int64 => format_typed!(Int64Array, i64, type_name, array, value_fmt, row),
            DataType::UInt8 => format_typed!(UInt8Array, u8, type_name, array, value_fmt, row),
            DataType::UInt16 => format_typed!(UInt16Array, u16, type_name, array, value_fmt, row),
            DataType::UInt32 => format_typed!(UInt32Array, u32, type_name, array, value_fmt, row),
            DataType::UInt64 => format_typed!(UInt64Array, u64, type_name, array, value_fmt, row),
            DataType::Float32 => format_typed!(Float32Array, f32, type_name, array, value_fmt, row),
            DataType::Float64 => format_typed!(Float64Array, f64, type_name, array, value_fmt, row),
            DataType::Timestamp(time_unit, _) => match time_unit {
                TimeUnit::Microsecond => {
                    let t_array = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    let t_value_fmt = value_fmt
                        .downcast_ref::<fn(DateTime<Utc>) -> String>()
                        .unwrap_or_else(|| {
                            panic!(
                                "Expected formatter for type DateTime<Utc> but for {type_name} \
                                 instead",
                            )
                        });
                    let value = t_array.value_as_datetime(row).unwrap();
                    let value = DateTime::from_naive_utc_and_offset(value, Utc);
                    t_value_fmt(value)
                }
                _ => unimplemented!(),
            },
            DataType::Utf8 => format_typed!(StringArray, &str, type_name, array, value_fmt, row),
            DataType::LargeUtf8 => {
                format_typed!(LargeStringArray, &str, type_name, array, value_fmt, row)
            }
            DataType::Binary => format_typed!(BinaryArray, &[u8], type_name, array, value_fmt, row),
            DataType::LargeBinary => {
                format_typed!(BinaryArray, &[u8], type_name, array, value_fmt, row)
            }
            _ => unimplemented!(),
        }
    }
}

pub struct ColumnFormatRef<'a> {
    style_spec: Option<&'a String>,
    null_value: Option<&'a String>,
    max_len: Option<usize>,
    max_bin_len: Option<usize>,
    value_fmt: Option<&'a ValueFormatterCallback>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TableWriter<W> {
    out: W,
    format: RecordsFormat,
    rows_written: usize,
    num_columns: usize,
    table: Table,
}

impl<W> TableWriter<W> {
    // TODO: prettytable is hard to print out into a generic Writer
    // as it wants tty output to implement term::Terminal trait
    pub fn new(schema: &Schema, format: RecordsFormat, out: W) -> Self {
        let mut table = Table::new();
        table.set_format(Self::get_table_format());

        let mut header = Vec::new();
        for field in schema.fields() {
            header.push(Cell::new(field.name()).style_spec("bc"));
        }
        table.set_titles(Row::new(header));

        Self {
            out,
            format,
            rows_written: 0,
            num_columns: schema.fields().len(),
            table,
        }
    }

    pub fn get_table_format() -> prettytable::format::TableFormat {
        use prettytable::format::*;

        FormatBuilder::new()
            .column_separator('│')
            .borders('│')
            .separators(&[LinePosition::Top], LineSeparator::new('─', '┬', '┌', '┐'))
            .separators(
                &[LinePosition::Title],
                LineSeparator::new('─', '┼', '├', '┤'),
            )
            .separators(
                &[LinePosition::Bottom],
                LineSeparator::new('─', '┴', '└', '┘'),
            )
            .padding(1, 1)
            .build()
    }
}

impl<W> RecordsWriter for TableWriter<W>
where
    W: std::io::Write,
{
    fn write_batch(&mut self, records: &RecordBatch) -> Result<(), WriterError> {
        for row in 0..records.num_rows() {
            let mut cells = Vec::new();
            for col in 0..records.num_columns() {
                let array = records.column(col);

                let style_spec = self.format.get_column_format(col).style_spec.unwrap();
                let value = self.format.format(row, col, array);
                cells.push(Cell::new(&value).style_spec(style_spec));
            }
            self.table.add_row(Row::new(cells));
            self.rows_written += 1;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        // BUG: Header doesn't render when there are no data rows in the table,
        // so we add an empty row
        if self.rows_written == 0 {
            let row = self.table.add_empty_row();
            for _ in 0..self.num_columns {
                row.add_cell(Cell::new(""));
            }
        }

        self.table
            .print(&mut self.out)
            .map_err(WriterError::IoError)?;
        Ok(())
    }
}
