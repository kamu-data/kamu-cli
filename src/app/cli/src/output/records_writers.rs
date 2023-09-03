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
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
pub use kamu_data_utils::data::format::{
    CsvWriter,
    CsvWriterBuilder,
    JsonArrayWriter,
    JsonLineDelimitedWriter,
    RecordsWriter,
};
use prettytable::{Cell, Row, Table};

/////////////////////////////////////////////////////////////////////////////////////////

macro_rules! format_typed {
    ($array_type:ty, $item_type: ty, $array: ident, $value_fmt: ident, $row: ident) => {{
        let t_array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        let t_value_fmt = $value_fmt
            .downcast_ref::<fn($item_type) -> String>()
            .unwrap();
        t_value_fmt(t_array.value($row))
    }};
}

/////////////////////////////////////////////////////////////////////////////////////////

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

    pub fn get_style_spec(&self, _row: usize, column: usize, _array: &ArrayRef) -> &str {
        self.column_formats
            .get(column)
            .and_then(|cf| cf.style_spec.as_ref())
            .or_else(|| self.default_column_format.style_spec.as_ref())
            .map(|s| s.as_str())
            .unwrap()
    }

    pub fn format(&self, row: usize, col: usize, array: &ArrayRef) -> String {
        use datafusion::arrow::array::*;
        use datafusion::arrow::datatypes::TimeUnit;

        // Check for null
        let null_value = self
            .column_formats
            .get(col)
            .and_then(|cf| cf.null_value.as_ref())
            .or_else(|| self.default_column_format.null_value.as_ref())
            .unwrap();

        if array.is_null(row) {
            return null_value.clone();
        }

        // Check for binary data
        match array.data_type() {
            DataType::Binary
            | DataType::LargeBinary
            | DataType::List(_)
            | DataType::LargeList(_) => {
                let binary_placeholder = self
                    .column_formats
                    .get(col)
                    .and_then(|cf| cf.binary_placeholder.as_ref())
                    .or_else(|| self.default_column_format.binary_placeholder.as_ref());

                if let Some(binary_placeholder) = binary_placeholder {
                    return binary_placeholder.clone();
                }
            }
            _ => (),
        }

        // Format value
        let mut value = if let Some(value_fmt) = self
            .column_formats
            .get(col)
            .and_then(|cf| cf.value_fmt.as_ref())
            .or_else(|| self.default_column_format.value_fmt.as_ref())
        {
            match array.data_type() {
                DataType::Int8 => format_typed!(Int8Array, i8, array, value_fmt, row),
                DataType::Int16 => format_typed!(Int16Array, i16, array, value_fmt, row),
                DataType::Int32 => format_typed!(Int32Array, i32, array, value_fmt, row),
                DataType::Int64 => format_typed!(Int64Array, i64, array, value_fmt, row),
                DataType::UInt8 => format_typed!(UInt8Array, u8, array, value_fmt, row),
                DataType::UInt16 => format_typed!(UInt16Array, u16, array, value_fmt, row),
                DataType::UInt32 => format_typed!(UInt32Array, u32, array, value_fmt, row),
                DataType::UInt64 => format_typed!(UInt64Array, u64, array, value_fmt, row),
                DataType::Float32 => format_typed!(Float32Array, f32, array, value_fmt, row),
                DataType::Float64 => format_typed!(Float64Array, f64, array, value_fmt, row),
                DataType::Timestamp(time_unit, _) => match time_unit {
                    TimeUnit::Microsecond => {
                        let t_array = array
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        let t_value_fmt = value_fmt
                            .downcast_ref::<fn(DateTime<Utc>) -> String>()
                            .unwrap();
                        let value = t_array.value_as_datetime(row).unwrap();
                        let value = DateTime::from_naive_utc_and_offset(value, Utc);
                        t_value_fmt(value)
                    }
                    _ => unimplemented!(),
                },
                _ => unimplemented!(),
            }
        } else {
            array_value_to_string(array, row).unwrap()
        };

        // Truncate to limit
        // TODO: Avoid formatting very long values in the first place
        if let Some(max_len) = self
            .column_formats
            .get(col)
            .and_then(|cf| cf.max_len)
            .or_else(|| self.default_column_format.max_len)
        {
            // Quick bytes check
            if value.len() > max_len {
                if let Some((byte_index, _)) = value.char_indices().nth(max_len) {
                    value.truncate(byte_index);
                    value.push_str("...");
                }
            }
        }

        value
    }
}

impl Default for RecordsFormat {
    fn default() -> Self {
        Self {
            column_formats: Vec::new(),
            default_column_format: ColumnFormat::new()
                .with_style_spec("r")
                .with_null_value("")
                .with_binary_placeholder("<binary>")
                .with_max_len(90),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct ColumnFormat {
    style_spec: Option<String>,
    null_value: Option<String>,
    binary_placeholder: Option<String>,
    max_len: Option<usize>,
    value_fmt: Option<Box<dyn Any>>,
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

    pub fn with_binary_placeholder(self, binary_placeholder: impl Into<String>) -> Self {
        Self {
            binary_placeholder: Some(binary_placeholder.into()),
            ..self
        }
    }

    pub fn with_max_len(self, max_len: usize) -> Self {
        Self {
            max_len: Some(max_len),
            ..self
        }
    }

    pub fn with_value_fmt<T: 'static>(self, value_fmt: fn(T) -> String) -> Self {
        Self {
            value_fmt: Some(Box::new(value_fmt)),
            ..self
        }
    }

    pub fn get_style_spec(&self) -> Option<&str> {
        self.style_spec.as_ref().map(|s| s.as_str())
    }
}

impl Default for ColumnFormat {
    fn default() -> Self {
        Self {
            style_spec: None,
            null_value: None,
            binary_placeholder: None,
            max_len: None,
            value_fmt: None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct TableWriter {
    format: RecordsFormat,
    header_written: bool,
    rows_written: usize,
    num_columns: usize,
    table: Table,
}

impl TableWriter {
    // TODO: prettytable is hard to print out into a generic Writer
    // as it wants tty output to implement term::Terminal trait
    pub fn new(format: RecordsFormat) -> Self {
        let mut table = Table::new();
        table.set_format(Self::get_table_format());

        Self {
            format,
            header_written: false,
            rows_written: 0,
            num_columns: 0,
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

impl RecordsWriter for TableWriter {
    fn write_batch(&mut self, records: &RecordBatch) -> Result<(), std::io::Error> {
        if !self.header_written {
            let mut header = Vec::new();
            for field in records.schema().fields() {
                header.push(Cell::new(&field.name()).style_spec("bc"));
            }
            self.table.set_titles(Row::new(header));
            self.header_written = true;
            self.num_columns = records.schema().fields().len();
        }

        for row in 0..records.num_rows() {
            let mut cells = Vec::new();
            for col in 0..records.num_columns() {
                let array = records.column(col);

                let style_spec = self.format.get_style_spec(row, col, &array);
                let value = self.format.format(row, col, &array);
                cells.push(Cell::new(&value).style_spec(style_spec));
            }
            self.table.add_row(Row::new(cells));
            self.rows_written += 1;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), std::io::Error> {
        // BUG: Header doesn't render when there are no data rows in the table
        // so we add an empty row
        if self.rows_written == 0 {
            let row = self.table.add_empty_row();
            for _ in 0..self.num_columns {
                row.add_cell(Cell::new(""));
            }
        }

        self.table.printstd();
        Ok(())
    }
}
