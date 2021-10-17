// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::util::display::array_value_to_string;
use datafusion::arrow::{datatypes::DataType, record_batch::RecordBatch};
pub use kamu::infra::utils::records_writers::{
    CsvWriter, CsvWriterBuilder, JsonArrayWriter, JsonLineDelimitedWriter, RecordsWriter,
};
use prettytable::{Cell, Row, Table};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct TableWriter {
    first_row_written: bool,
    table: Table,
    max_cell_len: Option<usize>,
    binary_placeholder: Option<String>,
}

impl TableWriter {
    // TODO: prettytable is hard to print out into a generic Writer
    // as it wants tty output to implement term::Terminal trait
    pub fn new() -> Self {
        let mut table = Table::new();
        table.set_format(Self::get_table_format());

        Self {
            first_row_written: false,
            table,
            max_cell_len: None,
            binary_placeholder: None,
        }
    }

    pub fn with_max_cell_len(self, max_cell_len: Option<usize>) -> Self {
        Self {
            max_cell_len,
            ..self
        }
    }

    pub fn with_binary_placeholder(self, binary_placeholder: Option<String>) -> Self {
        Self {
            binary_placeholder,
            ..self
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
        if !self.first_row_written {
            let mut header = Vec::new();
            for field in records.schema().fields() {
                header.push(Cell::new(&field.name()).style_spec("bc"));
            }
            self.table.set_titles(Row::new(header));
            self.first_row_written = true;
        }

        for row in 0..records.num_rows() {
            let mut cells = Vec::new();
            for col in 0..records.num_columns() {
                let column = records.column(col);

                if let Some(binary_placeholder) = &self.binary_placeholder {
                    match column.data_type() {
                        DataType::Binary
                        | DataType::LargeBinary
                        | DataType::List(_)
                        | DataType::LargeList(_) => {
                            cells.push(Cell::new(binary_placeholder).style_spec("r"));
                            continue;
                        }
                        _ => (),
                    }
                }

                let mut value = array_value_to_string(&column, row).unwrap();

                if self.max_cell_len.is_some() && value.len() > self.max_cell_len.unwrap() {
                    value.truncate(self.max_cell_len.unwrap());
                    value.push_str("...");
                }

                cells.push(Cell::new(&value).style_spec("r"));
            }
            self.table.add_row(Row::new(cells));
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), std::io::Error> {
        self.table.printstd();
        Ok(())
    }
}
