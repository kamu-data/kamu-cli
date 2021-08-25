use std::io::Write;

pub use datafusion::arrow::csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
pub use datafusion::arrow::json::ArrayWriter as JsonArrayWriter;
pub use datafusion::arrow::json::LineDelimitedWriter as JsonLineDelimitedWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use prettytable::{Cell, Row, Table};

use crate::CLIError;

pub trait RecordsWriter {
    fn write_batch(&mut self, records: &RecordBatch) -> Result<(), CLIError>;

    fn write_batches(&mut self, record_batches: &[RecordBatch]) -> Result<(), CLIError> {
        for records in record_batches {
            self.write_batch(records)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), CLIError> {
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct TableWriter {
    first_row_written: bool,
    table: Table,
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
    fn write_batch(&mut self, records: &RecordBatch) -> Result<(), CLIError> {
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
                let value =
                    array_value_to_string(&column, row).map_err(|e| CLIError::failure(e))?;
                cells.push(Cell::new(&value).style_spec("r"));
            }
            self.table.add_row(Row::new(cells));
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), CLIError> {
        self.table.printstd();
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<W: Write> RecordsWriter for CsvWriter<W> {
    fn write_batch(&mut self, records: &RecordBatch) -> Result<(), CLIError> {
        self.write(records).map_err(|e| CLIError::failure(e))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<W: Write> RecordsWriter for JsonArrayWriter<W> {
    fn write_batch(&mut self, _records: &RecordBatch) -> Result<(), CLIError> {
        unimplemented!();
    }

    fn write_batches(&mut self, record_batches: &[RecordBatch]) -> Result<(), CLIError> {
        JsonArrayWriter::write_batches(self, record_batches).map_err(|e| CLIError::failure(e))
    }

    fn finish(&mut self) -> Result<(), CLIError> {
        JsonArrayWriter::finish(self).map_err(|e| CLIError::failure(e))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<W: Write> RecordsWriter for JsonLineDelimitedWriter<W> {
    fn write_batch(&mut self, _records: &RecordBatch) -> Result<(), CLIError> {
        unimplemented!();
    }

    fn write_batches(&mut self, record_batches: &[RecordBatch]) -> Result<(), CLIError> {
        JsonLineDelimitedWriter::write_batches(self, record_batches)
            .map_err(|e| CLIError::failure(e))
    }

    fn finish(&mut self) -> Result<(), CLIError> {
        JsonLineDelimitedWriter::finish(self).map_err(|e| CLIError::failure(e))
    }
}
