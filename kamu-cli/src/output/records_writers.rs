use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
pub use kamu::infra::utils::records_writers::{
    CsvWriter, CsvWriterBuilder, JsonArrayWriter, JsonLineDelimitedWriter, RecordsWriter,
};
use prettytable::{Cell, Row, Table};

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
                let value = array_value_to_string(&column, row).unwrap();
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
