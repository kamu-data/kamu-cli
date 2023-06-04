// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataBatchFormat {
    Json,
    JsonLD,
    JsonSOA,
    Csv,
}

#[derive(SimpleObject)]
pub struct DataBatch {
    pub format: DataBatchFormat,
    pub content: String,
    pub num_records: u64,
}

impl DataBatch {
    pub fn empty(format: DataBatchFormat) -> DataBatch {
        DataBatch {
            format,
            content: match format {
                DataBatchFormat::Csv => String::from(""),
                DataBatchFormat::Json | DataBatchFormat::JsonLD | DataBatchFormat::JsonSOA => {
                    String::from("{}")
                }
            },
            num_records: 0,
        }
    }

    pub fn from_records(
        record_batches: &Vec<datafusion::arrow::record_batch::RecordBatch>,
        format: DataBatchFormat,
    ) -> Result<DataBatch> {
        use kamu_data_utils::data::format::*;

        let num_records: usize = record_batches.iter().map(|b| b.num_rows()).sum();

        let mut buf = Vec::new();
        {
            let mut writer: Box<dyn RecordsWriter> = match format {
                DataBatchFormat::Csv => {
                    Box::new(CsvWriterBuilder::new().has_headers(true).build(&mut buf))
                }
                DataBatchFormat::Json => {
                    // HACK: JsonArrayWriter should be producing [] when there are no rows
                    if num_records != 0 {
                        Box::new(JsonArrayWriter::new(&mut buf))
                    } else {
                        return Ok(DataBatch {
                            format,
                            content: "[]".to_string(),
                            num_records: num_records as u64,
                        });
                    }
                }
                DataBatchFormat::JsonLD => Box::new(JsonLineDelimitedWriter::new(&mut buf)),
                DataBatchFormat::JsonSOA => {
                    unimplemented!("SoA Json format is not yet implemented")
                }
            };

            writer.write_batches(record_batches)?;
            writer.finish()?;
        }

        Ok(DataBatch {
            format,
            content: String::from_utf8(buf).unwrap(),
            num_records: num_records as u64,
        })
    }
}
