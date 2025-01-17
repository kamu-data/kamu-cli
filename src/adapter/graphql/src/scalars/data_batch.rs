// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Externalize
const MAX_SOA_BUFFER_SIZE: usize = 100_000_000;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataBatchFormat {
    /// Deprecated: Use `JSON_AOS` instead and expect it to become default in
    /// future versions
    #[default]
    Json,
    JsonAOS,
    JsonSOA,
    JsonAOA,
    NdJson,
    Csv,
    /// Deprecated: Use `ND_JSON` instead
    JsonLD,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
                DataBatchFormat::Csv | DataBatchFormat::JsonLD | DataBatchFormat::NdJson => {
                    String::new()
                }
                DataBatchFormat::Json | DataBatchFormat::JsonAOS | DataBatchFormat::JsonAOA => {
                    String::from("[]")
                }
                DataBatchFormat::JsonSOA => String::from("{}"),
            },
            num_records: 0,
        }
    }

    pub fn from_records(
        record_batches: &[datafusion::arrow::record_batch::RecordBatch],
        format: DataBatchFormat,
    ) -> Result<DataBatch> {
        use odf::utils::data::format::*;

        let num_records: usize = record_batches
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum();

        if num_records == 0 {
            return Ok(Self::empty(format));
        }

        let mut buf = Vec::new();
        {
            let mut writer: Box<dyn RecordsWriter> = match format {
                DataBatchFormat::Csv => Box::new(CsvWriter::new(
                    &mut buf,
                    CsvWriterOptions {
                        header: true,
                        ..Default::default()
                    },
                )),
                DataBatchFormat::Json | DataBatchFormat::JsonAOS => {
                    Box::new(JsonArrayOfStructsWriter::new(&mut buf))
                }
                DataBatchFormat::JsonSOA => {
                    Box::new(JsonStructOfArraysWriter::new(&mut buf, MAX_SOA_BUFFER_SIZE))
                }
                DataBatchFormat::JsonAOA => Box::new(JsonArrayOfArraysWriter::new(&mut buf)),
                DataBatchFormat::JsonLD | DataBatchFormat::NdJson => {
                    Box::new(JsonLineDelimitedWriter::new(&mut buf))
                }
            };

            writer.write_batches(record_batches).int_err()?;
            writer.finish().int_err()?;
        }

        Ok(DataBatch {
            format,
            content: String::from_utf8(buf).unwrap(),
            num_records: num_records as u64,
        })
    }
}
