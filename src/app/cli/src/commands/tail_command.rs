// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, UInt8Array};
use datafusion::arrow::datatypes::DataType;
use kamu::domain::{GetDataOptions, QueryService};

use super::{CLIError, Command};
use crate::output::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct TailCommand {
    output_cfg: Arc<OutputConfig>,
    query_svc: Arc<dyn QueryService>,

    #[dill::component(explicit)]
    dataset_ref: odf::DatasetRef,

    #[dill::component(explicit)]
    skip: u64,

    #[dill::component(explicit)]
    limit: u64,
}

#[async_trait::async_trait(?Send)]
impl Command for TailCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let res = self
            .query_svc
            .tail_old(
                &self.dataset_ref,
                self.skip,
                self.limit,
                GetDataOptions::default(),
            )
            .await
            .map_err(CLIError::failure)?;

        let (schema, record_batches) = match res.df {
            None => (datafusion::arrow::datatypes::Schema::empty(), Vec::new()),
            Some(df) => (
                df.schema().as_arrow().clone(),
                df.collect().await.map_err(CLIError::failure)?,
            ),
        };

        let mut writer = self.output_cfg.get_records_writer(
            &schema,
            RecordsFormat::default().with_column_formats(vec![
                // TODO: `RecordsFormat` should allow specifying column formats by name, not
                // only positionally
                ColumnFormat::default(),
                ColumnFormat::default().with_value_fmt(|array, row, _| {
                    let err = Err(odf::metadata::InvalidOperationType(0));
                    let op = match array.data_type() {
                        DataType::UInt8 => array
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .map(|a| a.value(row))
                            .map_or(err, odf::metadata::OperationType::try_from),
                        // Compatibility fallback
                        DataType::Int32 => array
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .and_then(|a| u8::try_from(a.value(row)).ok())
                            .map(odf::metadata::OperationType::try_from)
                            .unwrap_or(err),
                        _ => err,
                    };
                    match op {
                        Ok(odf::metadata::OperationType::Append) => "+A",
                        Ok(odf::metadata::OperationType::Retract) => "-R",
                        Ok(odf::metadata::OperationType::CorrectFrom) => "-C",
                        Ok(odf::metadata::OperationType::CorrectTo) => "+C",
                        _ => "??",
                    }
                    .to_string()
                }),
            ]),
        );

        writer.write_batches(&record_batches)?;
        writer.finish()?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
