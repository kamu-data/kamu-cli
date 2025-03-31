// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::engine::*;
use kamu_core::{ObjectStoreRegistry, *};
use odf::utils::data::DataFrameExt;

use crate::engine::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "info", skip_all)]
pub(crate) async fn preprocess(
    operation_id: &str,
    engine_provisioner: &dyn EngineProvisioner,
    ctx: &SessionContext,
    transform: &odf::metadata::Transform,
    input_data: DataFrameExt,
    maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
) -> Result<Option<DataFrameExt>, EngineError> {
    let engine = match transform.engine().to_lowercase().as_str() {
        "datafusion" => Arc::new(EngineDatafusionInproc::new()),
        engine_id => engine_provisioner
            .provision_engine(engine_id, maybe_listener)
            .await
            .int_err()?,
    };

    let response = engine
        .execute_raw_query(RawQueryRequestExt {
            operation_id: operation_id.to_string(),
            ctx: ctx.clone(),
            input_data,
            transform: transform.clone(),
        })
        .await?;

    Ok(response.output_data)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Called when source does not specify an explicit preprocessing step to
/// perform best-effort processing.
///
/// Currently we use it to automatically:
/// - Rename columns that conflict with system columns
/// - Coerce event time column's type, if present, into a timestamp
pub fn preprocess_default(
    df: DataFrameExt,
    read_step: &odf::metadata::ReadStep,
    merge_strategy: &odf::metadata::MergeStrategy,
    vocab: &odf::metadata::DatasetVocabulary,
    opts: &SchemaInferenceOpts,
) -> Result<DataFrameExt, datafusion::error::DataFusionError> {
    let df = if read_step.schema().is_none() && opts.rename_on_conflict_with_system_column {
        let mut system_cols = vec![&vocab.offset_column, &vocab.system_time_column];

        match merge_strategy {
            odf::metadata::MergeStrategy::Append(_)
            | odf::metadata::MergeStrategy::Ledger(_)
            | odf::metadata::MergeStrategy::Snapshot(_) => {
                system_cols.push(&vocab.operation_type_column);
            }
            odf::metadata::MergeStrategy::ChangelogStream(_)
            | odf::metadata::MergeStrategy::UpsertStream(_) => (),
        }

        let mut select = Vec::new();
        let mut noop = true;

        for field in df.schema().fields() {
            let col_orig = col(Column::from_name(field.name()));
            if system_cols.contains(&field.name()) {
                let new_name = format!("_{}", field.name());

                tracing::debug!(
                    old_name = field.name(),
                    new_name,
                    "Inference: Renaming field that conflicts with a system column"
                );

                noop = false;
                select.push(col_orig.alias(new_name));
            } else {
                select.push(col_orig);
            }
        }

        if noop {
            df
        } else {
            df.select(select)?
        }
    } else {
        df
    };

    let df = if read_step.schema().is_none() && opts.coerce_event_time_column_type {
        let mut select = Vec::new();
        let mut noop = true;

        for field in df.schema().fields() {
            let col_orig = col(Column::from_name(field.name()));
            if *field.name() != vocab.event_time_column {
                select.push(col_orig);
                continue;
            }

            match field.data_type() {
                DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64 => {
                    noop = false;

                    tracing::debug!(
                        column_name = field.name(),
                        original_data_type = ?field.data_type(),
                        "Inference: Treating numeric event time column as a UNIX timestamp in seconds"
                    );

                    select.push(
                        Expr::ScalarFunction(
                            datafusion::logical_expr::expr::ScalarFunction::new_udf(
                                datafusion::functions::datetime::to_timestamp_seconds(),
                                vec![col_orig],
                            ),
                        )
                        .alias(field.name()),
                    );
                }
                // TODO: Support using timestamp formats specified in read block
                DataType::Utf8 => {
                    noop = false;

                    tracing::debug!(
                        column_name = field.name(),
                        original_data_type = ?field.data_type(),
                        "Inference: Treating symbolic event time column as an RFC3339 timestamp"
                    );

                    select.push(
                        Expr::ScalarFunction(
                            datafusion::logical_expr::expr::ScalarFunction::new_udf(
                                datafusion::functions::datetime::to_timestamp_millis(),
                                vec![col_orig],
                            ),
                        )
                        .alias(field.name()),
                    );
                }
                _ => select.push(col_orig),
            }
        }

        if noop {
            df
        } else {
            df.select(select)?
        }
    } else {
        df
    };

    Ok(df)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn new_session_context(
    ingest_config: &EngineConfigDatafusionEmbeddedIngest,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
) -> SessionContext {
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::*;

    let config = ingest_config.0.clone();

    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_object_store_registry(object_store_registry.as_datafusion_registry())
            .build()
            .unwrap(),
    );

    #[allow(unused_mut)]
    let mut ctx = SessionContext::new_with_config_rt(config, runtime);

    // TODO: As part of the ODF spec we should let people opt-in into various
    // SQL extensions on per-transform basis
    cfg_if::cfg_if! {
        if #[cfg(feature = "ingest-evm")] {
            datafusion_ethers::udf::register_all(&mut ctx).unwrap();
        }
    }

    cfg_if::cfg_if! {
        if #[cfg(feature = "query-extensions-json")] {
            datafusion_functions_json::register_all(&mut ctx).unwrap();
        }
    }

    ctx
}
