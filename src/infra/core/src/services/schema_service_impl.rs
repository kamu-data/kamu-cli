// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::prelude::ParquetReadOptions;
use internal_error::ResultIntoInternal;
use kamu_core::{QueryError, QueryOptions, SchemaService};
use kamu_datasets::ResolvedDataset;

use crate::SessionContextBuilder;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn SchemaService)]
pub struct SchemaServiceImpl {
    session_context_builder: Arc<SessionContextBuilder>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl SchemaService for SchemaServiceImpl {
    #[tracing::instrument(level = "info", name = SchemaServiceImpl_get_schema, skip_all, fields(hdl = %source.get_handle()))]
    async fn get_schema(
        &self,
        source: ResolvedDataset,
    ) -> Result<Option<Arc<odf::schema::DataSchema>>, QueryError> {
        use odf::dataset::MetadataChainExt;
        let schema = source
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetDataSchemaVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map(|e| Arc::new(e.upgrade().schema));

        Ok(schema)
    }

    #[tracing::instrument(level = "info", skip_all, name = SchemaServiceImpl_get_last_data_chunk_schema_arrow, fields(hdl = %source.get_handle()))]
    async fn get_last_data_chunk_schema_arrow(
        &self,
        source: ResolvedDataset,
    ) -> Result<Option<datafusion::arrow::datatypes::SchemaRef>, QueryError> {
        use odf::dataset::MetadataChainExt;
        let Some(last_data_slice_hash) = source
            .as_metadata_chain()
            .last_data_block_with_new_data()
            .await
            .int_err()
            .map_err(|e| {
                tracing::error!(error = ?e, error_msg = %e, "Resolving last data slice failed");
                e
            })?
            .into_event()
            .and_then(|event| event.new_data)
            .map(|new_data| new_data.physical_hash)
        else {
            return Ok(None);
        };

        let data_url = source
            .as_data_repo()
            .get_internal_url(&last_data_slice_hash)
            .await;

        let ctx = self
            .session_context_builder
            .session_context(QueryOptions {
                input_datasets: Some(BTreeMap::new()),
            })
            .await?;

        let df = ctx
            .read_parquet(
                data_url.to_string(),
                ParquetReadOptions {
                    schema: None,
                    file_sort_order: Vec::new(),
                    file_extension: "",
                    table_partition_cols: Vec::new(),
                    parquet_pruning: None,
                    skip_metadata: None,
                    file_decryption_properties: None,
                    metadata_size_hint: None,
                },
            )
            .await
            .int_err()?;

        Ok(Some(df.schema().inner().clone()))
    }

    #[tracing::instrument(level = "info", skip_all, name = SchemaServiceImpl_get_last_data_chunk_schema_parquet, fields(hdl = %source.get_handle()))]
    async fn get_last_data_chunk_schema_parquet(
        &self,
        source: ResolvedDataset,
    ) -> Result<Option<datafusion::parquet::schema::types::Type>, QueryError> {
        use odf::dataset::MetadataChainExt;
        let Some(last_data_slice_hash) = source
            .as_metadata_chain()
            .last_data_block_with_new_data()
            .await
            .int_err()
            .map_err(|e| {
                tracing::error!(error = ?e, error_msg = %e, "Resolving last data slice failed");
                e
            })?
            .into_event()
            .and_then(|event| event.new_data)
            .map(|new_data| new_data.physical_hash)
        else {
            return Ok(None);
        };

        let data_url = Box::new(
            source
                .as_data_repo()
                .get_internal_url(&last_data_slice_hash)
                .await,
        );

        let ctx = self
            .session_context_builder
            .session_context(QueryOptions {
                input_datasets: Some(BTreeMap::new()),
            })
            .await?;

        let object_store = ctx.runtime_env().object_store(&data_url)?;

        let data_path = object_store::path::Path::from_url_path(data_url.path()).int_err()?;

        let metadata = read_data_slice_metadata(object_store, data_path).await?;

        Ok(Some(metadata.file_metadata().schema().clone()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "debug", skip_all, fields(%data_slice_store_path))]
async fn read_data_slice_metadata(
    object_store: Arc<dyn object_store::ObjectStore>,
    data_slice_store_path: object_store::path::Path,
) -> Result<Arc<ParquetMetaData>, QueryError> {
    let mut parquet_object_reader = ParquetObjectReader::new(object_store, data_slice_store_path);

    use datafusion::parquet::arrow::async_reader::AsyncFileReader;
    let metadata = parquet_object_reader
        .get_metadata(None)
        .await
        .map_err(|e| {
            tracing::error!(
                error = ?e,
                error_msg = %e,
                "SchemaService::read_data_slice_metadata: Parquet reader get metadata failed"
            );
            e
        })
        .int_err()?;

    Ok(metadata)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
