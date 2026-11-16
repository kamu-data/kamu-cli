// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_auth_rebac::RebacDatasetRegistryFacade;
use kamu_core::{GetDatasetSchemaUseCase, QueryError, SchemaService};

use super::helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn GetDatasetSchemaUseCase)]
pub struct GetDatasetSchemaUseCaseImpl {
    schema_service: Arc<dyn SchemaService>,
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl GetDatasetSchemaUseCase for GetDatasetSchemaUseCaseImpl {
    #[tracing::instrument(level = "debug", name = GetDatasetSchemaUseCaseImpl_get_schema, skip_all, fields(%dataset_ref))]
    async fn get_schema(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<Arc<odf::schema::DataSchema>>, QueryError> {
        // Resolve source with ReBAC check
        let source = helpers::resolve_dataset_for_querying(
            self.rebac_dataset_registry_facade.as_ref(),
            dataset_ref,
        )
        .await?;

        // Get schema
        self.schema_service.get_schema(source.clone()).await
    }

    #[tracing::instrument(level = "debug", name = GetDatasetSchemaUseCaseImpl_get_last_data_chunk_schema_arrow, skip_all, fields(%dataset_ref))]
    async fn get_last_data_chunk_schema_arrow(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<datafusion::arrow::datatypes::SchemaRef>, QueryError> {
        // Resolve source with ReBAC check
        let source = helpers::resolve_dataset_for_querying(
            self.rebac_dataset_registry_facade.as_ref(),
            dataset_ref,
        )
        .await?;

        // Get last data chunk schema in Arrow format
        self.schema_service
            .get_last_data_chunk_schema_arrow(source)
            .await
    }

    #[tracing::instrument(level = "debug", name = GetDatasetSchemaUseCaseImpl_get_last_data_chunk_schema_parquet, skip_all, fields(%dataset_ref))]
    async fn get_last_data_chunk_schema_parquet(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<datafusion::parquet::schema::types::Type>, QueryError> {
        // Resolve source with ReBAC check
        let source = helpers::resolve_dataset_for_querying(
            self.rebac_dataset_registry_facade.as_ref(),
            dataset_ref,
        )
        .await?;

        // Get last data chunk schema in Parquet format
        self.schema_service
            .get_last_data_chunk_schema_parquet(source)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
