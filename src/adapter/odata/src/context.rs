// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use auth::{DatasetAction, DatasetActionAuthorizer};
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion_odata::collection::{CollectionAddr, QueryParams};
use datafusion_odata::context::{CollectionContext, OnUnsupported, ServiceContext};
use datafusion_odata::error::ODataError;
use dill::Catalog;
use internal_error::ResultIntoInternal;
use kamu_core::auth::DatasetActionAuthorizerExt;
use kamu_core::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Externalize config
const DEFAULT_RECORDS_PER_PAGE: usize = 100;
const MAX_RECORDS_PER_PAGE: usize = usize::MAX;
const KEY_COLUMN_ALIAS: &str = "__id__";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ODataServiceContext {
    catalog: Catalog,
    account_name: Option<odf::AccountName>,
    service_base_url: String,
}

impl ODataServiceContext {
    pub(crate) fn new(catalog: Catalog, account_name: Option<odf::AccountName>) -> Self {
        let config = catalog.get_one::<ServerUrlConfig>().unwrap();
        let service_base_url = config.protocols.odata_base_url();

        Self {
            catalog,
            account_name,
            service_base_url,
        }
    }
}

#[async_trait::async_trait]
impl ServiceContext for ODataServiceContext {
    fn service_base_url(&self) -> String {
        self.service_base_url.clone()
    }

    async fn list_collections(&self) -> Result<Vec<Arc<dyn CollectionContext>>, ODataError> {
        use futures::TryStreamExt;

        let registry: Arc<dyn DatasetRegistry> = self.catalog.get_one().unwrap();
        let authorizer: Arc<dyn DatasetActionAuthorizer> = self.catalog.get_one().unwrap();

        let dataset_handles = if let Some(account_name) = &self.account_name {
            registry.all_dataset_handles_by_owner(account_name)
        } else {
            registry.all_dataset_handles()
        };
        let readable_dataset_handles_stream =
            authorizer.filtered_datasets_stream(dataset_handles, DatasetAction::Read);

        let collections = readable_dataset_handles_stream
            .map_ok(|dataset_handle| {
                let resolved_dataset = registry.get_dataset_by_handle(&dataset_handle);
                let context: Arc<dyn CollectionContext> = Arc::new(ODataCollectionContext {
                    catalog: self.catalog.clone(),
                    addr: CollectionAddr {
                        name: dataset_handle.alias.dataset_name.to_string(),
                        key: None,
                    },
                    resolved_dataset,
                    service_base_url: self.service_base_url.clone(),
                });
                context
            })
            .try_collect::<Vec<_>>()
            .await
            .map_err(ODataError::internal)?;

        Ok(collections)
    }

    fn on_unsupported_feature(&self) -> OnUnsupported {
        OnUnsupported::Warn
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ODataCollectionContext {
    catalog: Catalog,
    addr: CollectionAddr,
    resolved_dataset: ResolvedDataset,
    service_base_url: String,
}

impl ODataCollectionContext {
    pub(crate) fn new(
        catalog: Catalog,
        addr: CollectionAddr,
        resolved_dataset: ResolvedDataset,
    ) -> Self {
        let config = catalog.get_one::<ServerUrlConfig>().unwrap();
        let service_base_url = config.protocols.odata_base_url();

        Self {
            catalog,
            addr,
            resolved_dataset,
            service_base_url,
        }
    }
}

#[async_trait::async_trait]
impl CollectionContext for ODataCollectionContext {
    fn addr(&self) -> Result<&CollectionAddr, ODataError> {
        Ok(&self.addr)
    }

    fn service_base_url(&self) -> Result<String, ODataError> {
        Ok(self.service_base_url.clone())
    }

    fn collection_base_url(&self) -> Result<String, ODataError> {
        Ok(format!(
            "{}/{}",
            self.service_base_url()?,
            self.collection_name()?
        ))
    }

    fn collection_namespace(&self) -> Result<String, ODataError> {
        Ok(datafusion_odata::context::DEFAULT_NAMESPACE.to_string())
    }

    fn collection_name(&self) -> Result<String, ODataError> {
        Ok(self.resolved_dataset.get_alias().dataset_name.to_string())
    }

    async fn last_updated_time(&self) -> DateTime<Utc> {
        use futures::TryStreamExt;
        use odf::dataset::MetadataChainExt;

        let (_, last_block) = self
            .resolved_dataset
            .as_metadata_chain()
            .iter_blocks()
            .try_next()
            .await
            .unwrap()
            .unwrap();

        last_block.system_time
    }

    async fn schema(&self) -> Result<SchemaRef, ODataError> {
        // TODO: Use QueryService after arrow schema is exposed
        // See: https://github.com/kamu-data/kamu-cli/issues/306
        use odf::dataset::{MetadataChainExt, TryStreamExtExt};
        use odf::metadata::EnumWithVariants;

        let set_data_schema = self
            .resolved_dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_map_ok(|(_, b)| b.event.into_variant::<odf::metadata::SetDataSchema>())
            .try_first()
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, error_msg = %e, "Resolving last data slice failed");
                e
            })
            .int_err()
            .unwrap();

        if let Some(set_schema) = set_data_schema {
            set_schema.schema_as_arrow().map_err(ODataError::internal)
        } else {
            Ok(Arc::new(Schema::empty()))
        }
    }

    async fn query(&self, query: QueryParams) -> Result<DataFrame, ODataError> {
        // TODO: Convert into config value
        let default_records_per_page: usize = std::env::var("KAMU_ODATA_DEFAULT_RECORDS_PER_PAGE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_RECORDS_PER_PAGE);

        use odf::dataset::MetadataChainExt;
        let vocab: odf::metadata::DatasetVocabulary = self
            .resolved_dataset
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetVocabVisitor::new())
            .await
            .map_err(ODataError::internal)?
            .into_event()
            .map(Into::into)
            .unwrap_or_default();

        let query_svc: Arc<dyn QueryService> = self.catalog.get_one().unwrap();

        let df = query_svc
            .get_data(&self.resolved_dataset.get_handle().as_local_ref())
            .await
            .unwrap();

        query
            .apply(
                df,
                &self.addr,
                &vocab.offset_column,
                KEY_COLUMN_ALIAS,
                default_records_per_page,
                MAX_RECORDS_PER_PAGE,
            )
            .map_err(ODataError::internal)
    }

    fn on_unsupported_feature(&self) -> OnUnsupported {
        OnUnsupported::Warn
    }
}
