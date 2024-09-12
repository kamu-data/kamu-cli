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

use axum::async_trait;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion_odata::collection::{CollectionAddr, QueryParams};
use datafusion_odata::context::{CollectionContext, OnUnsupported, ServiceContext};
use dill::Catalog;
use internal_error::ResultIntoInternal;
use kamu_core::*;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Externalize config
const DEFAULT_RECORDS_PER_PAGE: usize = 100;
const MAX_RECORDS_PER_PAGE: usize = usize::MAX;
const KEY_COLUMN_ALIAS: &str = "__id__";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ODataServiceContext {
    catalog: Catalog,
    account_name: Option<AccountName>,
    service_base_url: String,
}

impl ODataServiceContext {
    pub(crate) fn new(catalog: Catalog, account_name: Option<AccountName>) -> Self {
        let config = catalog.get_one::<ServerUrlConfig>().unwrap();
        let service_base_url = config.protocols.odata_base_url();

        Self {
            catalog,
            account_name,
            service_base_url,
        }
    }
}

// TODO: Authorization checks
#[async_trait]
impl ServiceContext for ODataServiceContext {
    fn service_base_url(&self) -> String {
        self.service_base_url.clone()
    }

    async fn list_collections(&self) -> Vec<Arc<dyn CollectionContext>> {
        use futures::TryStreamExt;

        let repo: Arc<dyn DatasetRepository> = self.catalog.get_one().unwrap();

        let datasets = if let Some(account_name) = &self.account_name {
            repo.get_datasets_by_owner(account_name)
        } else {
            repo.get_all_datasets()
        };

        let datasets: Vec<_> = datasets.try_collect().await.unwrap();

        let mut collections: Vec<Arc<dyn CollectionContext>> = Vec::new();
        for dataset_handle in datasets {
            let dataset = repo.get_dataset_by_handle(&dataset_handle);

            collections.push(Arc::new(ODataCollectionContext {
                catalog: self.catalog.clone(),
                addr: CollectionAddr {
                    name: dataset_handle.alias.dataset_name.to_string(),
                    key: None,
                },
                dataset_handle,
                dataset,
                service_base_url: self.service_base_url.clone(),
            }));
        }

        collections
    }

    fn on_unsupported_feature(&self) -> OnUnsupported {
        OnUnsupported::Warn
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ODataCollectionContext {
    catalog: Catalog,
    addr: CollectionAddr,
    dataset_handle: DatasetHandle,
    dataset: Arc<dyn Dataset>,
    service_base_url: String,
}

impl ODataCollectionContext {
    pub(crate) fn new(
        catalog: Catalog,
        addr: CollectionAddr,
        dataset_handle: DatasetHandle,
        dataset: Arc<dyn Dataset>,
    ) -> Self {
        let config = catalog.get_one::<ServerUrlConfig>().unwrap();
        let service_base_url = config.protocols.odata_base_url();

        Self {
            catalog,
            addr,
            dataset_handle,
            dataset,
            service_base_url,
        }
    }
}

#[async_trait]
impl CollectionContext for ODataCollectionContext {
    fn addr(&self) -> &CollectionAddr {
        &self.addr
    }

    fn service_base_url(&self) -> String {
        self.service_base_url.clone()
    }

    fn collection_base_url(&self) -> String {
        format!("{}/{}", self.service_base_url(), self.collection_name())
    }

    fn collection_namespace(&self) -> String {
        datafusion_odata::context::DEFAULT_NAMESPACE.to_string()
    }

    fn collection_name(&self) -> String {
        self.dataset_handle.alias.dataset_name.to_string()
    }

    async fn last_updated_time(&self) -> DateTime<Utc> {
        use futures::TryStreamExt;

        let (_, last_block) = self
            .dataset
            .as_metadata_chain()
            .iter_blocks()
            .try_next()
            .await
            .unwrap()
            .unwrap();

        last_block.system_time
    }

    async fn schema(&self) -> SchemaRef {
        // TODO: Use QueryService after arrow schema is exposed
        // See: https://github.com/kamu-data/kamu-cli/issues/306

        let set_data_schema = self
            .dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_map_ok(|(_, b)| b.event.into_variant::<SetDataSchema>())
            .try_first()
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, error_msg = %e, "Resolving last data slice failed");
                e
            })
            .int_err()
            .unwrap();

        if let Some(set_schema) = set_data_schema {
            set_schema.schema_as_arrow().unwrap()
        } else {
            Arc::new(Schema::empty())
        }
    }

    async fn query(&self, query: QueryParams) -> datafusion::error::Result<DataFrame> {
        // TODO: Convert into config value
        let default_records_per_page: usize = std::env::var("KAMU_ODATA_DEFAULT_RECORDS_PER_PAGE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_RECORDS_PER_PAGE);

        let vocab: DatasetVocabulary = self
            .dataset
            .as_metadata_chain()
            .accept_one(SearchSetVocabVisitor::new())
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?
            .into_event()
            .map(Into::into)
            .unwrap_or_default();

        let query_svc: Arc<dyn QueryService> = self.catalog.get_one().unwrap();

        let df = query_svc
            .get_data(&self.dataset_handle.as_local_ref())
            .await
            .unwrap();

        query.apply(
            df,
            &self.addr,
            &vocab.offset_column,
            KEY_COLUMN_ALIAS,
            default_records_per_page,
            MAX_RECORDS_PER_PAGE,
        )
    }

    fn on_unsupported_feature(&self) -> OnUnsupported {
        OnUnsupported::Warn
    }
}
