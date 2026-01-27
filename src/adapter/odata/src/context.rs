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

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion_odata::collection::{CollectionAddr, QueryParams};
use datafusion_odata::context::{CollectionContext, OnUnsupported, ServiceContext};
use datafusion_odata::error::{CollectionNotFound, ODataError};
use internal_error::ResultIntoInternal;
use kamu_core::*;
use kamu_datasets::{
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetActionAuthorizerExt,
    DatasetRegistry,
    ResolvedDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Externalize config
const DEFAULT_RECORDS_PER_PAGE: usize = 100;
const MAX_RECORDS_PER_PAGE: usize = usize::MAX;
const KEY_COLUMN_ALIAS: &str = "__id__";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ODataServiceContext {
    catalog: dill::Catalog,
    account_name: Option<odf::AccountName>,
    service_base_url: String,
}

impl ODataServiceContext {
    pub(crate) fn new(catalog: dill::Catalog, account_name: Option<odf::AccountName>) -> Self {
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
            registry.all_dataset_handles_by_owner_name(account_name)
        } else {
            registry.all_dataset_handles()
        };
        let mut readable_dataset_handles_stream =
            authorizer.filtered_datasets_stream(dataset_handles, DatasetAction::Read);

        let mut collections = Vec::new();
        while let Some(hdl) = readable_dataset_handles_stream
            .try_next()
            .await
            .map_err(ODataError::internal)?
        {
            let readable_dataset = registry.get_dataset_by_handle(&hdl).await;
            let context: Arc<dyn CollectionContext> = Arc::new(ODataCollectionContext {
                catalog: self.catalog.clone(),
                addr: CollectionAddr {
                    name: hdl.alias.dataset_name.to_string(),
                    key: None,
                },
                readable_dataset,
                service_base_url: self.service_base_url.clone(),
            });

            collections.push(context);
        }

        Ok(collections)
    }

    fn on_unsupported_feature(&self) -> OnUnsupported {
        OnUnsupported::Warn
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ODataCollectionContext {
    catalog: dill::Catalog,
    addr: CollectionAddr,
    readable_dataset: ResolvedDataset,
    service_base_url: String,
}

impl ODataCollectionContext {
    pub(crate) fn new(
        catalog: dill::Catalog,
        addr: CollectionAddr,
        readable_dataset: ResolvedDataset,
    ) -> Self {
        let config = catalog.get_one::<ServerUrlConfig>().unwrap();
        let service_base_url = config.protocols.odata_base_url();

        Self {
            catalog,
            addr,
            readable_dataset,
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
        Ok(self.readable_dataset.get_alias().dataset_name.to_string())
    }

    async fn last_updated_time(&self) -> DateTime<Utc> {
        let chain = self.readable_dataset.as_metadata_chain();
        let Ok(head) = chain.resolve_ref(&odf::BlockRef::Head).await else {
            panic!("Head block must be resolvable")
        };
        let Ok(head_block) = chain.get_block(&head).await else {
            panic!("Head block must be retrievable")
        };

        head_block.system_time
    }

    async fn schema(&self) -> Result<SchemaRef, ODataError> {
        let schema_svc: Arc<dyn SchemaService> = self.catalog.get_one().unwrap();

        let maybe_data_schema = schema_svc
            .get_schema(self.readable_dataset.clone())
            .await
            .map_int_err(ODataError::internal)?;

        let arrow_schema = if let Some(data_schema) = maybe_data_schema {
            data_schema
                .to_arrow(&odf::metadata::ToArrowSettings::default())
                .map_int_err(ODataError::internal)?
        } else {
            Schema::empty()
        };

        Ok(Arc::new(arrow_schema))
    }

    async fn query(&self, query: QueryParams) -> Result<DataFrame, ODataError> {
        // TODO: Convert into config value
        let default_records_per_page: usize = std::env::var("KAMU_ODATA_DEFAULT_RECORDS_PER_PAGE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_RECORDS_PER_PAGE);

        use odf::dataset::MetadataChainExt;
        let vocab: odf::metadata::DatasetVocabulary = self
            .readable_dataset
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetVocabVisitor::new())
            .await
            .map_err(ODataError::internal)?
            .into_event()
            .map(Into::into)
            .unwrap_or_default();

        let query_svc: Arc<dyn QueryService> = self.catalog.get_one().unwrap();

        let res = query_svc
            .get_data(self.readable_dataset.clone(), GetDataOptions::default())
            .await
            .unwrap();

        let Some(df) = res.df else {
            return Err(ODataError::CollectionNotFound(CollectionNotFound {
                collection: self.readable_dataset.get_alias().to_string(),
            }));
        };

        query
            .apply(
                df.into_inner(),
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
