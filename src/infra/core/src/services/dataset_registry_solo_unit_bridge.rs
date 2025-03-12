// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use internal_error::ResultIntoInternal;
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME_STR};
use kamu_core::{
    DatasetHandlesResolution,
    DatasetRegistry,
    GetMultipleDatasetsError,
    ResolvedDataset,
    TenancyConfig,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetRegistrySoloUnitBridge {
    dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>,
    current_account_subject: Arc<CurrentAccountSubject>,
    tenancy_config: Arc<TenancyConfig>,
}

#[component(pub)]
#[interface(dyn DatasetRegistry)]
impl DatasetRegistrySoloUnitBridge {
    pub fn new(
        dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>,
        current_account_subject: Arc<CurrentAccountSubject>,
        tenancy_config: Arc<TenancyConfig>,
    ) -> Self {
        Self {
            dataset_storage_unit,
            current_account_subject,
            tenancy_config,
        }
    }

    fn normalize_alias(&self, alias: &odf::DatasetAlias) -> odf::DatasetAlias {
        match self.tenancy_config.as_ref() {
            TenancyConfig::SingleTenant => {
                if alias.is_multi_tenant() {
                    assert!(
                        !alias.is_multi_tenant()
                            || alias.account_name.as_ref().unwrap() == DEFAULT_ACCOUNT_NAME_STR,
                        "Multi-tenant refs shouldn't have reached down to here with earlier \
                         validations"
                    );
                }
                alias.clone()
            }
            TenancyConfig::MultiTenant => {
                if alias.is_multi_tenant() {
                    alias.clone()
                } else {
                    match self.current_account_subject.as_ref() {
                        CurrentAccountSubject::Anonymous(_) => {
                            panic!("Anonymous account misused, use multi-tenant alias");
                        }
                        CurrentAccountSubject::Logged(l) => odf::DatasetAlias::new(
                            Some(l.account_name.clone()),
                            alias.dataset_name.clone(),
                        ),
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl odf::dataset::DatasetHandleResolver for DatasetRegistrySoloUnitBridge {
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<odf::DatasetHandle, odf::DatasetRefUnresolvedError> {
        match dataset_ref {
            odf::DatasetRef::Handle(h) => Ok(h.clone()),
            odf::DatasetRef::Alias(alias) => {
                let normalized_alias = self.normalize_alias(alias);

                use futures::StreamExt;
                let mut datasets = self.all_dataset_handles();
                while let Some(hdl) = datasets.next().await {
                    let hdl = hdl?;
                    if hdl.alias == normalized_alias {
                        return Ok(hdl);
                    }
                }
                Err(odf::DatasetRefUnresolvedError::NotFound(
                    odf::DatasetNotFoundError {
                        dataset_ref: alias.as_local_ref(),
                    },
                ))
            }
            odf::DatasetRef::ID(dataset_id) => {
                let dataset = self
                    .dataset_storage_unit
                    .get_stored_dataset_by_id(dataset_id)
                    .await
                    .map_err(odf::DatasetRefUnresolvedError::from)?;
                let dataset_alias = odf::dataset::read_dataset_alias(dataset.as_ref()).await?;
                Ok(odf::DatasetHandle::new(dataset_id.clone(), dataset_alias))
            }
        }
    }
}

#[async_trait::async_trait]
impl DatasetRegistry for DatasetRegistrySoloUnitBridge {
    fn all_dataset_handles(&self) -> odf::dataset::DatasetHandleStream<'_> {
        Box::pin(async_stream::try_stream! {
            use futures::TryStreamExt;
            let mut dataset_ids_stream = self.dataset_storage_unit.stored_dataset_ids();
            while let Some(dataset_id) = dataset_ids_stream.try_next().await? {
                let dataset = self.dataset_storage_unit.get_stored_dataset_by_id(&dataset_id).await.int_err()?;
                let dataset_alias = odf::dataset::read_dataset_alias(dataset.as_ref()).await?;

                yield odf::DatasetHandle::new(dataset_id, dataset_alias);
            }
        })
    }

    fn all_dataset_handles_by_owner(
        &self,
        owner_name: &odf::AccountName,
    ) -> odf::dataset::DatasetHandleStream<'_> {
        let owner_name = owner_name.clone();
        Box::pin(async_stream::try_stream! {
            use futures::TryStreamExt;
            let mut dataset_handles_stream = self.all_dataset_handles();
            while let Some(hdl) = dataset_handles_stream.try_next().await? {
                match self.tenancy_config.as_ref() {
                    TenancyConfig::MultiTenant => {
                        if hdl.alias.account_name.as_ref() == Some(&owner_name) {
                            yield hdl;
                        }
                    }
                    TenancyConfig::SingleTenant => {
                        yield hdl;
                    }
                }

            }
        })
    }

    async fn resolve_multiple_dataset_handles_by_ids(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<DatasetHandlesResolution, GetMultipleDatasetsError> {
        use odf::dataset::DatasetHandleResolver;

        let mut res: DatasetHandlesResolution = Default::default();

        for dataset_id in dataset_ids {
            let dataset_ref = dataset_id.as_local_ref();
            let resolve_res = self.resolve_dataset_handle_by_ref(&dataset_ref).await;
            match resolve_res {
                Ok(hdl) => res.resolved_handles.push(hdl),
                Err(e) => res.unresolved_datasets.push((dataset_id.clone(), e)),
            }
        }

        Ok(res)
    }

    async fn get_dataset_by_handle(&self, dataset_handle: &odf::DatasetHandle) -> ResolvedDataset {
        let dataset = self
            .dataset_storage_unit
            .get_stored_dataset_by_id(&dataset_handle.id)
            .await
            .expect("Dataset must exist");
        ResolvedDataset::new(dataset, dataset_handle.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
