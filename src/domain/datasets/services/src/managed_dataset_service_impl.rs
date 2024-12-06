// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use database_common::ManagedOperationRef;
use dill::*;
use internal_error::InternalError;
use kamu_core::{ManagedDatasetService, ResolvedDataset};
use kamu_datasets::DatasetReferenceRepository;
use opendatafabric as odf;

use crate::ManagedDatasetImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ManagedDatasetServiceImpl {
    dataset_references_repo: Arc<dyn DatasetReferenceRepository>,
    managed_operation_ref: Arc<ManagedOperationRef>,
    cache: RwLock<Cache>,
}

#[component(pub)]
#[interface(dyn ManagedDatasetService)]
impl ManagedDatasetServiceImpl {
    pub fn new(
        dataset_references_repo: Arc<dyn DatasetReferenceRepository>,
        managed_operation_ref: Arc<ManagedOperationRef>,
    ) -> Self {
        Self {
            dataset_references_repo,
            managed_operation_ref,
            cache: RwLock::default(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct Cache {
    managed_datasets_by_id: HashMap<odf::DatasetID, Arc<ManagedDatasetImpl>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ManagedDatasetService for ManagedDatasetServiceImpl {
    async fn new_managed(
        &self,
        new_unmanaged_dataset: ResolvedDataset,
        head: odf::Multihash,
    ) -> Result<ResolvedDataset, InternalError> {
        {
            let cache_guard = self.cache.read().unwrap();
            assert!(!cache_guard
                .managed_datasets_by_id
                .contains_key(new_unmanaged_dataset.get_id()));
        }
        let dataset_id = new_unmanaged_dataset.get_id().clone();

        let managed_dataset = Arc::new(
            ManagedDatasetImpl::create(
                self.dataset_references_repo.as_ref(),
                (*new_unmanaged_dataset).clone(),
                dataset_id,
                head,
            )
            .await?,
        );

        self.managed_operation_ref
            .track_entity(managed_dataset.clone());

        self.cache.write().unwrap().managed_datasets_by_id.insert(
            new_unmanaged_dataset.get_id().clone(),
            managed_dataset.clone(),
        );

        Ok(ResolvedDataset::new(
            managed_dataset,
            new_unmanaged_dataset.get_handle().clone(),
        ))
    }

    async fn existing_managed(
        &self,
        unmanaged_dataset: ResolvedDataset,
    ) -> Result<ResolvedDataset, InternalError> {
        {
            let cache_guard = self.cache.read().unwrap();
            if let Some(managed_dataset) = cache_guard
                .managed_datasets_by_id
                .get(unmanaged_dataset.get_id())
            {
                return Ok(ResolvedDataset::new(
                    managed_dataset.clone(),
                    unmanaged_dataset.get_handle().clone(),
                ));
            }
        }

        let dataset_id = unmanaged_dataset.get_id().clone();

        let managed_dataset = Arc::new(
            ManagedDatasetImpl::load(
                self.dataset_references_repo.as_ref(),
                (*unmanaged_dataset).clone(),
                dataset_id,
            )
            .await?,
        );

        self.managed_operation_ref
            .track_entity(managed_dataset.clone());

        self.cache
            .write()
            .unwrap()
            .managed_datasets_by_id
            .insert(unmanaged_dataset.get_id().clone(), managed_dataset.clone());

        Ok(ResolvedDataset::new(
            managed_dataset,
            unmanaged_dataset.get_handle().clone(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
