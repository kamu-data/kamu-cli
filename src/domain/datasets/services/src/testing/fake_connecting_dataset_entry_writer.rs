// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use internal_error::InternalError;
use kamu_datasets::{DatasetEntryCreatedListener, DatasetEntryRemovalListener};

use crate::{CreateDatasetEntryError, DatasetEntryWriter, RenameDatasetEntryError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FakeConnectingDatasetEntryWriter {
    created_listeners: Vec<Arc<dyn DatasetEntryCreatedListener>>,
    removal_listeners: Vec<Arc<dyn DatasetEntryRemovalListener>>,
}

#[component(pub)]
#[interface(dyn DatasetEntryWriter)]
impl FakeConnectingDatasetEntryWriter {
    pub fn new(
        created_listeners: Vec<Arc<dyn DatasetEntryCreatedListener>>,
        removal_listeners: Vec<Arc<dyn DatasetEntryRemovalListener>>,
    ) -> Self {
        Self {
            created_listeners,
            removal_listeners,
        }
    }
}

#[async_trait::async_trait]
impl DatasetEntryWriter for FakeConnectingDatasetEntryWriter {
    async fn create_entry(
        &self,
        dataset_id: &odf::DatasetID,
        _owner_account_id: &odf::AccountID,
        _dataset_name: &odf::DatasetName,
    ) -> Result<(), CreateDatasetEntryError> {
        for listener in &self.created_listeners {
            listener.on_dataset_entry_created(dataset_id).await.unwrap();
        }

        Ok(())
    }

    async fn rename_entry(
        &self,
        _dataset_handle: &odf::DatasetHandle,
        _new_dataset_name: &odf::DatasetName,
    ) -> Result<(), RenameDatasetEntryError> {
        // Nothing to do
        Ok(())
    }

    async fn remove_entry(&self, dataset_handle: &odf::DatasetHandle) -> Result<(), InternalError> {
        for listener in &self.removal_listeners {
            listener
                .on_dataset_entry_removed(&dataset_handle.id)
                .await
                .unwrap();
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
