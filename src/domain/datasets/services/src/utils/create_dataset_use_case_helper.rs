// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::component;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_core::{ResolvedDataset, TenancyConfig};
use kamu_datasets::{
    CreateDatasetError,
    CreateDatasetFromSnapshotError,
    DatasetLifecycleMessage,
    DatasetReferenceCASError,
    NameCollisionError,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use messaging_outbox::{Outbox, OutboxExt};
use nutype::nutype;
use thiserror::Error;

use crate::{CreateDatasetEntryError, DatasetEntryWriter};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct CreateDatasetUseCaseHelper {
    tenancy_config: Arc<TenancyConfig>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl CreateDatasetUseCaseHelper {
    pub fn canonical_dataset_alias(
        &self,
        raw_alias: &odf::DatasetAlias,
        logged_account_name: &odf::AccountName,
    ) -> CanonicalDatasetAlias {
        let alias = match self.tenancy_config.as_ref() {
            TenancyConfig::SingleTenant => {
                // Ignore the account name in the alias
                odf::DatasetAlias::new(None, raw_alias.dataset_name.clone())
            }
            TenancyConfig::MultiTenant => {
                let account_name = if raw_alias.is_multi_tenant() {
                    // Safety: In multi-tenant, we have a name.
                    let alias_account_name = raw_alias.account_name.as_ref().unwrap();
                    alias_account_name
                } else {
                    logged_account_name
                };

                odf::DatasetAlias::new(Some(account_name.clone()), raw_alias.dataset_name.clone())
            }
        };
        CanonicalDatasetAlias::new(alias)
    }

    pub fn validate_canonical_dataset_alias_account_name(
        &self,
        canonical_alias: &CanonicalDatasetAlias,
        logged_account_name: &odf::AccountName,
    ) -> Result<(), ValidateCanonicalDatasetAliasAccountNameError> {
        match self.tenancy_config.as_ref() {
            TenancyConfig::SingleTenant => {
                // No name, nothing to validate.
                Ok(())
            }
            TenancyConfig::MultiTenant => {
                // TODO: Organizations: verify access in more detail:
                //       - Does the requested account exist (if different)?
                //       - Does the current user have access to the requested account?
                if canonical_alias.account_name() != logged_account_name {
                    return Err(ValidateCanonicalDatasetAliasAccountNameError::Access(
                        odf::AccessError::Unauthorized(
                            format!(
                                "No permission to create dataset for another user: {}",
                                canonical_alias.account_name()
                            )
                            .into(),
                        ),
                    ));
                }

                Ok(())
            }
        }
    }

    pub async fn create_dataset_entry(
        &self,
        dataset_id: &odf::DatasetID,
        owner_account_id: &odf::AccountID,
        dataset_alias: &odf::DatasetAlias,
        dataset_kind: odf::DatasetKind,
    ) -> Result<(), CreateDatasetError> {
        self.dataset_entry_writer
            .create_entry(
                dataset_id,
                owner_account_id,
                &dataset_alias.dataset_name,
                dataset_kind,
            )
            .await
            .map_err(|e| match e {
                CreateDatasetEntryError::Internal(e) => CreateDatasetError::Internal(e),
                CreateDatasetEntryError::DuplicateId(e) => {
                    CreateDatasetError::Internal(e.int_err())
                }
                CreateDatasetEntryError::NameCollision(e) => {
                    CreateDatasetError::NameCollision(NameCollisionError {
                        alias: odf::DatasetAlias::new(
                            dataset_alias.account_name.clone(),
                            e.dataset_name,
                        ),
                    })
                }
            })?;

        Ok(())
    }

    pub async fn store_dataset(
        &self,
        dataset_alias: &odf::DatasetAlias,
        seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
    ) -> Result<odf::dataset::StoreDatasetResult, CreateDatasetError> {
        let store_result = self
            .dataset_storage_unit_writer
            .store_dataset(
                seed_block,
                odf::dataset::StoreDatasetOpts { set_head: false },
            )
            .await
            .map_err(|e| match e {
                odf::dataset::StoreDatasetError::RefCollision(e) => {
                    CreateDatasetError::RefCollision(e)
                }
                odf::dataset::StoreDatasetError::Internal(e) => CreateDatasetError::Internal(e),
            })?;

        // Write dataset alias file immediately. Even if creation transaction fails,
        // the dataset is still invisible without HEAD reference present in storage.
        odf::dataset::write_dataset_alias(store_result.dataset.as_ref(), dataset_alias).await?;

        Ok(store_result)
    }

    pub async fn notify_dataset_created(
        &self,
        dataset_id: &odf::DatasetID,
        dataset_name: &odf::DatasetName,
        owner_account_id: &odf::AccountID,
        visibility: odf::DatasetVisibility,
    ) -> Result<(), InternalError> {
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    dataset_id.clone(),
                    owner_account_id.clone(),
                    visibility,
                    dataset_name.clone(),
                ),
            )
            .await
    }

    pub async fn set_created_head(
        &self,
        dataset: ResolvedDataset,
        initial_head: &odf::Multihash,
    ) -> Result<(), CreateDatasetError> {
        dataset
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                initial_head,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Some(None),
                },
            )
            .await
            .map_err(|e| match e {
                odf::dataset::SetChainRefError::CASFailed(e) => {
                    CreateDatasetError::CASFailed(Box::new(DatasetReferenceCASError {
                        dataset_id: dataset.get_id().clone(),
                        block_ref: e.reference,
                        expected_prev_block_hash: e.expected,
                        actual_prev_block_hash: e.actual,
                    }))
                }
                odf::dataset::SetChainRefError::Internal(e) => CreateDatasetError::Internal(e),
                _ => CreateDatasetError::Internal(e.int_err()),
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// New type that guarantees a username for multi-tenant mode.
#[nutype(derive(AsRef))]
struct CanonicalDatasetAlias(odf::DatasetAlias);

impl CanonicalDatasetAlias {
    pub fn account_name(&self) -> &odf::AccountName {
        // Safety: In a canonical alias, we are guaranteed to have a name
        self.as_ref().account_name.as_ref().unwrap()
    }

    pub fn dataset_name(&self) -> &odf::DatasetName {
        &self.as_ref().dataset_name
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ValidateCanonicalDatasetAliasAccountNameError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),
}

impl From<ValidateCanonicalDatasetAliasAccountNameError> for CreateDatasetError {
    fn from(v: ValidateCanonicalDatasetAliasAccountNameError) -> CreateDatasetError {
        match v {
            ValidateCanonicalDatasetAliasAccountNameError::Access(e) => {
                CreateDatasetError::Access(e)
            }
        }
    }
}

impl From<ValidateCanonicalDatasetAliasAccountNameError> for CreateDatasetFromSnapshotError {
    fn from(v: ValidateCanonicalDatasetAliasAccountNameError) -> CreateDatasetFromSnapshotError {
        match v {
            ValidateCanonicalDatasetAliasAccountNameError::Access(e) => {
                CreateDatasetFromSnapshotError::Access(e)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
