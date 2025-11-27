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
use internal_error::*;
use kamu_accounts::{DEFAULT_ACCOUNT_NAME, LoggedAccount};
use kamu_core::TenancyConfig;
use kamu_datasets::{
    CreateDatasetError,
    CreateDatasetFromSnapshotError,
    DatasetLifecycleMessage,
    DatasetReferenceCASError,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    NameCollisionError,
    ResolvedDataset,
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

    // TODO: This is here temporarily - using Lazy to avoid modifying all tests
    account_svc: dill::Lazy<Arc<dyn kamu_accounts::AccountService>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl CreateDatasetUseCaseHelper {
    /// Given a raw alias under which user wants to create a dataset and user's
    /// credentials determine the target account under which dataset will be
    /// created and whether user has permissions to do so
    pub async fn resolve_alias_target(
        &self,
        raw_alias: &odf::DatasetAlias,
        subject: &LoggedAccount,
    ) -> Result<(CanonicalDatasetAlias, odf::AccountID), CreateDatasetError> {
        match self.tenancy_config.as_ref() {
            TenancyConfig::SingleTenant => {
                // Ignore the account name in the alias and use subject account
                Ok((
                    CanonicalDatasetAlias::new(odf::DatasetAlias::new(
                        None,
                        raw_alias.dataset_name.clone(),
                    )),
                    subject.account_id.clone(),
                ))
            }
            TenancyConfig::MultiTenant => {
                if let Some(account_name) = &raw_alias.account_name
                    && *account_name != subject.account_name
                {
                    // Creating dataset for account different that the subject
                    //
                    // TODO: HACK: SEC: Validate subject account has permissions to create datasets
                    // in target account e.g. by checking `member-of` relationship for
                    // organizations. Currently only allowing cross-account creation for `kamu` and
                    // `molecule` / `molecule.dev`.
                    //
                    // See: https://github.com/kamu-data/kamu-node/issues/233
                    if let Some(account) = self
                        .account_svc
                        .get()
                        .int_err()?
                        .account_by_name(account_name)
                        .await?
                        && (subject.account_name == "kamu"
                            || subject.account_name == "molecule"
                            || subject.account_name == "molecule.dev")
                        && account
                            .account_name
                            .starts_with(subject.account_name.as_str())
                    {
                        Ok((CanonicalDatasetAlias::new(raw_alias.clone()), account.id))
                    } else {
                        Err(odf::AccessError::Unauthorized(
                            format!("Cannot create a dataset in account {account_name}").into(),
                        )
                        .into())
                    }
                } else {
                    Ok((
                        CanonicalDatasetAlias::new(odf::DatasetAlias::new(
                            Some(subject.account_name.clone()),
                            raw_alias.dataset_name.clone(),
                        )),
                        subject.account_id.clone(),
                    ))
                }
            }
        }
    }

    pub fn canonical_dataset_alias(
        &self,
        raw_alias: &odf::DatasetAlias,
        logged_account_name: &odf::AccountName,
    ) -> CanonicalDatasetAlias {
        match self.tenancy_config.as_ref() {
            TenancyConfig::SingleTenant => {
                // Ignore the account name in the alias
                CanonicalDatasetAlias::new(odf::DatasetAlias::new(
                    None,
                    raw_alias.dataset_name.clone(),
                ))
            }
            TenancyConfig::MultiTenant if raw_alias.account_name.is_some() => {
                CanonicalDatasetAlias::new(raw_alias.clone())
            }
            TenancyConfig::MultiTenant => CanonicalDatasetAlias::new(odf::DatasetAlias::new(
                Some(logged_account_name.clone()),
                raw_alias.dataset_name.clone(),
            )),
        }
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
                dataset_alias
                    .account_name
                    .as_ref()
                    .unwrap_or(&DEFAULT_ACCOUNT_NAME),
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
            .store_dataset(seed_block)
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
