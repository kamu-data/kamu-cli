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
use kamu_accounts::CurrentAccountSubject;
use kamu_core::ResolvedDataset;
use kamu_datasets::{
    CreateDatasetError,
    CreateDatasetResult,
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
};

use crate::utils::CreateDatasetUseCaseHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn CreateDatasetUseCase)]
pub struct CreateDatasetUseCaseImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    create_helper: Arc<CreateDatasetUseCaseHelper>,
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl CreateDatasetUseCase for CreateDatasetUseCaseImpl {
    #[tracing::instrument(level = "info", name = CreateDatasetUseCaseImpl_execute, skip_all, fields(dataset_alias))]
    async fn execute(
        &self,
        dataset_alias: &odf::DatasetAlias,
        seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
        options: CreateDatasetUseCaseOptions,
    ) -> Result<CreateDatasetResult, CreateDatasetError> {
        tracing::info!(
            ?seed_block,
            ?options,
            "Initiating creation of dataset from seed block"
        );

        // There must be a logged-in user
        let subject = match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(subj) => subj,
            CurrentAccountSubject::Anonymous(_) => {
                panic!("Anonymous account cannot create dataset");
            }
        };

        // Resolve target account and full alias of the dataset
        let (canonical_alias, target_account_id) = self
            .create_helper
            .resolve_alias_target(dataset_alias, subject)?;

        // Dataset entry goes first, this guarantees name collision check
        self.create_helper
            .create_dataset_entry(
                &seed_block.event.dataset_id,
                &target_account_id,
                canonical_alias.as_ref(),
                seed_block.event.dataset_kind,
            )
            .await?;

        // Make storage level dataset (no HEAD yet)
        let store_result = self
            .create_helper
            .store_dataset(canonical_alias.as_ref(), seed_block)
            .await?;

        // Set initial dataset HEAD
        self.create_helper
            .set_created_head(
                ResolvedDataset::from_stored(&store_result, canonical_alias.as_ref()),
                &store_result.seed,
            )
            .await?;

        // TODO: Creating dataset under another account is not supported yet.
        // In future we should check organization-level permissions here.
        //
        // See: https://github.com/kamu-data/kamu-node/issues/233
        assert_eq!(
            target_account_id, subject.account_id,
            "Creating dataset under another account is not supported yet"
        );

        // Notify interested parties the dataset was created
        self.create_helper
            .notify_dataset_created(
                &store_result.dataset_id,
                canonical_alias.dataset_name(),
                &target_account_id,
                options.dataset_visibility,
            )
            .await?;

        Ok(CreateDatasetResult::from_stored(
            store_result,
            canonical_alias.into_inner(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
