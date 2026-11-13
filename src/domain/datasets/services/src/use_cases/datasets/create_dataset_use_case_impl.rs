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
use internal_error::*;
use kamu_accounts::Principal;
use kamu_datasets::{
    CreateDatasetError,
    CreateDatasetResult,
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
    ResolvedDataset,
};

use crate::utils::CreateDatasetUseCaseHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn CreateDatasetUseCase)]
pub struct CreateDatasetUseCaseImpl {
    principal: Arc<Principal>,
    create_helper: Arc<CreateDatasetUseCaseHelper>,

    // TODO: Rebac is here temporarily - using Lazy to avoid modifying all tests
    rebac_svc: dill::Lazy<Arc<dyn kamu_auth_rebac::RebacService>>,
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

        let principal_account = match self.principal.as_ref() {
            Principal::User(account) => account,
            Principal::Node => {
                return Err(CreateDatasetError::Access(odf::AccessError::Unauthorized(
                    "The node has no account to own a dataset".into(),
                )));
            }
            Principal::Anonymous => {
                panic!("Anonymous account cannot create dataset");
            }
        };

        // Resolve target account and full alias of the dataset
        let (canonical_alias, target_account_id) = self
            .create_helper
            .resolve_alias_target(dataset_alias, principal_account)?;

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
            .store_dataset(
                canonical_alias.as_ref(),
                seed_block,
                odf::dataset::StoreDatasetOpts {
                    seed_block_bytes: options.seed_block_bytes,
                },
            )
            .await?;

        // Set initial dataset HEAD
        let initial_head = options
            .initial_head
            .unwrap_or_else(|| store_result.seed.clone());
        self.create_helper
            .set_created_head(
                ResolvedDataset::from_stored(&store_result, canonical_alias.as_ref()),
                &initial_head,
            )
            .await?;

        // TODO: HACK: SEC: When creating a dataset under another account we currently
        // give the principal a "maintainer" role on it. In future this should be refactored
        // into organization-level permissions.
        //
        // See: https://github.com/kamu-data/kamu-node/issues/233
        if target_account_id != principal_account.did {
            let _ = self
                .rebac_svc
                .get()
                .int_err()?
                .set_account_dataset_relation(
                    &principal_account.did,
                    kamu_auth_rebac::AccountToDatasetRelation::Maintainer,
                    &store_result.dataset_id,
                )
                .await
                .int_err()?;
        }

        // Notify interested parties the dataset was created
        self.create_helper
            .notify_dataset_created(
                &store_result.dataset_id,
                canonical_alias.dataset_name(),
                &target_account_id,
                options.dataset_visibility,
            )
            .await?;

        Ok(CreateDatasetResult {
            head: initial_head,
            ..CreateDatasetResult::from_stored(store_result, canonical_alias.into_inner())
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
