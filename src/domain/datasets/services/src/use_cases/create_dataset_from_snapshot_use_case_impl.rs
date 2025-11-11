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
use kamu_accounts::{
    CurrentAccountSubject,
    DidEntity,
    DidSecretEncryptionConfig,
    DidSecretKey,
    DidSecretKeyRepository,
};
use kamu_core::{DatasetRegistry, DidGenerator, ResolvedDataset};
use kamu_datasets::{
    CreateDatasetFromSnapshotError,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetResult,
    CreateDatasetUseCaseOptions,
};
use secrecy::{ExposeSecret, SecretString};
use time_source::SystemTimeSource;

use crate::utils::CreateDatasetUseCaseHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CreateDatasetFromSnapshotUseCaseImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    system_time_source: Arc<dyn SystemTimeSource>,
    did_generator: Arc<dyn DidGenerator>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    create_helper: Arc<CreateDatasetUseCaseHelper>,
    did_secret_encryption_key: Option<SecretString>,
    did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,

    // TODO: Rebac is here temporarily - using Lazy to avoid modifying all tests
    rebac_svc: dill::Lazy<Arc<dyn kamu_auth_rebac::RebacService>>,
}

#[component(pub)]
#[interface(dyn CreateDatasetFromSnapshotUseCase)]
impl CreateDatasetFromSnapshotUseCaseImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        current_account_subject: Arc<CurrentAccountSubject>,
        system_time_source: Arc<dyn SystemTimeSource>,
        did_generator: Arc<dyn DidGenerator>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        create_helper: Arc<CreateDatasetUseCaseHelper>,
        did_secret_encryption_config: Arc<DidSecretEncryptionConfig>,
        did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,
        rebac_svc: dill::Lazy<Arc<dyn kamu_auth_rebac::RebacService>>,
    ) -> Self {
        Self {
            current_account_subject,
            system_time_source,
            did_generator,
            dataset_registry,
            create_helper,
            did_secret_encryption_key: did_secret_encryption_config
                .encryption_key
                .as_ref()
                .map(|encryption_key| SecretString::from(encryption_key.clone())),
            did_secret_key_repo,
            rebac_svc,
        }
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl CreateDatasetFromSnapshotUseCase for CreateDatasetFromSnapshotUseCaseImpl {
    #[tracing::instrument(level = "info", name = CreateDatasetFromSnapshotUseCaseImpl_execute, skip_all)]
    async fn execute(
        &self,
        mut snapshot: odf::DatasetSnapshot,
        options: CreateDatasetUseCaseOptions,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError> {
        tracing::info!(
            ?snapshot,
            ?options,
            "Initiating creation of dataset from snapshot"
        );

        // There must be a logged-in user
        let subject = match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(subj) => subj,
            CurrentAccountSubject::Anonymous(_) => {
                panic!("Anonymous account cannot create dataset");
            }
        };

        // Validate / resolve metadata events from the snapshot
        odf::dataset::normalize_and_validate_dataset_snapshot(
            self.dataset_registry.as_ref(),
            &mut snapshot,
        )
        .await?;

        // Resolve a target account and full alias of the dataset
        let (canonical_alias, target_account_id) = self
            .create_helper
            .resolve_alias_target(&snapshot.name, subject)
            .await?;

        let dataset_did = self.did_generator.generate_dataset_id();
        // Make a seed block
        let system_time = self.system_time_source.now();
        let seed_block =
            odf::dataset::make_seed_block(dataset_did.0.clone(), snapshot.kind, system_time);

        // Dataset entry goes first, this guarantees name collision check
        self.create_helper
            .create_dataset_entry(
                &seed_block.event.dataset_id,
                &target_account_id,
                canonical_alias.as_ref(),
                snapshot.kind,
            )
            .await?;

        if let Some(did_secret_encryption_key) = &self.did_secret_encryption_key {
            let dataset_did_secret_key =
                DidSecretKey::try_new(&dataset_did.1, did_secret_encryption_key.expose_secret())
                    .int_err()?;

            // Note: subject and target might be two different accounts, but we use subject
            // here as the creator of the key
            self.did_secret_key_repo
                .save_did_secret_key(
                    &DidEntity::new_dataset(dataset_did.0.to_string()),
                    &dataset_did_secret_key,
                )
                .await
                .int_err()?;
        }

        // Make a storage level dataset (no HEAD yet)
        let store_result = self
            .create_helper
            .store_dataset(canonical_alias.as_ref(), seed_block)
            .await?;

        // Append snapshot metadata
        let append_result = odf::dataset::append_snapshot_metadata_to_dataset(
            snapshot.metadata,
            store_result.dataset.as_ref(),
            &store_result.seed,
            system_time,
        )
        .await?;

        // TODO: HACK: SEC: When creating a dataaset under another account we currently
        // give subject a "maintainer" role on it. In future this should be refactored
        // into organization-level permissions.
        //
        // See: https://github.com/kamu-data/kamu-node/issues/233
        if target_account_id != subject.account_id {
            self.rebac_svc
                .get()
                .int_err()?
                .set_account_dataset_relation(
                    &subject.account_id,
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

        // Set initial dataset HEAD
        self.create_helper
            .set_created_head(
                ResolvedDataset::from_stored(&store_result, canonical_alias.as_ref()),
                &append_result.proposed_head,
            )
            .await?;

        Ok(CreateDatasetResult {
            head: append_result.proposed_head,
            dataset: store_result.dataset,
            dataset_handle: odf::DatasetHandle::new(
                store_result.dataset_id,
                canonical_alias.into_inner(),
                store_result.dataset_kind,
            ),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
