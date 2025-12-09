// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_accounts::{AccountService, LoggedAccount};
use kamu_auth_rebac::RebacService;
use kamu_datasets::{
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetUseCaseOptions,
    ResolvedDataset,
};
use kamu_molecule_domain::{
    MoleculeCreateVersionedFileDatasetError,
    MoleculeCreateVersionedFileDatasetUseCase,
    MoleculeDatasetSnapshots,
    MoleculeProject,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeCreateVersionedFileDatasetUseCase)]
pub struct MoleculeCreateVersionedFileDatasetUseCaseImpl {
    create_dataset_from_snapshot_uc: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    rebac_service: Arc<dyn RebacService>,
    account_service: Arc<dyn AccountService>,
}

impl MoleculeCreateVersionedFileDatasetUseCaseImpl {
    async fn build_new_file_dataset_alias(
        &self,
        molecule_project: &MoleculeProject,
        file_path: &kamu_datasets::CollectionPathV2,
    ) -> odf::DatasetAlias {
        // TODO: PERF: Add AccountRequestState similar to DatasetRequestState and reuse
        //             possibly resolved account?
        let project_account = self
            .account_service
            .get_account_by_id(&molecule_project.account_id)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to load project account [{}]: {e}",
                    molecule_project.account_id
                )
            });
        let project_account_name = project_account.account_name.clone();
        let new_file_name =
            kamu_datasets_services::utils::DatasetNameGenerator::based_on_collection_path(
                file_path,
            );

        odf::DatasetAlias::new(Some(project_account_name), new_file_name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeCreateVersionedFileDatasetUseCase for MoleculeCreateVersionedFileDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeCreateVersionedFileDatasetUseCaseImpl_execute,
        skip_all,
    )]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        molecule_project: &MoleculeProject,
        path: kamu_datasets::CollectionPathV2,
    ) -> Result<ResolvedDataset, MoleculeCreateVersionedFileDatasetError> {
        let alias = self
            .build_new_file_dataset_alias(molecule_project, &path)
            .await;

        let versioned_file_snapshot = MoleculeDatasetSnapshots::versioned_file_v2(alias);

        let (versioned_file_dataset, _) = {
            let create_versioned_file_res = self
                .create_dataset_from_snapshot_uc
                .execute(
                    versioned_file_snapshot,
                    CreateDatasetUseCaseOptions::default(),
                )
                .await
                .int_err()?;

            (
                ResolvedDataset::from_created(&create_versioned_file_res),
                create_versioned_file_res.head,
            )
        };

        // Give maintainer permissions to molecule
        self.rebac_service
            .set_account_dataset_relation(
                &molecule_subject.account_id,
                kamu_auth_rebac::AccountToDatasetRelation::Maintainer,
                versioned_file_dataset.get_id(),
            )
            .await
            .int_err()?;

        Ok(versioned_file_dataset)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
