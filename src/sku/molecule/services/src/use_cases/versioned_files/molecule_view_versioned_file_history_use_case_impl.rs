// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::ErrorIntoInternal;
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_datasets::{
    DatasetAction,
    ReadCheckedDataset,
    ViewVersionedFileHistoryError,
    ViewVersionedFileHistoryUseCase,
};
use kamu_molecule_domain::{
    MoleculeVersionedFileEntry,
    MoleculeVersionedFileHistoryPage,
    MoleculeViewVersionedFileHistoryError,
    MoleculeViewVersionedFileHistoryUseCase,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewVersionedFileHistoryUseCase)]
pub struct MoleculeViewVersionedFileHistoryUseCaseImpl {
    view_versioned_file_history_uc: Arc<dyn ViewVersionedFileHistoryUseCase>,
    rebac_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

impl MoleculeViewVersionedFileHistoryUseCaseImpl {
    async fn readable_versioned_file_dataset(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
    ) -> Result<ReadCheckedDataset<'static>, MoleculeViewVersionedFileHistoryError> {
        let readable_dataset = self
            .rebac_registry_facade
            .resolve_dataset_by_ref(
                &versioned_file_dataset_id.as_local_ref(),
                DatasetAction::Read,
            )
            .await
            .map_err(|e| match e {
                RebacDatasetRefUnresolvedError::NotFound(e) => e.int_err().into(),
                RebacDatasetRefUnresolvedError::Access(e) => {
                    MoleculeViewVersionedFileHistoryError::Access(e)
                }
                e @ RebacDatasetRefUnresolvedError::Internal(_) => e.int_err().into(),
            })?;

        Ok(ReadCheckedDataset::from_owned(readable_dataset))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeViewVersionedFileHistoryUseCase for MoleculeViewVersionedFileHistoryUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeViewVersionedFileHistoryUseCaseImpl_execute,
        skip_all,
        fields(%versioned_file_dataset_id, ?max_version, ?pagination)
    )]
    async fn execute(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
        max_version: Option<kamu_datasets::FileVersion>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeVersionedFileHistoryPage, MoleculeViewVersionedFileHistoryError> {
        let versioned_file_dataset = self
            .readable_versioned_file_dataset(versioned_file_dataset_id)
            .await?;

        let history_page = self
            .view_versioned_file_history_uc
            .execute(versioned_file_dataset, max_version, pagination)
            .await
            .map_err(|e| match e {
                ViewVersionedFileHistoryError::Internal(e) => {
                    MoleculeViewVersionedFileHistoryError::Internal(e)
                }
            })?;

        Ok(MoleculeVersionedFileHistoryPage {
            list: history_page
                .list
                .into_iter()
                .map(MoleculeVersionedFileEntry::from_raw_versioned_file_entry)
                .collect(),
            total_count: history_page.total_count,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
