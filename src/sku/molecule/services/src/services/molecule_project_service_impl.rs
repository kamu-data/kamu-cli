// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_accounts::LoggedAccount;
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::auth::DatasetAction;
use kamu_core::{GetDataOptions, QueryError, QueryService};
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, ResolvedDataset};
use odf::utils::data::DataFrameExt;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeProjectService)]
pub struct MoleculeProjectServiceImpl {
    rebac_dataset_registry: Arc<dyn RebacDatasetRegistryFacade>,
    create_dataset_use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    query_service: Arc<dyn QueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeProjectService for MoleculeProjectServiceImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectServiceImpl_get_projects_dataset,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist)
    )]
    async fn get_projects_dataset(
        &self,
        molecule_account_name: &odf::AccountName,
        action: DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<ResolvedDataset, MoleculeGetProjectsError> {
        const PROJECTS_DATASET_NAME: &str = "projects";

        let projects_dataset_alias = odf::DatasetAlias::new(
            Some(molecule_account_name.clone()),
            odf::DatasetName::new_unchecked(PROJECTS_DATASET_NAME),
        );

        match self
            .rebac_dataset_registry
            .resolve_dataset_by_ref(&projects_dataset_alias.as_local_ref(), action)
            .await
        {
            Ok(ds) => Ok(ds),
            Err(RebacDatasetRefUnresolvedError::NotFound(_)) if create_if_not_exist => {
                let snapshot = MoleculeDatasetSnapshots::projects(odf::DatasetAlias::new(
                    None,
                    odf::DatasetName::new_unchecked(PROJECTS_DATASET_NAME),
                ));

                let create_res = self
                    .create_dataset_use_case
                    .execute(
                        snapshot,
                        kamu_datasets::CreateDatasetUseCaseOptions {
                            dataset_visibility: odf::DatasetVisibility::Private,
                        },
                    )
                    .await
                    .int_err()?;

                // TODO: Use case should return ResolvedDataset directly
                Ok(ResolvedDataset::new(
                    create_res.dataset,
                    create_res.dataset_handle,
                ))
            }
            Err(RebacDatasetRefUnresolvedError::NotFound(err)) => {
                Err(MoleculeGetProjectsError::NotFound(err))
            }
            Err(RebacDatasetRefUnresolvedError::Access(err)) => {
                Err(MoleculeGetProjectsError::Access(err))
            }
            Err(err) => Err(err.int_err().into()),
        }
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectServiceImpl_get_projects_ledger_data_frame,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist)
    )]
    async fn get_projects_ledger_data_frame(
        &self,
        molecule_subject: &LoggedAccount,
        action: DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetProjectsError> {
        // Resolve projects dataset
        let projects_dataset = self
            .get_projects_dataset(&molecule_subject.account_name, action, create_if_not_exist)
            .await?;

        // Query full data
        let df = match self
            .query_service
            .get_data(projects_dataset.clone(), GetDataOptions::default())
            .await
        {
            Ok(res) => Ok(res.df),
            Err(QueryError::Access(err)) => Err(MoleculeGetProjectsError::Access(err)),
            Err(err) => Err(MoleculeGetProjectsError::Internal(err.int_err())),
        }?;

        Ok((projects_dataset, df))
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectServiceImpl_get_projects_data_frame,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist)
    )]
    async fn get_projects_data_frame(
        &self,
        molecule_subject: &LoggedAccount,
        action: DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetProjectsError> {
        let (projects_dataset, df) = self
            .get_projects_ledger_data_frame(molecule_subject, action, create_if_not_exist)
            .await?;

        let df = if let Some(df) = df {
            Some(
                odf::utils::data::changelog::project(
                    df,
                    &["account_id".to_string()],
                    &odf::metadata::DatasetVocabulary::default(),
                )
                .int_err()?,
            )
        } else {
            None
        };

        Ok((projects_dataset, df))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
