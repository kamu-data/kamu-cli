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
use kamu_auth_rebac::RebacDatasetRegistryFacade;
use kamu_core::{GetDataOptions, QueryError, QueryService, auth};
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, ResolvedDataset};
use odf::utils::data::DataFrameExt;

use super::molecule_datasets_helper::MoleculeDatasetsHelper;
use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeProjectsDatasetService)]
pub struct MoleculeProjectsDatasetServiceImpl {
    rebac_dataset_registry: Arc<dyn RebacDatasetRegistryFacade>,
    create_dataset_use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    query_service: Arc<dyn QueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeProjectsDatasetService for MoleculeProjectsDatasetServiceImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectsDatasetServiceImpl_get_projects_dataset,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist)
    )]
    async fn get_projects_dataset(
        &self,
        molecule_account_name: &odf::AccountName,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<ResolvedDataset, MoleculeGetDatasetError> {
        let projects_dataset_alias =
            MoleculeDatasetSnapshots::projects_alias(molecule_account_name.clone());

        MoleculeDatasetsHelper::get_or_create_dataset(
            self.create_dataset_use_case.as_ref(),
            self.rebac_dataset_registry.as_ref(),
            &projects_dataset_alias.as_local_ref(),
            action,
            create_if_not_exist,
            || MoleculeDatasetSnapshots::projects(molecule_account_name.clone()),
        )
        .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectsDatasetServiceImpl_get_projects_raw_ledger_data_frame,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist)
    )]
    async fn get_projects_raw_ledger_data_frame(
        &self,
        molecule_subject: &LoggedAccount,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetDatasetError> {
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
            Err(QueryError::Access(err)) => Err(MoleculeGetDatasetError::Access(err)),
            Err(err) => Err(MoleculeGetDatasetError::Internal(err.int_err())),
        }?;

        Ok((projects_dataset, df))
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectsDatasetServiceImpl_get_projects_changelog_projection_data_frame,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist)
    )]
    async fn get_projects_changelog_projection_data_frame(
        &self,
        molecule_subject: &LoggedAccount,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetDatasetError> {
        // Resolve projects dataset
        let projects_dataset = self
            .get_projects_dataset(&molecule_subject.account_name, action, create_if_not_exist)
            .await?;

        // Query full data
        let maybe_df = MoleculeDatasetsHelper::try_get_data_frame(
            self.query_service.as_ref(),
            projects_dataset.clone(),
        )
        .await?;

        // Project into snapshot
        let df = if let Some(df) = maybe_df {
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

    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectsDatasetServiceImpl_get_project_changelog_entry,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist, %ipnft_uid)
    )]
    async fn get_project_changelog_entry(
        &self,
        molecule_subject: &LoggedAccount,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
        ipnft_uid: &str,
    ) -> Result<(ResolvedDataset, Option<MoleculeProject>), MoleculeGetDatasetError> {
        use datafusion::prelude::*;

        let (projects_dataset, df_opt) = self
            .get_projects_changelog_projection_data_frame(
                molecule_subject,
                action,
                create_if_not_exist,
            )
            .await?;

        let Some(df) = df_opt else {
            return Ok((projects_dataset, None));
        };

        let df = df.filter(col("ipnft_uid").eq(lit(ipnft_uid))).int_err()?;
        let records = df.collect_json_aos().await.int_err()?;
        let project = records
            .into_iter()
            .next()
            .map(MoleculeProject::from_json)
            .transpose()?;

        Ok((projects_dataset, project))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
