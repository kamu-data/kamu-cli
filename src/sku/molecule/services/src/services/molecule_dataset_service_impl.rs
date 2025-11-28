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
use kamu_core::{GetDataOptions, QueryError, QueryService, auth};
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, ResolvedDataset};
use odf::utils::data::DataFrameExt;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeDatasetService)]
pub struct MoleculeDatasetServiceImpl {
    rebac_dataset_registry: Arc<dyn RebacDatasetRegistryFacade>,
    create_dataset_use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    query_service: Arc<dyn QueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeDatasetService for MoleculeDatasetServiceImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeDatasetServiceImpl_get_projects_dataset,
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

        match self
            .rebac_dataset_registry
            .resolve_dataset_by_ref(&projects_dataset_alias.as_local_ref(), action)
            .await
        {
            Ok(ds) => Ok(ds),
            Err(RebacDatasetRefUnresolvedError::NotFound(_)) if create_if_not_exist => {
                let snapshot = MoleculeDatasetSnapshots::projects(molecule_account_name.clone());

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
                Err(MoleculeGetDatasetError::NotFound(err))
            }
            Err(RebacDatasetRefUnresolvedError::Access(err)) => {
                Err(MoleculeGetDatasetError::Access(err))
            }
            Err(err) => Err(err.int_err().into()),
        }
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeDatasetServiceImpl_get_projects_raw_ledger_data_frame,
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
        name = MoleculeDatasetServiceImpl_get_projects_changelog_data_frame,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist)
    )]
    async fn get_projects_changelog_data_frame(
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
        let df = match self
            .query_service
            .get_data(projects_dataset.clone(), GetDataOptions::default())
            .await
        {
            Ok(res) => Ok(res.df),
            Err(QueryError::Access(err)) => Err(MoleculeGetDatasetError::Access(err)),
            Err(err) => Err(MoleculeGetDatasetError::Internal(err.int_err())),
        }?;

        // Project into snapshot
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

    async fn get_project_changelog_entry(
        &self,
        molecule_subject: &LoggedAccount,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
        ipnft_uid: &str,
    ) -> Result<(ResolvedDataset, Option<MoleculeProjectEntity>), MoleculeGetDatasetError> {
        use datafusion::prelude::*;

        let (projects_dataset, df_opt) = self
            .get_projects_changelog_data_frame(molecule_subject, action, create_if_not_exist)
            .await?;

        let project = if let Some(df) = df_opt {
            let df = df.filter(col("ipnft_uid").eq(lit(ipnft_uid))).int_err()?;
            let records = df.collect_json_aos().await.int_err()?;
            if let Some(record) = records.into_iter().next() {
                Some(MoleculeProjectEntity::from_json(record).int_err()?)
            } else {
                None
            }
        } else {
            None
        };

        Ok((projects_dataset, project))
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeDatasetServiceImpl_get_global_data_room_activity_dataset,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist)
    )]
    async fn get_global_data_room_activity_dataset(
        &self,
        molecule_account_name: &odf::AccountName,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<ResolvedDataset, MoleculeGetDatasetError> {
        let data_room_activity_dataset_alias =
            MoleculeDatasetSnapshots::global_data_room_activity_alias(
                molecule_account_name.clone(),
            );

        match self
            .rebac_dataset_registry
            .resolve_dataset_by_ref(&data_room_activity_dataset_alias.as_local_ref(), action)
            .await
        {
            Ok(ds) => Ok(ds),
            Err(RebacDatasetRefUnresolvedError::NotFound(_)) if create_if_not_exist => {
                let snapshot = MoleculeDatasetSnapshots::global_data_room_activity(
                    molecule_account_name.clone(),
                );

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
                Err(MoleculeGetDatasetError::NotFound(err))
            }
            Err(RebacDatasetRefUnresolvedError::Access(err)) => {
                Err(MoleculeGetDatasetError::Access(err))
            }
            Err(err) => Err(err.int_err().into()),
        }
    }

    async fn get_global_data_room_activity_data_frame(
        &self,
        molecule_account_name: &odf::AccountName,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetDatasetError> {
        let data_room_activity_dataset = self
            .get_global_data_room_activity_dataset(
                molecule_account_name,
                action,
                create_if_not_exist,
            )
            .await?;

        let df = match self
            .query_service
            .get_data(
                data_room_activity_dataset.clone(),
                GetDataOptions::default(),
            )
            .await
        {
            Ok(res) => Ok(res.df),
            Err(QueryError::Access(err)) => Err(MoleculeGetDatasetError::Access(err)),
            Err(err) => Err(MoleculeGetDatasetError::Internal(err.int_err())),
        }?;

        Ok((data_room_activity_dataset, df))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
