// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::{GetDataOptions, QueryError, QueryService, auth};
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, ResolvedDataset};
use kamu_molecule_domain::MoleculeGetDatasetError;
use odf::utils::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct MoleculeDatasetsHelper {}

impl MoleculeDatasetsHelper {
    pub(crate) async fn get_or_create_dataset(
        create_dataset_use_case: &dyn CreateDatasetFromSnapshotUseCase,
        rebac_dataset_registry: &dyn RebacDatasetRegistryFacade,
        dataset_ref: &odf::DatasetRef,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
        snapshot_getter: impl FnOnce() -> odf::DatasetSnapshot,
    ) -> Result<ResolvedDataset, MoleculeGetDatasetError> {
        match rebac_dataset_registry
            .resolve_dataset_by_ref(dataset_ref, action)
            .await
        {
            Ok(ds) => Ok(ds),
            Err(RebacDatasetRefUnresolvedError::NotFound(_)) if create_if_not_exist => {
                let snapshot = snapshot_getter();

                let create_res = create_dataset_use_case
                    .execute(snapshot, Default::default())
                    .await
                    .int_err()?;

                Ok(ResolvedDataset::new(
                    create_res.dataset,
                    create_res.dataset_handle,
                ))
            }
            Err(RebacDatasetRefUnresolvedError::NotFound(e)) => {
                Err(MoleculeGetDatasetError::NotFound(e))
            }
            Err(RebacDatasetRefUnresolvedError::Access(e)) => {
                Err(MoleculeGetDatasetError::Access(e))
            }
            Err(e) => Err(e.int_err().into()),
        }
    }

    pub(crate) async fn try_get_data_frame(
        query_service: &dyn QueryService,
        dataset: ResolvedDataset,
    ) -> Result<Option<DataFrameExt>, MoleculeGetDatasetError> {
        match query_service
            .get_data(dataset, GetDataOptions::default())
            .await
        {
            Ok(res) => Ok(res.df),
            Err(QueryError::Access(e)) => Err(e.into()),
            Err(e) => Err(e.int_err().into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
