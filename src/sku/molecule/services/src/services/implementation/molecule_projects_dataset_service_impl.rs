// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_auth_rebac::RebacDatasetRefUnresolvedError;
use kamu_molecule_domain::*;

use crate::{
    MoleculeDatasetAccessorFactory,
    MoleculeDatasetReadAccessor,
    MoleculeDatasetWriteAccessor,
    MoleculeProjectsDatasetService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeProjectsDatasetService)]
pub struct MoleculeProjectsDatasetServiceImpl {
    accessor_factory: Arc<MoleculeDatasetAccessorFactory>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeProjectsDatasetService for MoleculeProjectsDatasetServiceImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectsDatasetServiceImpl_request_read_of_projects_dataset,
        skip_all,
        fields(molecule_account_name)
    )]
    async fn request_read_of_projects_dataset(
        &self,
        molecule_account_name: &odf::AccountName,
    ) -> Result<MoleculeDatasetReadAccessor, RebacDatasetRefUnresolvedError> {
        let projects_dataset_alias =
            MoleculeDatasetSnapshots::projects_alias(molecule_account_name.clone());

        self.accessor_factory
            .read_accessor(&projects_dataset_alias.as_local_ref())
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectsDatasetServiceImpl_request_write_of_projects_dataset,
        skip_all,
        fields(molecule_account_name, create_if_not_exist)
    )]
    async fn request_write_of_projects_dataset(
        &self,
        molecule_account_name: &odf::AccountName,
        create_if_not_exist: bool,
    ) -> Result<MoleculeDatasetWriteAccessor, RebacDatasetRefUnresolvedError> {
        let projects_dataset_alias =
            MoleculeDatasetSnapshots::projects_alias(molecule_account_name.clone());

        self.accessor_factory
            .write_accessor(
                &projects_dataset_alias.as_local_ref(),
                create_if_not_exist,
                || MoleculeDatasetSnapshots::projects(molecule_account_name.clone()),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
