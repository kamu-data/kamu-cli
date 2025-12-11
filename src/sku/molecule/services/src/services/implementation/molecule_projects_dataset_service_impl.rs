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
    MoleculeProjectsDatasetReader,
    MoleculeProjectsDatasetService,
    MoleculeProjectsDatasetWriter,
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
        name = MoleculeProjectsDatasetServiceImpl_reader,
        skip_all,
        fields(molecule_account_name)
    )]
    async fn reader(
        &self,
        molecule_account_name: &odf::AccountName,
    ) -> Result<Arc<dyn MoleculeProjectsDatasetReader>, RebacDatasetRefUnresolvedError> {
        let projects_dataset_alias =
            MoleculeDatasetSnapshots::projects_alias(molecule_account_name.clone());

        let read_accessor = self
            .accessor_factory
            .read_accessor(&projects_dataset_alias.as_local_ref())
            .await?;

        Ok(Arc::new(MoleculeProjectsDatasetReadAccessorImpl(
            read_accessor,
        )))
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectsDatasetServiceImpl_writer,
        skip_all,
        fields(molecule_account_name, create_if_not_exist)
    )]
    async fn writer(
        &self,
        molecule_account_name: &odf::AccountName,
        create_if_not_exist: bool,
    ) -> Result<Arc<dyn MoleculeProjectsDatasetWriter>, RebacDatasetRefUnresolvedError> {
        let projects_dataset_alias =
            MoleculeDatasetSnapshots::projects_alias(molecule_account_name.clone());

        let write_accessor = self
            .accessor_factory
            .write_accessor(
                &projects_dataset_alias.as_local_ref(),
                create_if_not_exist,
                || MoleculeDatasetSnapshots::projects(molecule_account_name.clone()),
            )
            .await?;

        Ok(Arc::new(MoleculeProjectsDatasetWriteAccessorImpl(
            write_accessor,
        )))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MoleculeProjectsDatasetReadAccessorImpl(MoleculeDatasetReadAccessor);

impl MoleculeProjectsDatasetReader for MoleculeProjectsDatasetReadAccessorImpl {
    fn raw_read_accessor(&self) -> &MoleculeDatasetReadAccessor {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MoleculeProjectsDatasetWriteAccessorImpl(MoleculeDatasetWriteAccessor);

impl MoleculeProjectsDatasetWriter for MoleculeProjectsDatasetWriteAccessorImpl {
    fn raw_write_accessor(&self) -> &MoleculeDatasetWriteAccessor {
        &self.0
    }

    fn as_reader(&self) -> Arc<dyn MoleculeProjectsDatasetReader> {
        Arc::new(MoleculeProjectsDatasetReadAccessorImpl(
            self.0.as_read_accessor(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
