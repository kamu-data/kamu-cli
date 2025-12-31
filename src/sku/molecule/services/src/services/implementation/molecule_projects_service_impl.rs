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
    MoleculeDatasetReader,
    MoleculeDatasetWriter,
    MoleculeProjectsReader,
    MoleculeProjectsService,
    MoleculeProjectsWriter,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeProjectsService)]
pub struct MoleculeProjectsServiceImpl {
    accessor_factory: Arc<MoleculeDatasetAccessorFactory>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeProjectsService for MoleculeProjectsServiceImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectsServiceImpl_reader,
        skip_all,
        fields(molecule_account_name)
    )]
    async fn reader(
        &self,
        molecule_account_name: &odf::AccountName,
    ) -> Result<Arc<dyn MoleculeProjectsReader>, RebacDatasetRefUnresolvedError> {
        let projects_dataset_alias =
            MoleculeDatasetSnapshots::projects_alias(molecule_account_name.clone());

        let reader = self
            .accessor_factory
            .reader(&projects_dataset_alias.as_local_ref())
            .await?;

        Ok(Arc::new(MoleculeProjectsReaderImpl(reader)))
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectsServiceImpl_writer,
        skip_all,
        fields(molecule_account_name, create_if_not_exist)
    )]
    async fn writer(
        &self,
        molecule_account_name: &odf::AccountName,
        create_if_not_exist: bool,
    ) -> Result<Arc<dyn MoleculeProjectsWriter>, RebacDatasetRefUnresolvedError> {
        let projects_dataset_alias =
            MoleculeDatasetSnapshots::projects_alias(molecule_account_name.clone());

        let writer = self
            .accessor_factory
            .writer(
                &projects_dataset_alias.as_local_ref(),
                create_if_not_exist,
                || MoleculeDatasetSnapshots::projects(molecule_account_name.clone()),
            )
            .await?;

        Ok(Arc::new(MoleculeProjectsWriterImpl(writer)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MoleculeProjectsReaderImpl(MoleculeDatasetReader);

impl MoleculeProjectsReader for MoleculeProjectsReaderImpl {
    fn raw_reader(&self) -> &MoleculeDatasetReader {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MoleculeProjectsWriterImpl(MoleculeDatasetWriter);

impl MoleculeProjectsWriter for MoleculeProjectsWriterImpl {
    fn raw_writer(&self) -> &MoleculeDatasetWriter {
        &self.0
    }

    fn as_reader(&self) -> Arc<dyn MoleculeProjectsReader> {
        Arc::new(MoleculeProjectsReaderImpl(self.0.as_reader()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
