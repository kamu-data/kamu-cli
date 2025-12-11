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
    MoleculeAnnouncementsService,
    MoleculeDatasetAccessorFactory,
    MoleculeDatasetReader,
    MoleculeDatasetWriter,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeAnnouncementsService)]
pub struct MoleculeAnnouncementsServiceImpl {
    accessor_factory: Arc<MoleculeDatasetAccessorFactory>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeAnnouncementsService for MoleculeAnnouncementsServiceImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeAnnouncementsServiceImpl_global_reader,
        skip_all,
        fields(molecule_account_name)
    )]
    async fn global_reader(
        &self,
        molecule_account_name: &odf::AccountName,
    ) -> Result<MoleculeDatasetReader, RebacDatasetRefUnresolvedError> {
        let announcements_dataset_alias =
            MoleculeDatasetSnapshots::global_announcements_alias(molecule_account_name.clone());

        self.accessor_factory
            .reader(&announcements_dataset_alias.as_local_ref())
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeAnnouncementsServiceImpl_global_writer,
        skip_all,
        fields(molecule_account_name)
    )]
    async fn global_writer(
        &self,
        molecule_account_name: &odf::AccountName,
        create_if_not_exist: bool,
    ) -> Result<MoleculeDatasetWriter, RebacDatasetRefUnresolvedError> {
        let announcements_dataset_alias =
            MoleculeDatasetSnapshots::global_announcements_alias(molecule_account_name.clone());

        self.accessor_factory
            .writer(
                &announcements_dataset_alias.as_local_ref(),
                create_if_not_exist,
                || MoleculeDatasetSnapshots::global_announcements(molecule_account_name.clone()),
            )
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeAnnouncementsServiceImpl_project_reader,
        skip_all,
        fields(molecule_project_id = %molecule_project.ipnft_uid)
    )]
    async fn project_reader(
        &self,
        molecule_project: &MoleculeProject,
    ) -> Result<MoleculeDatasetReader, RebacDatasetRefUnresolvedError> {
        self.accessor_factory
            .reader(&molecule_project.announcements_dataset_id.as_local_ref())
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeAnnouncementsServiceImpl_project_writer,
        skip_all,
        fields(molecule_project_id = %molecule_project.ipnft_uid)
    )]
    async fn project_writer(
        &self,
        molecule_project: &MoleculeProject,
    ) -> Result<MoleculeDatasetWriter, RebacDatasetRefUnresolvedError> {
        self.accessor_factory
            .writer(
                &molecule_project.announcements_dataset_id.as_local_ref(),
                false,
                || unreachable!("Project announcements dataset should exist already"),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
