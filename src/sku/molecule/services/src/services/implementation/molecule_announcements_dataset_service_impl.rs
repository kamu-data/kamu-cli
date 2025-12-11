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
    MoleculeAnnouncementsDatasetService,
    MoleculeDatasetAccessorFactory,
    MoleculeDatasetReadAccessor,
    MoleculeDatasetWriteAccessor,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeAnnouncementsDatasetService)]
pub struct MoleculeAnnouncementsDatasetServiceImpl {
    accessor_factory: Arc<MoleculeDatasetAccessorFactory>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeAnnouncementsDatasetService for MoleculeAnnouncementsDatasetServiceImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeAnnouncementsDatasetServiceImpl_request_read_of_global_announcements_dataset,
        skip_all,
        fields(molecule_account_name)
    )]
    async fn request_read_of_global_announcements_dataset(
        &self,
        molecule_account_name: &odf::AccountName,
    ) -> Result<MoleculeDatasetReadAccessor, RebacDatasetRefUnresolvedError> {
        let announcements_dataset_alias =
            MoleculeDatasetSnapshots::global_announcements_alias(molecule_account_name.clone());

        self.accessor_factory
            .read_accessor(&announcements_dataset_alias.as_local_ref())
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeAnnouncementsDatasetServiceImpl_request_write_of_global_announcements_dataset,
        skip_all,
        fields(molecule_account_name)
    )]
    async fn request_write_of_global_announcements_dataset(
        &self,
        molecule_account_name: &odf::AccountName,
        create_if_not_exist: bool,
    ) -> Result<MoleculeDatasetWriteAccessor, RebacDatasetRefUnresolvedError> {
        let announcements_dataset_alias =
            MoleculeDatasetSnapshots::global_announcements_alias(molecule_account_name.clone());

        self.accessor_factory
            .write_accessor(
                &announcements_dataset_alias.as_local_ref(),
                create_if_not_exist,
                || MoleculeDatasetSnapshots::global_announcements(molecule_account_name.clone()),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
