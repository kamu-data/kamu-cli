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
    MoleculeGlobalActivitiesService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeGlobalActivitiesService)]
pub struct MoleculeGlobalActivitiesServiceImpl {
    accessor_factory: Arc<MoleculeDatasetAccessorFactory>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeGlobalActivitiesService for MoleculeGlobalActivitiesServiceImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeGlobalActivitiesServiceImpl_reader,
        skip_all,
        fields(molecule_account_name)
    )]
    async fn reader(
        &self,
        molecule_account_name: &odf::AccountName,
    ) -> Result<MoleculeDatasetReader, RebacDatasetRefUnresolvedError> {
        let activity_dataset_alias = MoleculeDatasetSnapshots::global_data_room_activity_alias(
            molecule_account_name.clone(),
        );

        self.accessor_factory
            .reader(&activity_dataset_alias.as_local_ref())
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeGlobalActivitiesServiceImpl_writer,
        skip_all,
        fields(molecule_account_name, create_if_not_exist)
    )]
    async fn writer(
        &self,
        molecule_account_name: &odf::AccountName,
        create_if_not_exist: bool,
    ) -> Result<MoleculeDatasetWriter, RebacDatasetRefUnresolvedError> {
        let activity_dataset_alias = MoleculeDatasetSnapshots::global_data_room_activity_alias(
            molecule_account_name.clone(),
        );

        self.accessor_factory
            .writer(
                &activity_dataset_alias.as_local_ref(),
                create_if_not_exist,
                || {
                    MoleculeDatasetSnapshots::global_data_room_activity(
                        molecule_account_name.clone(),
                    )
                },
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
