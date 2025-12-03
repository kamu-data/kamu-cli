// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::mutations::molecule_mut::v2::{
    MoleculeAnnouncementsDatasetMutV2,
    MoleculeDataRoomMutV2,
};
use crate::prelude::*;
use crate::queries::molecule::v2::MoleculeProjectV2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeProjectMutV2 {
    pub project: Arc<MoleculeProjectV2>,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeProjectMutV2 {
    /// Strongly typed data room mutator
    #[tracing::instrument(level = "info", name = MoleculeProjectMutV2_data_room, skip_all)]
    async fn data_room(&self) -> Result<MoleculeDataRoomMutV2> {
        // Note: access control is enforced in use cases via ReBAC
        Ok(MoleculeDataRoomMutV2::new(self.project.clone()))
    }

    /// Strongly typed announcements mutator
    async fn announcements(&self, ctx: &Context<'_>) -> Result<MoleculeAnnouncementsDatasetMutV2> {
        // TODO: !!!: update
        // TODO: PERF: GQL: DataLoader
        let rebac_dataset_registry_facade =
            from_catalog_n!(ctx, dyn kamu_auth_rebac::RebacDatasetRegistryFacade);

        let announcements_handle = rebac_dataset_registry_facade
            .resolve_dataset_handle_by_ref(
                &self.project.entity.announcements_dataset_id.as_local_ref(),
                auth::DatasetAction::Write,
            )
            .await
            .map_err(|e| -> GqlError {
                use RebacDatasetRefUnresolvedError as E;

                match e {
                    E::Access(e) => e.into(),
                    E::NotFound(_) | E::Internal(_) => e.int_err().into(),
                }
            })?;
        let data_room_writable_state = DatasetRequestState::new(announcements_handle);

        Ok(MoleculeAnnouncementsDatasetMutV2::new(
            data_room_writable_state,
            self.project.as_ref(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeProjectMutationResultV2 {
    message: String,
    project: MoleculeProjectMutV2,
}

impl MoleculeProjectMutationResultV2 {
    pub fn from_entity(entity: kamu_molecule_domain::MoleculeProject, message: String) -> Self {
        Self {
            project: MoleculeProjectMutV2::from_entity(entity),
            message,
        }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeProjectMutationResultV2 {
    async fn project(&self) -> &MoleculeProjectMutV2 {
        &self.project
    }

    async fn message(&self) -> &str {
        &self.message
    }
}

impl MoleculeProjectMutV2 {
    pub fn from_entity(entity: kamu_molecule_domain::MoleculeProject) -> Self {
        Self {
            project: Arc::new(MoleculeProjectV2::new(entity)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
