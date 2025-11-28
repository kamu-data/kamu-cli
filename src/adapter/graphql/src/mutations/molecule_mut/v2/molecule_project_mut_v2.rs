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
use kamu_core::auth;

use crate::mutations::molecule_mut::v2::{
    MoleculeAnnouncementsDatasetMutV2,
    MoleculeDataRoomMutV2,
};
use crate::prelude::*;
use crate::queries::DatasetRequestState;
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
    async fn data_room(&self, ctx: &Context<'_>) -> Result<MoleculeDataRoomMutV2> {
        let rebac_dataset_registry_facade =
            from_catalog_n!(ctx, dyn kamu_auth_rebac::RebacDatasetRegistryFacade);

        let data_room_handle = rebac_dataset_registry_facade
            .resolve_dataset_handle_by_ref(
                &self.project.entity.data_room_dataset_id.as_local_ref(),
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
        let data_room_writable_state = DatasetRequestState::new(data_room_handle);

        Ok(MoleculeDataRoomMutV2::new(
            self.project.clone(),
            data_room_writable_state,
        ))
    }

    /// Strongly typed announcements mutator
    #[expect(clippy::unused_async)]
    async fn announcements(&self, _ctx: &Context<'_>) -> Result<MoleculeAnnouncementsDatasetMutV2> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeProjectMutationResultV2 {
    message: String,
    project: MoleculeProjectMutV2,
}

impl MoleculeProjectMutationResultV2 {
    pub fn from_entity(
        entity: kamu_molecule_domain::MoleculeProjectEntity,
        message: String,
    ) -> Self {
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
    pub fn from_entity(entity: kamu_molecule_domain::MoleculeProjectEntity) -> Self {
        Self {
            project: Arc::new(MoleculeProjectV2::new(entity)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
