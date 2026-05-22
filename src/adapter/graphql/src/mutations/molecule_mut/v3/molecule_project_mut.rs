// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::mutations::molecule_mut::v3::{MoleculeAnnouncementsDatasetMut, MoleculeDataRoomMut};
use crate::prelude::*;
use crate::queries::molecule::v3::MoleculeProject;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeProjectMut {
    pub project: Arc<MoleculeProject>,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeProjectMut {
    /// Strongly typed data room mutator
    #[tracing::instrument(level = "info", name = MoleculeProjectMut_data_room, skip_all)]
    async fn data_room(&self) -> Result<MoleculeDataRoomMut> {
        // Note: access control is enforced in use cases via ReBAC
        Ok(MoleculeDataRoomMut::new(self.project.clone()))
    }

    /// Strongly typed announcements mutator
    #[tracing::instrument(level = "info", name = MoleculeProjectMut_announcements, skip_all)]
    async fn announcements(&self) -> Result<MoleculeAnnouncementsDatasetMut> {
        Ok(MoleculeAnnouncementsDatasetMut::new(self.project.clone()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeProjectMutationResult {
    message: String,
    project: MoleculeProjectMut,
}

impl MoleculeProjectMutationResult {
    pub fn from_entity(entity: kamu_molecule_domain::MoleculeProject, message: String) -> Self {
        Self {
            project: MoleculeProjectMut::from_entity(entity),
            message,
        }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeProjectMutationResult {
    async fn project(&self) -> &MoleculeProjectMut {
        &self.project
    }

    async fn message(&self) -> &str {
        &self.message
    }
}

impl MoleculeProjectMut {
    pub fn from_entity(entity: kamu_molecule_domain::MoleculeProject) -> Self {
        Self {
            project: Arc::new(MoleculeProject::new(entity)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
