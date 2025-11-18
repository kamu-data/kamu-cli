// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::molecule_mut::v1;
use crate::prelude::*;
use crate::queries::molecule::v2::MoleculeAccessLevelV2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeAnnouncementsDatasetMutV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeAnnouncementsDatasetMutV2 {
    /// Creates an announcement record for the project.
    #[tracing::instrument(level = "info", name = MoleculeAnnouncementsDatasetMutV2_create, skip_all)]
    async fn create(
        &self,
        _ctx: &Context<'_>,
        headline: String,
        body: String,
        #[graphql(desc = "List of dataset DIDs to link")] attachments: Option<Vec<String>>,
        molecule_access_level: MoleculeAccessLevelV2,
        molecule_change_by: String,
        categories: Vec<String>,
        tags: Vec<String>,
    ) -> Result<v1::CreateAnnouncementResult> {
        let _ = headline;
        let _ = body;
        let _ = attachments;
        let _ = molecule_access_level;
        let _ = molecule_change_by;
        let _ = categories;
        let _ = tags;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
