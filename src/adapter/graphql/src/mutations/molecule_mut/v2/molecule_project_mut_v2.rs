// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::molecule_mut::v2::{
    MoleculeAnnouncementsDatasetMutV2,
    MoleculeDataRoomMutV2,
};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeProjectMutV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeProjectMutV2 {
    #[expect(clippy::unused_async)]
    /// Strongly typed data room mutator
    async fn data_room(&self, _ctx: &Context<'_>) -> Result<MoleculeDataRoomMutV2> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    /// Strongly typed announcements mutator
    async fn announcements(&self, _ctx: &Context<'_>) -> Result<MoleculeAnnouncementsDatasetMutV2> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
