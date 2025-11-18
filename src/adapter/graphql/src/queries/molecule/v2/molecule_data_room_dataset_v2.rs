// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::Dataset;
use crate::queries::molecule::v2::{MoleculeDataRoomEntryV2, MoleculeDataRoomEntryV2Connection};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomDatasetV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomDatasetV2 {
    #[expect(clippy::unused_async)]
    /// Access the underlying core Dataset
    async fn dataset(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn entries(
        &self,
        _ctx: &Context<'_>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeDataRoomEntryV2Connection> {
        let _ = path_prefix;
        let _ = max_depth;
        let _ = page;
        let _ = per_page;

        // TODO: implement
        Ok(MoleculeDataRoomEntryV2Connection::new(vec![], 0, 0, 0))
    }

    #[expect(clippy::unused_async)]
    async fn entry(
        &self,
        _ctx: &Context<'_>,
        path: CollectionPath,
    ) -> Result<MoleculeDataRoomEntryV2> {
        let _ = path;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
