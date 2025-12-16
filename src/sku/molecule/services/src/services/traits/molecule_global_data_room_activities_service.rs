// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_auth_rebac::RebacDatasetRefUnresolvedError;

use crate::{MoleculeDatasetReader, MoleculeDatasetWriter};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeGlobalDataRoomActivitiesService: Send + Sync {
    async fn reader(
        &self,
        molecule_account_name: &odf::AccountName,
    ) -> Result<MoleculeDatasetReader, RebacDatasetRefUnresolvedError>;

    async fn writer(
        &self,
        molecule_account_name: &odf::AccountName,
        create_if_not_exist: bool,
    ) -> Result<MoleculeDatasetWriter, RebacDatasetRefUnresolvedError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
