// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_auth_rebac::RebacDatasetRefUnresolvedError;
use kamu_molecule_domain::MoleculeProject;

use crate::{MoleculeDatasetReader, MoleculeDatasetWriter};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeAnnouncementsService: Send + Sync {
    async fn global_reader(
        &self,
        molecule_account_name: &odf::AccountName,
    ) -> Result<MoleculeDatasetReader, RebacDatasetRefUnresolvedError>;

    async fn global_writer(
        &self,
        molecule_account_name: &odf::AccountName,
        create_if_not_exist: bool,
    ) -> Result<MoleculeDatasetWriter, RebacDatasetRefUnresolvedError>;

    async fn project_reader(
        &self,
        molecule_project: &MoleculeProject,
    ) -> Result<MoleculeDatasetReader, RebacDatasetRefUnresolvedError>;

    async fn project_writer(
        &self,
        molecule_project: &MoleculeProject,
    ) -> Result<MoleculeDatasetWriter, RebacDatasetRefUnresolvedError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
