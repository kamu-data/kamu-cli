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
use kamu_core::QueryError;
use odf::utils::data::DataFrameExt;

use crate::{MoleculeDatasetReader, MoleculeDatasetWriter};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeProjectsService: Send + Sync {
    async fn reader(
        &self,
        molecule_account_name: &odf::AccountName,
    ) -> Result<Arc<dyn MoleculeProjectsReader>, RebacDatasetRefUnresolvedError>;

    async fn writer(
        &self,
        molecule_account_name: &odf::AccountName,
        create_if_not_exist: bool,
    ) -> Result<Arc<dyn MoleculeProjectsWriter>, RebacDatasetRefUnresolvedError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeProjectsReader: Send + Sync {
    fn raw_reader(&self) -> &MoleculeDatasetReader;

    async fn changelog_projection_data_frame(&self) -> Result<Option<DataFrameExt>, QueryError> {
        self.raw_reader()
            .changelog_projection_data_frame_by("account_id")
            .await
    }

    async fn changelog_entry_by_ipnft_uid(
        &self,
        ipnft_uid: &str,
    ) -> Result<Option<serde_json::Value>, QueryError> {
        self.raw_reader()
            .changelog_projection_entry_by("account_id", "ipnft_uid", ipnft_uid)
            .await
    }
}

impl std::ops::Deref for dyn MoleculeProjectsReader {
    type Target = MoleculeDatasetReader;

    fn deref(&self) -> &Self::Target {
        self.raw_reader()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeProjectsWriter: Send + Sync {
    fn raw_writer(&self) -> &MoleculeDatasetWriter;

    fn as_reader(&self) -> Arc<dyn MoleculeProjectsReader>;
}

impl std::ops::Deref for dyn MoleculeProjectsWriter {
    type Target = MoleculeDatasetWriter;

    fn deref(&self) -> &Self::Target {
        self.raw_writer()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
