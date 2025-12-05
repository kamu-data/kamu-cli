// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{MoleculeGlobalAnnouncementDataRecord, MoleculeProject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeCreateAnnouncementUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        molecule_project: &MoleculeProject,
        global_announcement: MoleculeGlobalAnnouncementDataRecord,
    ) -> Result<MoleculeCreateAnnouncementResult, MoleculeCreateAnnouncementError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeCreateAnnouncementResult {
    pub new_announcement_id: uuid::Uuid,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum MoleculeCreateAnnouncementError {
    #[error(transparent)]
    NotFoundAttachments(#[from] MoleculeCreateAnnouncementNotFoundAttachmentsError),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error)]
#[error(
    "Not found attachment(s): {}",
    format_utils::format_collection(not_found_dataset_ids)
)]
pub struct MoleculeCreateAnnouncementNotFoundAttachmentsError {
    pub not_found_dataset_ids: Vec<odf::DatasetID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
