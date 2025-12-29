// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_core::PushIngestError;

use crate::{MoleculeAnnouncementPayloadRecord, MoleculeProject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeCreateAnnouncementUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        molecule_project: &MoleculeProject,
        source_event_time: Option<DateTime<Utc>>,
        announcement_record: MoleculeAnnouncementPayloadRecord,
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
    QuotaExceeded(
        #[from]
        #[backtrace]
        kamu_accounts::QuotaError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<PushIngestError> for MoleculeCreateAnnouncementError {
    fn from(err: PushIngestError) -> Self {
        match err {
            PushIngestError::Access(e) => Self::Access(e),
            PushIngestError::QuotaExceeded(e) => Self::QuotaExceeded(e),
            PushIngestError::Internal(e) => Self::Internal(e),
            other => Self::Internal(other.int_err()),
        }
    }
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
