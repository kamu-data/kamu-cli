// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;

use crate::MoleculeDataRoomActivityPayloadRecord;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeAppendGlobalDataRoomActivityUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        source_event_time: DateTime<Utc>,
        activity_record: MoleculeDataRoomActivityPayloadRecord,
    ) -> Result<(), MoleculeAppendDataRoomActivityError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeAppendDataRoomActivityError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
