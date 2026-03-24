// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::ConcurrentModificationError;
use internal_error::InternalError;
use thiserror::Error;

use crate::{DeclarativeResource, ResourceID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DeleteResourcesUseCase<R: DeclarativeResource>: Send + Sync {
    async fn execute(
        &self,
        account_id: odf::AccountID,
        resource_ids: Vec<ResourceID>,
    ) -> Result<(), DeleteResourcesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DeleteResourcesError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeleteResourcesError {
    pub fn not_enough_permissions(resource_id: ResourceID, resource_type: &'static str) -> Self {
        Self::Access(odf::AccessError::Unauthorized(
            DeleteResourcesNotEnoughPermissionsError {
                resource_id,
                resource_type,
            }
            .into(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("User has no permission to delete resource '{resource_id}' of type '{resource_type}'")]
pub struct DeleteResourcesNotEnoughPermissionsError {
    pub resource_id: ResourceID,
    pub resource_type: &'static str,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
