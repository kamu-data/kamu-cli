// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use internal_error::InternalError;

use crate::{
    DeclarativeResource,
    ResourceTypeMismatchError,
    ResourceUID,
    ResourceUIDNotFoundError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TypedResourceQueryService<R: DeclarativeResource>: Send + Sync {
    async fn ensure_resource_uid_matches_type(
        &self,
        uid: &ResourceUID,
    ) -> Result<(), TypedResourceQueryError>;

    async fn get_state_by_uid(
        &self,
        account_id: odf::AccountID,
        uid: &ResourceUID,
    ) -> Result<R::ResourceState, TypedResourceQueryError>;

    async fn list_states_by_kind(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<Vec<R::ResourceState>, InternalError>;
}

#[derive(Debug, thiserror::Error)]
pub enum TypedResourceQueryError {
    #[error(transparent)]
    NotFound(#[from] ResourceUIDNotFoundError),

    #[error(transparent)]
    TypeMismatch(#[from] ResourceTypeMismatchError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
