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
    ResourceID,
    ResourceMetadataInput,
    ResourceNotFoundError,
    ResourceSnapshot,
    ResourceTypeMismatchError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceQueryService<R: DeclarativeResource>: Send + Sync {
    async fn allocate_id(&self) -> Result<ResourceID, InternalError>;

    async fn find_existing_id_by_name(
        &self,
        resource_id: Option<ResourceID>,
        metadata: &ResourceMetadataInput,
    ) -> Result<Option<ResourceID>, InternalError>;

    async fn ensure_resource_id_matches_type(
        &self,
        resource_id: &ResourceID,
    ) -> Result<(), TypedResourceQueryError>;

    async fn find_owned_snapshot(
        &self,
        account_id: &odf::AccountID,
        resource_id: ResourceID,
    ) -> Result<Option<ResourceSnapshot>, FindOwnedResourceError>;

    async fn get_state_by_id(
        &self,
        account_id: odf::AccountID,
        resource_id: &ResourceID,
    ) -> Result<R::ResourceState, TypedResourceQueryError>;

    async fn list_states_by_kind(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<Vec<R::ResourceState>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum FindOwnedResourceError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] internal_error::InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum TypedResourceQueryError {
    #[error(transparent)]
    NotFound(#[from] ResourceNotFoundError),

    #[error(transparent)]
    TypeMismatch(#[from] ResourceTypeMismatchError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
