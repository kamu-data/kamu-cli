// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::{
    DeclarativeResource,
    ResourceID,
    ResourceIDNotFoundError,
    ResourceLabelPair,
    ResourceVisibility,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Checks whether a principal may run an operation on resources of one type.
///
/// A type without a guard of its own is checked by the default rule, whether
/// the principal may use the account ([`crate::ResourceAccountAccessPort`]).
/// A type's guard replaces that rule and may delegate to it. Its doc comment holds the type's rule
/// table, as `DatasetOperationGuard` does for datasets.
///
/// A guard decides which account may be used and whether the principal may
/// see a resource there; it never widens a query across accounts. Anonymous
/// callers are turned away before any guard runs.
///
/// A resource the principal may not read fails as not found, so a denial does
/// not reveal that it exists; a readable one fails with the missing
/// permission.
///
/// A `List` the guard allows returns every resource of the selector without a
/// further check, so it must not admit a resource that `Read` would hide.
#[async_trait::async_trait]
pub trait ResourceOperationGuard<R: DeclarativeResource>: Send + Sync {
    /// Decides on the account before any resource is loaded. With
    /// [`ResourceAccountAccess::PerResource`], every loaded resource is then
    /// checked with [`Self::check_operations`]
    async fn check_account(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        account: &odf::ResourceOwnerHandle,
        operation: ResourceAccountOperation,
    ) -> Result<ResourceAccountAccess, ResourceOperationError>;

    async fn check_operation(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        request: &ResourceOperationRequest<'_, R>,
    ) -> Result<(), ResourceOperationError>;

    /// One result per request, in request order
    async fn check_operations(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        requests: &[ResourceOperationRequest<'_, R>],
    ) -> Result<Vec<Result<(), ResourceOperationError>>, InternalError> {
        let mut results = Vec::with_capacity(requests.len());
        for request in requests {
            results.push(match self.check_operation(principal, request).await {
                Ok(()) => Ok(()),
                Err(ResourceOperationError::Internal(err)) => return Err(err),
                Err(
                    err @ (ResourceOperationError::NotFound(_) | ResourceOperationError::Access(_)),
                ) => Err(err),
            });
        }
        Ok(results)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceOperationRequest<'a, R: DeclarativeResource> {
    /// The account the resource is in, or is to be created in
    pub account: &'a odf::ResourceOwnerHandle,
    pub operation: ResourceOperation<'a, R>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ResourceOperation<'a, R: DeclarativeResource> {
    /// An apply that creates the resource, with its prepared spec
    Create {
        spec: &'a R::SpecInput,
    },
    /// An apply that changes an existing resource, with the prepared spec
    Update {
        current: &'a R,
        new_spec: &'a R::SpecInput,
    },
    Read {
        resource: &'a R,
    },
    Delete {
        resource: &'a R,
    },
    /// A search of the account by one selector of this type
    List {
        scope: &'a ResourceListScope,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An operation decided on the account first, then per resource
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceAccountOperation {
    Read,
    Delete,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceAccountAccess {
    /// Every resource of the type in the account
    Allowed,
    /// Each loaded resource is checked on its own
    PerResource,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What a search selector filters by, as the repository will: a guard may
/// require a filter before it allows another account
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceListScope {
    pub id: Option<ResourceID>,
    /// SQL `LIKE` pattern
    pub name_pattern: Option<String>,
    /// Canonical label pairs, all of which a listed resource carries
    pub label_pairs: Vec<ResourceLabelPair>,
    /// The resource that listed resources must name as an owner
    pub owner: Option<ResourceListOwner>,
    /// The states of deletion listed resources may be in
    pub visibility: ResourceVisibility,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The owner a search narrows by, and whether the principal may read it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceListOwner {
    /// A live resource the principal may read
    Found(ResourceID),
    /// An ID that names no live resource the principal may read: the owner
    /// may be gone, hidden from the principal, or never have existed
    NotFound(ResourceID),
}

impl ResourceListOwner {
    pub fn id(self) -> ResourceID {
        match self {
            Self::Found(id) | Self::NotFound(id) => id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResourceOperationError {
    /// The principal may not read the resource
    #[error(transparent)]
    NotFound(#[from] ResourceIDNotFoundError),

    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
