// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use odf::metadata::serde::yaml::IntoDto;

use crate::{ResourceHandle, ResourceID, ResourceRef, TypeRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MAX_OWNER_REFERENCES: usize = 64;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Why a reference cannot name an owner, whatever resources exist
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidOwnerReferenceError {
    #[error("resources cannot be referenced by `did` yet")]
    Did,

    #[error("at least one of `id` or `name` is required")]
    Empty,

    #[error("a `name` also needs a `type`")]
    UntypedName,

    #[error("unknown resource type '{0}'")]
    UnknownType(TypeRef),
}

impl From<UnresolvableResourceRefError> for InvalidOwnerReferenceError {
    fn from(value: UnresolvableResourceRefError) -> Self {
        match value {
            UnresolvableResourceRefError::Did => Self::Did,
            UnresolvableResourceRefError::Empty => Self::Empty,
            UnresolvableResourceRefError::UntypedName => Self::UntypedName,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Why a reference cannot name a resource, judged by its fields alone
#[derive(Debug, PartialEq, Eq)]
pub enum UnresolvableResourceRefError {
    Did,
    Empty,
    UntypedName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn validate_owner_reference(
    owner_reference: &ResourceRef,
) -> Result<(), UnresolvableResourceRefError> {
    if owner_reference.did.is_some() {
        return Err(UnresolvableResourceRefError::Did);
    }
    if owner_reference.id.is_none() {
        if owner_reference.name.is_none() {
            return Err(UnresolvableResourceRefError::Empty);
        }
        if owner_reference.r#type.is_none() {
            return Err(UnresolvableResourceRefError::UntypedName);
        }
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Owners are resolved as part of the apply process, before a
/// [`ResourceHeadersInput`](crate::ResourceHeadersInput) is constructed.
/// Panics on a reference that was not, as for the account.
///
/// `account` is that of the owned resource, which its owners share. It replaces
/// the one in a reference, whose name may be out of date.
pub(crate) fn owner_handles_from_input(
    input: Option<Vec<ResourceRef>>,
    account: &odf::ResourceOwnerHandle,
) -> Option<Vec<ResourceHandle>> {
    let handles = input
        .unwrap_or_default()
        .into_iter()
        .map(|owner_reference| owner_handle_from_resolved_ref(owner_reference, account))
        .collect::<Vec<_>>();

    (!handles.is_empty()).then_some(handles)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn owner_handle_from_resolved_ref(
    owner_reference: ResourceRef,
    account: &odf::ResourceOwnerHandle,
) -> ResourceHandle {
    match owner_reference {
        ResourceRef {
            account: _,
            r#type: Some(TypeRef::Uri(r#type)),
            id: Some(id),
            did,
            name: Some(name),
        } => ResourceHandle {
            account: account.clone(),
            r#type,
            id,
            did,
            name,
        },
        other => panic!(
            "ResourceHeadersInput.owner_references must be fully resolved by the time headers are \
             constructed, got: {other:?}"
        ),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Lists of references are equal if they name the same owners, in any order.
/// The other parts of a handle only record what the owner was like.
pub(crate) fn have_same_owners(
    handles: Option<&Vec<ResourceHandle>>,
    input: Option<&Vec<ResourceRef>>,
) -> bool {
    let current = handles
        .into_iter()
        .flatten()
        .map(|handle| Some(handle.id))
        .collect::<BTreeSet<_>>();
    let new = input
        .into_iter()
        .flatten()
        .map(|owner_reference| owner_reference.id)
        .collect::<BTreeSet<_>>();

    current == new
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// IDs of the owners, as indexed for queries by owner
pub fn owner_ids(owner_references: Option<&Vec<ResourceHandle>>) -> Vec<ResourceID> {
    owner_references
        .into_iter()
        .flatten()
        .map(|handle| handle.id)
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn resource_owner_references_to_json(
    owner_references: Option<&Vec<ResourceHandle>>,
) -> Option<serde_json::Value> {
    owner_references.map(|handles| {
        let proxies = handles
            .iter()
            .cloned()
            .map(odf::metadata::serde::yaml::resources::ResourceHandle::from)
            .collect::<Vec<_>>();
        serde_json::to_value(proxies).unwrap()
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Takes `account` as [`owner_handles_from_input`] does
pub(crate) fn resource_owner_references_from_json(
    value: Option<serde_json::Value>,
    account: &odf::ResourceOwnerHandle,
) -> Option<Vec<ResourceHandle>> {
    value.map(|value| {
        let proxies: Vec<odf::metadata::serde::yaml::resources::ResourceHandle> =
            serde_json::from_value(value).unwrap();
        proxies
            .into_iter()
            .map(|proxy| ResourceHandle {
                account: account.clone(),
                ..proxy.into_dto().unwrap()
            })
            .collect()
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
