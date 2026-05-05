// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use domain::{GenericResourceQueryService, ResourceName, ResourceUID};
use kamu_resources as domain;
use kamu_resources::{ResourceNameNotFoundError, ResourceUIDNotFoundError};

use crate::{BatchRequestProblem, GetResourceError, GetResourceRequest, ResourceRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) type BatchUidEntries = Vec<(usize, GetResourceRequest, ResourceUID)>;
pub(super) type BatchNameGroups = HashMap<String, Vec<(usize, GetResourceRequest, ResourceName)>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) struct BatchRequestGroups {
    pub uid_entries: BatchUidEntries,
    pub name_groups: BatchNameGroups,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Result of the name→UID resolution phase. All requests are now keyed by UID;
/// any name-not-found failures are recorded in `problems`.
pub(super) struct BatchUidsResolutionResponse {
    pub uid_entries: BatchUidEntries,
    pub problems: Vec<BatchRequestProblem<GetResourceError>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Split a flat list of requests into those already keyed by UID and those
/// that still need a name→UID lookup, grouped by kind.
pub(super) fn group_batch_requests(requests: Vec<GetResourceRequest>) -> BatchRequestGroups {
    let mut uid_entries = Vec::new();
    let mut name_groups: BatchNameGroups = HashMap::new();

    for (request_index, get_request) in requests.into_iter().enumerate() {
        match &get_request.resource_ref {
            ResourceRef::ById(uid) => {
                let uid = *uid;
                uid_entries.push((request_index, get_request, uid));
            }
            ResourceRef::ByName(name) => {
                let name = name.clone();
                name_groups
                    .entry(get_request.kind.clone())
                    .or_default()
                    .push((request_index, get_request, name));
            }
        }
    }

    BatchRequestGroups {
        uid_entries,
        name_groups,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolve all `ByName` groups to UIDs using a single batched query per kind.
/// Returns a combined list of `(request_index, request, uid)` entries plus any
/// name-not-found problems.
pub(super) async fn resolve_batch_uids(
    query_service: &dyn GenericResourceQueryService,
    account_id: &odf::AccountID,
    groups: BatchRequestGroups,
) -> Result<BatchUidsResolutionResponse, GetResourceError> {
    let mut uid_entries = groups.uid_entries;
    let mut problems = Vec::new();

    for (kind, entries) in groups.name_groups {
        let names = entries
            .iter()
            .map(|(_, _, name)| name.clone())
            .collect::<Vec<_>>();

        let uid_by_name = query_service
            .find_resource_identities_by_names(account_id, &kind, &names)
            .await?
            .into_iter()
            .map(|row| (row.name, ResourceUID::new(row.uid)))
            .collect::<HashMap<_, _>>();

        for (request_index, get_request, name) in entries {
            match uid_by_name.get(&name) {
                Some(uid) => uid_entries.push((request_index, get_request, *uid)),
                None => problems.push(BatchRequestProblem {
                    request_index,
                    error: GetResourceError::NameNotFound(ResourceNameNotFoundError {
                        kind: get_request.kind,
                        name,
                    }),
                }),
            }
        }
    }

    Ok(BatchUidsResolutionResponse {
        uid_entries,
        problems,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Scalar name→UID resolution. Returns `None`-as-`NameNotFound` converted to
/// the caller's error type so it can be used in both batch and scalar paths.
pub(super) async fn resolve_resource_uid<E>(
    query_service: &dyn GenericResourceQueryService,
    kind: &str,
    account_id: &odf::AccountID,
    resource_ref: &ResourceRef,
) -> Result<ResourceUID, E>
where
    E: From<internal_error::InternalError> + From<ResourceNameNotFoundError>,
{
    match resource_ref {
        ResourceRef::ById(uid) => Ok(*uid),
        ResourceRef::ByName(name) => query_service
            .find_resource_uid_by_name(account_id, kind, name)
            .await?
            .ok_or_else(|| {
                ResourceNameNotFoundError {
                    kind: kind.to_string(),
                    name: name.clone(),
                }
                .into()
            }),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// UID not-found error for use after a batch UID fetch misses an entry.
pub(super) fn uid_not_found(uid: ResourceUID) -> GetResourceError {
    GetResourceError::UIDNotFound(ResourceUIDNotFoundError(uid))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
