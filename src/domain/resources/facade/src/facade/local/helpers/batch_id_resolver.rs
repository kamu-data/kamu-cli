// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use kamu_resources::{
    GenericResourceQueryService,
    ResourceID,
    ResourceIDNotFoundError,
    ResourceName,
    ResourceNameNotFoundError,
};

use crate::{
    BatchResourceError,
    BatchResourceProblem,
    ResourceBatchSelector,
    ResourceLookupProblem,
    ResourceRef,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) type BatchIdEntries = Vec<(usize, ResourceRef, ResourceID)>;
pub(crate) type BatchNameEntries = Vec<(usize, ResourceRef, ResourceName)>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct BatchResourceRefGroups {
    pub id_entries: BatchIdEntries,
    pub name_entries: BatchNameEntries,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Result of the name→UID resolution phase. All requests are now keyed by UID;
/// any name-not-found failures are recorded in `problems`.
pub(crate) struct BatchIdsResolutionResponse {
    pub id_entries: BatchIdEntries,
    pub problems: Vec<BatchResourceProblem<ResourceLookupProblem>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Split a flat list of requests into those already keyed by UID and those
/// that still need a name→UID lookup, grouped by kind.
pub(crate) fn group_batch_resource_refs(selector: ResourceBatchSelector) -> BatchResourceRefGroups {
    let mut uid_entries = Vec::new();
    let mut name_entries = Vec::new();

    for (request_index, resource_ref) in selector.resource_refs.into_iter().enumerate() {
        match &resource_ref {
            ResourceRef::ById(id) => {
                let id = *id;
                uid_entries.push((request_index, resource_ref, id));
            }
            ResourceRef::ByName(name) => {
                let name = name.clone();
                name_entries.push((request_index, resource_ref, name));
            }
        }
    }

    BatchResourceRefGroups {
        id_entries: uid_entries,
        name_entries,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolve all `ByName` groups to UIDs using a single batched query per kind.
/// Returns a combined list of `(request_index, request, id)` entries plus any
/// name-not-found problems.
pub(crate) async fn resolve_batch_ids(
    query_service: &dyn GenericResourceQueryService,
    account_id: &odf::AccountID,
    kind: &str,
    groups: BatchResourceRefGroups,
) -> Result<BatchIdsResolutionResponse, BatchResourceError> {
    let mut id_entries = groups.id_entries;
    let mut problems = Vec::new();

    if !groups.name_entries.is_empty() {
        let names = groups
            .name_entries
            .iter()
            .map(|(_, _, name)| name.clone())
            .collect::<Vec<_>>();

        let id_by_name = query_service
            .find_resource_identities_by_names(account_id, kind, &names)
            .await?
            .into_iter()
            .map(|row| {
                (
                    ResourceName::new_unchecked(&row.name),
                    ResourceID::new(row.id),
                )
            })
            .collect::<HashMap<_, _>>();

        for (request_index, resource_ref, name) in groups.name_entries {
            match id_by_name.get(&name) {
                Some(id) => id_entries.push((request_index, resource_ref, *id)),
                None => problems.push(BatchResourceProblem {
                    request_index,
                    error: ResourceLookupProblem::NameNotFound(ResourceNameNotFoundError {
                        kind: kind.to_string(),
                        name,
                    }),
                }),
            }
        }
    }

    Ok(BatchIdsResolutionResponse {
        id_entries,
        problems,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Scalar name→ID resolution. Returns `None`-as-`NameNotFound` converted to
/// the caller's error type so it can be used in both batch and scalar paths.
pub(crate) async fn resolve_resource_id<E>(
    query_service: &dyn GenericResourceQueryService,
    kind: &str,
    account_id: &odf::AccountID,
    resource_ref: &ResourceRef,
) -> Result<ResourceID, E>
where
    E: From<internal_error::InternalError> + From<ResourceLookupProblem>,
{
    match resource_ref {
        ResourceRef::ById(id) => Ok(*id),
        ResourceRef::ByName(name) => query_service
            .find_resource_id_by_name(account_id, kind, name)
            .await?
            .ok_or_else(|| {
                ResourceLookupProblem::NameNotFound(ResourceNameNotFoundError {
                    kind: kind.to_string(),
                    name: name.clone(),
                })
                .into()
            }),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// UID not-found error for use after a batch UID fetch misses an entry.
pub(crate) fn id_not_found(id: ResourceID) -> ResourceLookupProblem {
    ResourceLookupProblem::IDNotFound(ResourceIDNotFoundError(id))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
