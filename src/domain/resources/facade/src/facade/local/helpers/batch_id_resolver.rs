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
    ResourceRef,
    ResourceVisibility,
    TypeUri,
    resource_type_name,
};

use crate::{BatchResourceError, BatchResourceProblem, ResourceLookupProblem};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) type BatchIdEntries = Vec<(usize, ResourceRef, ResourceID)>;
pub(crate) type BatchNameEntries = Vec<(usize, ResourceRef, ResourceName)>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct BatchResourceRefGroups {
    pub id_entries: BatchIdEntries,
    pub name_entries: BatchNameEntries,
    /// Refs naming neither an id nor a name, carried as per-item problems so a
    /// bad entry fails only itself and the surviving indexes stay aligned.
    pub problems: Vec<BatchResourceProblem<ResourceLookupProblem>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Result of the name→UID resolution phase. All requests are now keyed by UID;
/// any name-not-found failures are recorded in `problems`.
pub(crate) struct BatchIdsResolutionResponse {
    pub id_entries: BatchIdEntries,
    pub problems: Vec<BatchResourceProblem<ResourceLookupProblem>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Split a flat list of refs into those already keyed by ID and those that
/// still need a name→ID lookup.
///
/// A ref carrying both is keyed by its ID: ODF treats the pair as a consistency
/// assertion rather than two lookups, and the ID is the authoritative half. The
/// assertion's other half is checked by `validate_handle_row`, which fails the
/// entry if the resolved resource turns out to carry a different name.
pub(crate) fn group_batch_resource_refs(resource_refs: Vec<ResourceRef>) -> BatchResourceRefGroups {
    let mut uid_entries = Vec::new();
    let mut name_entries = Vec::new();
    let mut problems = Vec::new();

    for (request_index, resource_ref) in resource_refs.into_iter().enumerate() {
        match (resource_ref.id, resource_ref.name.clone()) {
            (Some(id), _) => uid_entries.push((request_index, resource_ref, id)),
            (None, Some(name)) => name_entries.push((request_index, resource_ref, name)),
            (None, None) => problems.push(BatchResourceProblem {
                request_index,
                error: ResourceLookupProblem::EmptyRef,
            }),
        }
    }

    BatchResourceRefGroups {
        id_entries: uid_entries,
        name_entries,
        problems,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolve all `ByName` groups to UIDs using a single batched query per schema.
/// Returns a combined list of `(request_index, request, id)` entries plus any
/// name-not-found problems. Under [`ResourceVisibility::IncludeDeleting`] a
/// name may yield several entries, the live resource first.
pub(crate) async fn resolve_batch_ids(
    query_service: &dyn GenericResourceQueryService,
    account_id: &odf::AccountID,
    schema: &TypeUri,
    groups: BatchResourceRefGroups,
    visibility: ResourceVisibility,
) -> Result<BatchIdsResolutionResponse, BatchResourceError> {
    let mut id_entries = groups.id_entries;
    let mut problems = groups.problems;

    if !groups.name_entries.is_empty() {
        let names = groups
            .name_entries
            .iter()
            .map(|(_, _, name)| name.clone())
            .collect::<Vec<_>>();

        let mut ids_by_name = HashMap::<_, Vec<_>>::new();
        for (name, id) in query_service
            .resolve_resource_ids_by_names(account_id, schema, &names, visibility)
            .await?
        {
            // A name repeated in the request may come back once per occurrence
            let ids = ids_by_name.entry(name).or_default();
            if !ids.contains(&id) {
                ids.push(id);
            }
        }

        for (request_index, resource_ref, name) in groups.name_entries {
            match ids_by_name.get(&name) {
                Some(ids) => id_entries.extend(
                    ids.iter()
                        .map(|id| (request_index, resource_ref.clone(), *id)),
                ),
                None => problems.push(BatchResourceProblem {
                    request_index,
                    error: ResourceLookupProblem::NameNotFound(ResourceNameNotFoundError {
                        type_name: resource_type_name(schema)?,
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

/// UID not-found error for use after a batch UID fetch misses an entry.
pub(crate) fn id_not_found(id: ResourceID) -> ResourceLookupProblem {
    ResourceLookupProblem::IDNotFound(ResourceIDNotFoundError(id))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
