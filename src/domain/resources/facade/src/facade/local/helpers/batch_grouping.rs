// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ResourceAccountRef, ResourceRef, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One `(account, schema)` slice of a batch, with the original request indexes
/// preserved.
pub(crate) struct BatchTargetGroup {
    /// The resolved account, carried whole rather than as a bare id: the
    /// resource-view path stamps it into `ResourceHeaders.account`.
    pub account: odf::AccountHandle,
    pub schema: TypeUri,
    /// `(request_index, ref)` pairs, so results can be merged back into the
    /// caller's ordering after every group has run.
    pub entries: Vec<(usize, ResourceRef)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Splits a batch into `(account, schema)` groups.
///
/// The services underneath are scalar in both —
/// `resolve_resource_ids_by_names`, `find_resource_handles_by_ids`,
/// `find_snapshots_by_ids` and the CRUD dispatcher's `delete` all take one
/// account and (where relevant) one schema — so a batch spanning either has to
/// be issued as one call per combination. Each group is still *batched*
/// internally, so an N-name group stays a single query; only the number of
/// distinct `(account, schema)` pairs multiplies the round trips.
///
/// This replaces the former `uniform_batch_target`, which rejected mixed
/// batches outright because account resolution was singular. Lifting it is the
/// point of the ODF-shaped `ResourceRef`: the wire has carried a per-ref
/// account and type since those types were adopted.
///
/// Accounts are compared **after** resolution, so the same account spelled by
/// id in one ref and by name in another lands in one group. The old check
/// compared unresolved refs structurally and rejected that case.
///
/// Groups come back in first-appearance order, which keeps the emitted query
/// order stable for a given input — worth having when reading logs, and it
/// makes the write path deterministic.
pub(crate) fn group_by_account_and_schema(
    entries: Vec<(usize, ResourceRef, odf::AccountHandle, TypeUri)>,
) -> Vec<BatchTargetGroup> {
    let mut groups: Vec<BatchTargetGroup> = Vec::new();

    for (request_index, resource_ref, account, schema) in entries {
        match groups
            .iter_mut()
            .find(|group| group.account.did == account.did && group.schema == schema)
        {
            Some(group) => group.entries.push((request_index, resource_ref)),
            None => groups.push(BatchTargetGroup {
                account,
                schema,
                entries: vec![(request_index, resource_ref)],
            }),
        }
    }

    groups
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The distinct account refs a batch names, in first-appearance order.
///
/// Deduplicated by spelling before resolution so that a batch naming one
/// account N times costs one lookup, not N. Two spellings of the same account
/// still resolve twice — which costs a lookup but cannot produce a wrong
/// answer, since the resolved ids are what the grouping compares.
pub(crate) fn distinct_account_refs(
    resource_refs: &[ResourceRef],
) -> Vec<Option<ResourceAccountRef>> {
    let mut seen: Vec<Option<ResourceAccountRef>> = Vec::new();

    for resource_ref in resource_refs {
        if !seen.iter().any(|account| account == &resource_ref.account) {
            seen.push(resource_ref.account.clone());
        }
    }

    seen
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
