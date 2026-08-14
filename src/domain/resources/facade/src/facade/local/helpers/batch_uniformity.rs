// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ResourceAccountRef, ResourceRef, TypeRef};

use crate::NonUniformBatchError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The single account and type a batch operates on.
pub(crate) struct UniformBatchTarget {
    pub account: Option<ResourceAccountRef>,
    pub r#type: TypeRef,
}

/// Extracts the one account and type a batch shares, rejecting a batch that
/// mixes them.
///
/// The batch pipeline below resolves one account and one schema up front, then
/// applies both to every entry — so a mixed batch cannot be executed correctly,
/// only silently mis-executed against whichever entry happened to be first.
///
/// This restriction is what the ODF-shaped API is meant to lift: the wire
/// already carries a per-ref account and type, and the repository already
/// fans out per type. Only account resolution is still singular
/// ([`ResourceAccountResolver::resolve_target_account`] returns one handle),
/// so the check stays until that is batched.
///
/// [`ResourceAccountResolver::resolve_target_account`]: crate::ResourceAccountResolver::resolve_target_account
/// Returns `None` for an empty batch, which names nothing and so yields an
/// empty result rather than an error — batch operations stay idempotent, and
/// callers need no special case for "I had nothing to fetch".
pub(crate) fn uniform_batch_target(
    resource_refs: &[ResourceRef],
) -> Result<Option<UniformBatchTarget>, NonUniformBatchError> {
    let Some(first) = resource_refs.first() else {
        return Ok(None);
    };

    for resource_ref in &resource_refs[1..] {
        if resource_ref.r#type != first.r#type {
            return Err(NonUniformBatchError::MixedTypes {
                first: first.r#type.to_string(),
                other: resource_ref.r#type.to_string(),
            });
        }
        if !accounts_match(resource_ref.account.as_ref(), first.account.as_ref()) {
            return Err(NonUniformBatchError::MixedAccounts);
        }
    }

    Ok(Some(UniformBatchTarget {
        account: first.account.clone(),
        r#type: first.r#type.clone(),
    }))
}

/// Compares two unresolved account references structurally.
///
/// Deliberately conservative: two references naming the same account by
/// different fields (id vs name) compare unequal and are rejected rather than
/// resolved and compared, which would cost a lookup per entry. Callers can
/// always spell the account the same way.
fn accounts_match(left: Option<&ResourceAccountRef>, right: Option<&ResourceAccountRef>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.id == right.id && left.did == right.did && left.name == right.name
        }
        _ => false,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
