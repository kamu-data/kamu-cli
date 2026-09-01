// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use odf::metadata::auth;

use crate::{ResourceHeadersInput, ResourceID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ResourceName = odf::metadata::resource::ResourceName;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ResourceHeaders = odf::metadata::resource::ResourceHeaders;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Placeholder account name used when a resource's owning account can no
/// longer be found (e.g. account deletion raced ahead of the async outbox
/// cleanup of that account's resources). Repos substitute this instead of
/// failing the read, since `resource_id` FK on `account_id` is not
/// cascade-enforced across this bounded-context boundary.
///
/// Postgres/SQLite embed this same literal directly in their
/// `COALESCE(a.account_name, 'deleted-account')` queries. It cannot be
/// spliced in via this constant: `sqlx::query!` inspects its SQL argument as
/// a raw token *before* macro expansion, so neither this `const` nor a
/// `concat!`-based `macro_rules!` twin of it can be substituted in — the
/// macro requires a literal written out in place. Keep both in sync by hand
/// if this value ever changes.
pub const DELETED_ACCOUNT_NAME_SENTINEL: &str = "deleted-account";

pub fn deleted_account_name_sentinel() -> auth::AccountName {
    auth::AccountName::new_unchecked(DELETED_ACCOUNT_NAME_SENTINEL)
}

/// Placeholder account-resource id substituted alongside
/// [`DELETED_ACCOUNT_NAME_SENTINEL`] when a resource's owning account can no
/// longer be found (e.g. deletion racing async cleanup). The nil UUID.
///
/// The SQL backends embed the equivalent nil-UUID literal directly in their
/// `COALESCE(a.resource_id, '00000000-0000-0000-0000-000000000000')` reads
/// (`sqlx::query!` cannot splice a Rust const into its SQL, same caveat as the
/// name sentinel), so keep the two in sync by hand if this ever changes.
pub fn deleted_account_resource_id_sentinel() -> ResourceID {
    ResourceID::new(uuid::Uuid::nil())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceHeadersExt {
    fn simple(now: DateTime<Utc>, id: ResourceID, account: auth::AccountHandle, name: &str)
    -> Self;
    fn from_input(now: DateTime<Utc>, id: ResourceID, input: ResourceHeadersInput) -> Self;
    fn is_equivalent_to(&self, input: &ResourceHeadersInput) -> bool;
    fn apply_update(&mut self, now: DateTime<Utc>, input: ResourceHeadersInput);
}

impl ResourceHeadersExt for ResourceHeaders {
    fn simple(
        now: DateTime<Utc>,
        id: ResourceID,
        account: auth::AccountHandle,
        name: &str,
    ) -> Self {
        Self {
            id,
            account,
            name: ResourceName::new_unchecked(name),
            labels: odf::metadata::resource::ResourceLabels {
                entries: std::collections::BTreeMap::new(),
            },
            annotations: odf::metadata::resource::ResourceAnnotations {
                entries: std::collections::BTreeMap::new(),
            },
            generation: 0,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        }
    }

    fn from_input(now: DateTime<Utc>, id: ResourceID, input: ResourceHeadersInput) -> Self {
        let account = account_handle_from_input(input.account.as_ref());

        Self {
            id,
            account,
            name: input.name,
            labels: input
                .labels
                .unwrap_or_else(|| odf::metadata::resource::ResourceLabels {
                    entries: std::collections::BTreeMap::new(),
                }),
            annotations: input.annotations.unwrap_or_else(|| {
                odf::metadata::resource::ResourceAnnotations {
                    entries: std::collections::BTreeMap::new(),
                }
            }),
            generation: 1,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        }
    }

    fn is_equivalent_to(&self, input: &ResourceHeadersInput) -> bool {
        let empty_map = std::collections::BTreeMap::new();
        let input_labels = input.labels.as_ref().map_or(&empty_map, |l| &l.entries);
        let input_annotations = input
            .annotations
            .as_ref()
            .map_or(&empty_map, |a| &a.entries);

        self.account == account_handle_from_input(input.account.as_ref())
            && self.name == input.name
            && self.labels.entries == *input_labels
            && self.annotations.entries == *input_annotations
    }

    fn apply_update(&mut self, now: DateTime<Utc>, input: ResourceHeadersInput) {
        self.account = account_handle_from_input(input.account.as_ref());
        self.name = input.name;
        self.labels = input
            .labels
            .unwrap_or_else(|| odf::metadata::resource::ResourceLabels {
                entries: std::collections::BTreeMap::new(),
            });
        self.annotations =
            input
                .annotations
                .unwrap_or_else(|| odf::metadata::resource::ResourceAnnotations {
                    entries: std::collections::BTreeMap::new(),
                });

        self.updated_at = now;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Account resolution (id + did + name lookup) happens upstream, as part of
/// the apply process (see `ResourceAccountResolver`), before a
/// [`ResourceHeadersInput`] is ever constructed — every production caller
/// populates `account` with a fully-resolved [`auth::AccountRef`]. Panics if
/// handed a partially-resolved reference, which would indicate a caller
/// bypassed account resolution.
fn account_handle_from_input(input: Option<&auth::AccountRef>) -> auth::AccountHandle {
    match input {
        Some(auth::AccountRef {
            id: Some(id),
            did: Some(did),
            name: Some(name),
        }) => auth::AccountHandle {
            id: *id,
            did: did.clone(),
            name: name.clone(),
        },
        other => panic!(
            "ResourceHeadersInput.account must be a fully resolved AccountRef (id, did, and name \
             all present) by the time headers are constructed, got: {other:?}"
        ),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
