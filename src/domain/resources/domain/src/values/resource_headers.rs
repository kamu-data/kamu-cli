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
        let account = account_handle_from_input(&input);

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

        self.account == account_handle_from_input(input)
            && self.name == input.name
            && self.labels.entries == *input_labels
            && self.annotations.entries == *input_annotations
    }

    fn apply_update(&mut self, now: DateTime<Utc>, input: ResourceHeadersInput) {
        self.account = account_handle_from_input(&input);
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

/// Account resolution (id + name lookup) happens upstream, as part of the
/// apply process (see `ResourceAccountResolver`), before a
/// [`ResourceHeadersInput`] is ever constructed — every production caller
/// populates `account` with an already-resolved
/// [`auth::AccountRef::IdAndName`]. The facade also overwrites the resulting
/// header's `account` with the freshly-resolved handle at the view boundary on
/// every read, so the name carried here is never persisted or relied upon (see
/// plan `.spec/022.resource-headers.plan.md` — avoiding name denormalization is
/// the point of that JOIN-on-read design). This function only exists to
/// satisfy the aliased struct's mandatory `account` field during
/// construction and panics if handed an unresolved reference, which would
/// indicate a caller bypassed account resolution.
fn account_handle_from_input(input: &ResourceHeadersInput) -> auth::AccountHandle {
    match &input.account {
        Some(auth::AccountRef::IdAndName(account)) => auth::AccountHandle {
            // The account *resource* id is not carried by an `AccountRef`. This
            // handle is a construction-time placeholder that the facade always
            // overwrites with the freshly-resolved handle (incl. the real
            // resource id) at the JOIN-on-read view boundary — see the doc
            // comment above — so a nil id here is never persisted or observed.
            id: odf::ResourceID::new(uuid::Uuid::nil()),
            did: account.did.clone(),
            name: account.name.clone(),
        },
        other => panic!(
            "ResourceHeadersInput.account must be a resolved AccountRef::IdAndName by the time \
             headers are constructed, got: {other:?}"
        ),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
