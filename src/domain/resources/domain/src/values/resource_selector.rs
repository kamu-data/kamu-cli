// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{ResourceAccountRef, ResourceID, ResourceLabelFilterInput, TypeRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Matches zero or many resources using identity and label filters.
///
/// Field-for-field the ODF `ResourceSelector`, with exactly one documented
/// superset: `r#type` is optional, meaning *any type*. The spec requires
/// `type`, but the API already supports type-less listing, and encoding that as
/// `None` avoids a magic `%` token on the wire. That single difference is why
/// this is a struct rather than a type alias like
/// [`ResourceRef`](crate::ResourceRef).
///
/// Several selectors in one call act as a logical OR.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResourceSelector {
    pub account: Option<ResourceAccountRef>,
    /// `None` matches every registered resource type.
    pub r#type: Option<TypeRef>,
    pub id: Option<ResourceID>,
    /// Name pattern in SQL `LIKE` format, per the ODF schema. A pattern with no
    /// wildcards *is* the exact-name case — the exact-vs-pattern distinction is
    /// the [`ResourceRef`](crate::ResourceRef)/`ResourceSelector` split, not a
    /// second field here.
    pub name: Option<String>,
    pub labels: Option<ResourceLabelFilterInput>,
}

/// Widens a manifest-authored ODF selector, whose `type` is required, into the
/// optional-type form. Destructured so a field added upstream breaks
/// compilation rather than being silently dropped.
impl From<odf::metadata::resource::ResourceSelector> for ResourceSelector {
    fn from(value: odf::metadata::resource::ResourceSelector) -> Self {
        let odf::metadata::resource::ResourceSelector {
            account,
            r#type,
            id,
            name,
            labels,
        } = value;

        Self {
            account,
            r#type: Some(r#type),
            id,
            name,
            labels,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
