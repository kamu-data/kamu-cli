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

impl ResourceSelector {
    /// Every resource of `r#type`.
    pub fn of_type(r#type: TypeRef) -> Self {
        Self {
            r#type: Some(r#type),
            ..Default::default()
        }
    }

    /// Resources of `r#type` whose names match a SQL `LIKE` pattern.
    pub fn name_pattern(r#type: TypeRef, pattern: impl Into<String>) -> Self {
        Self {
            name: Some(pattern.into()),
            ..Self::of_type(r#type)
        }
    }

    /// One resource of `r#type`, by id.
    pub fn id_of_type(r#type: TypeRef, id: ResourceID) -> Self {
        Self {
            id: Some(id),
            ..Self::of_type(r#type)
        }
    }

    /// One resource of any type, by id.
    pub fn any_type_id(id: ResourceID) -> Self {
        Self {
            id: Some(id),
            ..Default::default()
        }
    }

    /// Resources of any type whose names match a SQL `LIKE` pattern.
    pub fn any_type_name_pattern(pattern: impl Into<String>) -> Self {
        Self {
            name: Some(pattern.into()),
            ..Default::default()
        }
    }

    /// One selector per id, all of `r#type`.
    ///
    /// The wire is scalar, so a batch of ids fans out here and is folded back
    /// into a single `ExactIds` row by the facade's coalescer.
    pub fn ids_of_type(r#type: &TypeRef, ids: impl IntoIterator<Item = ResourceID>) -> Vec<Self> {
        ids.into_iter()
            .map(|id| Self::id_of_type(r#type.clone(), id))
            .collect()
    }

    /// One selector per id, spanning every type.
    pub fn any_type_ids(ids: impl IntoIterator<Item = ResourceID>) -> Vec<Self> {
        ids.into_iter().map(Self::any_type_id).collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
