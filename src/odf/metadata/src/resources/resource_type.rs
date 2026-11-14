// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::{TypeRef, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A resource type known at compile time, which parameterizes typed links such
/// as [`ResourceRefT`](super::ResourceRefT) and
/// [`ResourceHandleT`](super::ResourceHandleT).
pub trait ResourceType {
    /// Full schema URI of the type, e.g.
    /// `https://opendatafabric.org/schemas/auth/v1alpha1/Group`.
    const SCHEMA_STR: &'static str;

    fn type_uri() -> TypeUri {
        TypeUri::new_unchecked(Self::SCHEMA_STR)
    }

    /// Short type name: the last path segment of the schema URI, e.g. `Group`.
    fn type_name() -> &'static str {
        Self::SCHEMA_STR
            .rsplit_once('/')
            .map_or(Self::SCHEMA_STR, |(_, name)| name)
    }

    /// Whether `r#type` names this type. A URI must match exactly, a short
    /// name matches ignoring ASCII case, as the short forms accept both.
    fn matches_type_ref(r#type: &TypeRef) -> bool {
        match r#type {
            TypeRef::Uri(uri) => uri.as_str() == Self::SCHEMA_STR,
            TypeRef::Name(name) => name.as_str().eq_ignore_ascii_case(Self::type_name()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
