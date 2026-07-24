// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use crate::{
    RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI,
    RESOURCE_ANNOTATION_DESCRIPTION_SHORT_NAME,
    TypeName,
    TypeRef,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn description_annotation_type_ref() -> TypeRef {
    TypeRef::Uri(TypeUri::new_unchecked(
        RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI,
    ))
}

pub fn description_annotation_short_name_type_ref() -> TypeRef {
    TypeRef::Name(TypeName::new_unchecked(
        RESOURCE_ANNOTATION_DESCRIPTION_SHORT_NAME,
    ))
}

/// Looks up the description under either the canonical schema URI key or the
/// short alias — there is no short-name-to-URI normalization mechanism yet,
/// so both are treated as equivalent identities for this annotation. This is
/// a temporary accommodation; future well-known annotations should not copy
/// this dual-key pattern once normalization exists.
pub fn get_description(annotations: &BTreeMap<TypeRef, serde_json::Value>) -> Option<&str> {
    annotations
        .get(&description_annotation_type_ref())
        .or_else(|| annotations.get(&description_annotation_short_name_type_ref()))
        .and_then(serde_json::Value::as_str)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
