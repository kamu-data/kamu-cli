// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use crate::{RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI, TypeRef, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn description_annotation_type_ref() -> TypeRef {
    TypeRef::Uri(TypeUri::new_unchecked(
        RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI,
    ))
}

pub fn get_description(annotations: &BTreeMap<TypeRef, serde_json::Value>) -> Option<&str> {
    annotations
        .get(&description_annotation_type_ref())
        .and_then(serde_json::Value::as_str)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
