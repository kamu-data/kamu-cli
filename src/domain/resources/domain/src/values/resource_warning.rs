// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{ResourceExtensionKind, TypeRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const WARNING_CODE_MISSING_DESCRIPTION: &str = "missing_description";
pub const WARNING_CODE_RESOURCE_LABEL_NOT_INDEXED: &str = "resource_label_not_indexed";
pub const WARNING_CODE_RESOURCE_FREEFORM_LABELS: &str = "resource_freeform_labels";
pub const WARNING_CODE_RESOURCE_FREEFORM_ANNOTATIONS: &str = "resource_freeform_annotations";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceWarning {
    pub code: String,
    pub path: Option<String>,
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn resource_missing_description_warning() -> ResourceWarning {
    ResourceWarning {
        code: WARNING_CODE_MISSING_DESCRIPTION.to_string(),
        path: Some("headers.annotations.description".to_string()),
        message: "Resource has no description".to_string(),
    }
}

pub fn resource_label_not_indexed_warning(key: &TypeRef) -> ResourceWarning {
    ResourceWarning {
        code: WARNING_CODE_RESOURCE_LABEL_NOT_INDEXED.to_string(),
        path: Some(format!("headers.labels.{key}")),
        message: format!("Resource label '{key}' is not indexed because its value is not a string"),
    }
}

pub fn resource_free_form_extension_warning(
    kind: ResourceExtensionKind,
    mut keys: Vec<String>,
) -> Option<ResourceWarning> {
    if keys.is_empty() {
        return None;
    }

    keys.sort();

    let (code, category_name, path) = match kind {
        ResourceExtensionKind::Label => (
            WARNING_CODE_RESOURCE_FREEFORM_LABELS,
            "labels",
            "headers.labels",
        ),
        ResourceExtensionKind::Annotation => (
            WARNING_CODE_RESOURCE_FREEFORM_ANNOTATIONS,
            "annotations",
            "headers.annotations",
        ),
        ResourceExtensionKind::Condition => return None,
    };

    Some(ResourceWarning {
        code: code.to_string(),
        path: Some(path.to_string()),
        message: format!(
            "resource headers contain free-form {category_name} not backed by a registered \
             schema: {}",
            keys.join(", ")
        ),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
