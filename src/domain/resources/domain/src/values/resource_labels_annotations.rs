// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Queryable [`crate::TypeRef`]-keyed resource metadata.
pub type ResourceLabels = odf::metadata::resource::ResourceLabels;

/// Non-queryable [`crate::TypeRef`]-keyed resource metadata.
pub type ResourceAnnotations = odf::metadata::resource::ResourceAnnotations;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Decodes [`ResourceLabels`] through the ODF YAML shadow proxy.
pub fn resource_labels_from_json(value: serde_json::Value) -> ResourceLabels {
    let proxy: odf::metadata::serde::yaml::resource::ResourceLabels =
        serde_json::from_value(value).unwrap();
    proxy.try_into().unwrap()
}

/// Decodes [`ResourceAnnotations`] through the ODF YAML shadow proxy.
pub fn resource_annotations_from_json(value: serde_json::Value) -> ResourceAnnotations {
    let proxy: odf::metadata::serde::yaml::resource::ResourceAnnotations =
        serde_json::from_value(value).unwrap();
    proxy.try_into().unwrap()
}

/// Encodes [`ResourceLabels`] through the ODF YAML shadow proxy.
pub fn resource_labels_to_json(labels: &ResourceLabels) -> serde_json::Value {
    let proxy: odf::metadata::serde::yaml::resource::ResourceLabels = labels.clone().into();
    serde_json::to_value(proxy).unwrap()
}

/// Encodes [`ResourceAnnotations`] through the ODF YAML shadow proxy.
pub fn resource_annotations_to_json(annotations: &ResourceAnnotations) -> serde_json::Value {
    let proxy: odf::metadata::serde::yaml::resource::ResourceAnnotations =
        annotations.clone().into();
    serde_json::to_value(proxy).unwrap()
}

/// Extracts top-level string label entries for projection indexing.
pub fn string_label_entries(labels: &ResourceLabels) -> Vec<(String, String)> {
    labels
        .entries
        .iter()
        .filter_map(|(key, value)| Some((key.as_str().to_owned(), value.as_str()?.to_owned())))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
