// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Map of [`crate::TypeRef`] keys to arbitrary JSON values that can be used to
/// organize, categorize, and query resources. Unlike [`ResourceAnnotations`],
/// labels are (eventually) indexed and queryable via selectors.
pub type ResourceLabels = odf::metadata::resource::ResourceLabels;

/// Map of [`crate::TypeRef`] keys to arbitrary JSON values, set by external
/// tools to store and retrieve arbitrary metadata. Unlike [`ResourceLabels`],
/// annotations are not indexed and cannot be queried by.
pub type ResourceAnnotations = odf::metadata::resource::ResourceAnnotations;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Decodes a [`ResourceLabels`] from a raw JSON value (e.g. a JSONB column),
/// going through the YAML shadow proxy since the ODF DTO has no direct
/// `Deserialize` of its own.
pub fn resource_labels_from_json(value: serde_json::Value) -> ResourceLabels {
    let proxy: odf::metadata::serde::yaml::resource::ResourceLabels =
        serde_json::from_value(value).unwrap();
    proxy.try_into().unwrap()
}

/// Decodes a [`ResourceAnnotations`] from a raw JSON value — see
/// [`resource_labels_from_json`] for why this goes through the proxy.
pub fn resource_annotations_from_json(value: serde_json::Value) -> ResourceAnnotations {
    let proxy: odf::metadata::serde::yaml::resource::ResourceAnnotations =
        serde_json::from_value(value).unwrap();
    proxy.try_into().unwrap()
}

/// Encodes a [`ResourceLabels`] into a raw JSON value (e.g. for a JSONB
/// column), going through the YAML shadow proxy since the ODF DTO has no
/// direct `Serialize` of its own.
pub fn resource_labels_to_json(labels: &ResourceLabels) -> serde_json::Value {
    let proxy: odf::metadata::serde::yaml::resource::ResourceLabels = labels.clone().into();
    serde_json::to_value(proxy).unwrap()
}

/// Encodes a [`ResourceAnnotations`] into a raw JSON value — see
/// [`resource_labels_to_json`] for why this goes through the proxy.
pub fn resource_annotations_to_json(annotations: &ResourceAnnotations) -> serde_json::Value {
    let proxy: odf::metadata::serde::yaml::resource::ResourceAnnotations =
        annotations.clone().into();
    serde_json::to_value(proxy).unwrap()
}

/// Extracts the top-level string-valued entries of `labels`, keyed by their
/// canonical string form. Used to maintain the `resource_labels_projection`
/// index — non-string values (numbers, objects, arrays, booleans, null) are
/// not indexed and are silently excluded.
pub fn string_label_entries(labels: &ResourceLabels) -> Vec<(String, String)> {
    labels
        .entries
        .iter()
        .filter_map(|(key, value)| Some((key.as_str().to_owned(), value.as_str()?.to_owned())))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
