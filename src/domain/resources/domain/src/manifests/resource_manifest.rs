// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{ResourceAccountRef, ResourceID, ResourceSchemaId, TypeRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceManifest {
    #[serde(rename = "$schema")]
    pub schema: ResourceSchemaId,
    pub headers: ResourceManifestHeaders,
    pub spec: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceManifestHeaders {
    pub id: Option<ResourceID>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<ResourceAccountRef>,
    pub name: String,
    pub description: Option<String>,
    #[serde(
        default,
        serialize_with = "serialize_entries",
        deserialize_with = "deserialize_entries"
    )]
    pub labels: Vec<(TypeRef, serde_json::Value)>,
    #[serde(
        default,
        serialize_with = "serialize_entries",
        deserialize_with = "deserialize_entries"
    )]
    pub annotations: Vec<(TypeRef, serde_json::Value)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn serialize_entries<S>(
    entries: &[(TypeRef, serde_json::Value)],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // Keep the Rust-side representation as an ordered list of pairs so
    // deserialization can report duplicate keys, but emit a normal manifest map
    // on the wire.
    let mut map = serializer.serialize_map(Some(entries.len()))?;

    for (key, value) in entries {
        map.serialize_entry(key, value)?;
    }

    map.end()
}

fn deserialize_entries<'de, D>(
    deserializer: D,
) -> Result<Vec<(TypeRef, serde_json::Value)>, D::Error>
where
    D: Deserializer<'de>,
{
    // Manifest files come from user-authored YAML/JSON, so we must detect duplicate
    // keys before converting into the storage/domain BTreeMap representation
    // that would hide them.
    struct EntriesVisitor;

    impl<'de> Visitor<'de> for EntriesVisitor {
        type Value = Vec<(TypeRef, serde_json::Value)>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a map or sequence of key/value entries")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Vec::new())
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Vec::new())
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut entries = Vec::new();
            let mut seen = BTreeSet::new();

            while let Some((key, value)) = map.next_entry::<TypeRef, serde_json::Value>()? {
                if !seen.insert(key.clone()) {
                    return Err(serde::de::Error::custom(format!(
                        "duplicate header key '{key}'"
                    )));
                }
                entries.push((key, value));
            }

            Ok(entries)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut entries = Vec::new();
            let mut seen = BTreeSet::new();

            while let Some((key, value)) = seq.next_element::<(TypeRef, serde_json::Value)>()? {
                if !seen.insert(key.clone()) {
                    return Err(serde::de::Error::custom(format!(
                        "duplicate header key '{key}'"
                    )));
                }
                entries.push((key, value));
            }

            Ok(entries)
        }
    }

    deserializer.deserialize_any(EntriesVisitor)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_matches;

    use super::*;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // `AccountRef`'s own serde contract (id/name/both, empty rejection) is
    // covered in ODF's `test_serde_account_ref`; this just confirms the field
    // is wired through `ResourceManifestHeaders`.
    #[test]
    fn deserializes_and_round_trips_account_field() {
        let id = odf::AccountID::new_generated_ed25519().1;
        let json = format!(
            r#"{{"id": null, "account": {{"id": "{id}"}}, "name": "n", "description": null, "labels": {{}}, "annotations": {{}}}}"#
        );

        let headers: ResourceManifestHeaders = serde_json::from_str(&json).unwrap();
        assert_matches!(&headers.account, Some(ResourceAccountRef::Id(actual)) if *actual == id);

        let round_tripped: ResourceManifestHeaders =
            serde_json::from_str(&serde_json::to_string(&headers).unwrap()).unwrap();
        assert_matches!(round_tripped.account, Some(ResourceAccountRef::Id(actual)) if actual == id);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn labels_and_annotations_accept_short_name_and_uri_keys_with_nested_values() {
        let json = r#"{
            "id": null, "account": null, "name": "n", "description": null,
            "labels": {
                "env": "prod",
                "https://opendatafabric.org/schemas/labels/v1/Team": {"name": "data-platform"}
            },
            "annotations": {
                "owner": "https://github.com/open-data-fabric"
            }
        }"#;

        let headers: ResourceManifestHeaders = serde_json::from_str(json).unwrap();
        assert_eq!(headers.labels.len(), 2);
        assert_eq!(headers.annotations.len(), 1);

        let round_tripped: ResourceManifestHeaders =
            serde_json::from_str(&serde_json::to_string(&headers).unwrap()).unwrap();
        assert_eq!(round_tripped.labels.len(), 2);
        assert_eq!(round_tripped.annotations.len(), 1);
    }

    #[test]
    fn rejects_duplicate_label_key_on_deserialize() {
        let json = r#"{
            "id": null, "account": null, "name": "n", "description": null,
            "labels": {"env": "prod"},
            "annotations": {}
        }"#;
        // Manually crafted with a duplicate `env` key — serde_json's `Value`
        // route collapses duplicates before we ever see them, so exercise the
        // visitor directly via a sequence form instead, which our custom
        // deserializer also accepts and does not collapse.
        let seq_json = r#"{
            "id": null, "account": null, "name": "n", "description": null,
            "labels": [["env", "prod"], ["env", "staging"]],
            "annotations": {}
        }"#;

        // Sanity: the map form above parses fine (no duplicates).
        let _: ResourceManifestHeaders = serde_json::from_str(json).unwrap();

        let err = serde_json::from_str::<ResourceManifestHeaders>(seq_json).unwrap_err();
        assert!(err.to_string().contains("duplicate header key 'env'"));
    }

    #[test]
    fn rejects_invalid_label_key_on_deserialize() {
        // Not a `https:`-prefixed URI and not a valid short type name grammar
        // (spaces are not allowed) — this is now a parse-time failure, not a
        // `ResourceHeadersValidationError::InvalidLabelKey`.
        let json = r#"{
            "id": null, "account": null, "name": "n", "description": null,
            "labels": {"not a valid key!": "prod"},
            "annotations": {}
        }"#;

        let err = serde_json::from_str::<ResourceManifestHeaders>(json).unwrap_err();
        // The error surfaces from `TypeRef`'s own `FromStr`/`Deserialize`.
        assert!(!err.to_string().is_empty());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
