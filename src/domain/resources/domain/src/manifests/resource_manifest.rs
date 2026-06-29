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

use crate::{ResourceID, ResourceName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceManifest {
    pub api_version: String,
    pub kind: String,
    pub headers: ResourceManifestHeaders,
    pub spec: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceManifestHeaders {
    pub id: Option<ResourceID>,
    pub account: Option<ResourceManifestAccount>,
    pub name: ResourceName,
    pub description: Option<String>,
    #[serde(
        default,
        serialize_with = "serialize_string_entries",
        deserialize_with = "deserialize_string_entries"
    )]
    pub labels: Vec<(String, String)>,
    #[serde(
        default,
        serialize_with = "serialize_string_entries",
        deserialize_with = "deserialize_string_entries"
    )]
    pub annotations: Vec<(String, String)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceManifestAccount {
    pub name: Option<String>,
    pub id: Option<odf::AccountID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn serialize_string_entries<S>(
    entries: &[(String, String)],
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

fn deserialize_string_entries<'de, D>(deserializer: D) -> Result<Vec<(String, String)>, D::Error>
where
    D: Deserializer<'de>,
{
    // Manifest files come from user-authored YAML/JSON, so we must detect duplicate
    // keys before converting into the storage/domain BTreeMap representation
    // that would hide them.
    struct EntriesVisitor;

    impl<'de> Visitor<'de> for EntriesVisitor {
        type Value = Vec<(String, String)>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a map or sequence of string key/value entries")
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

            while let Some((key, value)) = map.next_entry::<String, String>()? {
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

            while let Some((key, value)) = seq.next_element::<(String, String)>()? {
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
