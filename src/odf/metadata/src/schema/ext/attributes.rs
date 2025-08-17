// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::DataTypeExt;
use crate::Attribute;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttrDescription {
    #[serde(rename = "opendatafabric.org/description")]
    pub description: String,
}

impl Attribute for AttrDescription {
    const KEYS: &[&str] = &["opendatafabric.org/description"];
}

impl AttrDescription {
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttrType {
    #[serde(rename = "opendatafabric.org/type")]
    pub r#type: DataTypeExt,
}

impl Attribute for AttrType {
    const KEYS: &[&str] = &["opendatafabric.org/type"];
}

impl AttrType {
    pub fn new(r#type: DataTypeExt) -> Self {
        Self { r#type }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttrArchetype {
    #[serde(rename = "kamu.dev/archetype")]
    pub archetype: DatasetArchetype,
}

impl Attribute for AttrArchetype {
    const KEYS: &[&str] = &["kamu.dev/archetype"];
}

impl AttrArchetype {
    pub fn new(archetype: DatasetArchetype) -> Self {
        Self { archetype }
    }
}

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub enum DatasetArchetype {
    #[serde(alias = "collection")]
    Collection,

    #[serde(alias = "versionedfile", alias = "versionedFile")]
    VersionedFile,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use std::assert_matches::assert_matches;

    use serde_json::json;

    use super::*;
    use crate::ExtraAttributes;

    #[test]
    fn test_attr_description() {
        let attr = AttrDescription::new("Test");

        let json = json!({
            "opendatafabric.org/description": "Test",
        });

        let mut attrs = ExtraAttributes::new();
        attrs.insert(&attr);

        pretty_assertions::assert_eq!(attrs.clone().into_json(), json);

        let attrs2 = ExtraAttributes::new_from_json(json).unwrap();
        let attr2 = attrs2.get::<AttrDescription>().unwrap();

        assert_eq!(attr2, Some(attr));
    }

    #[test]
    fn test_attr_type_extended() {
        let attr = AttrType::new(DataTypeExt::object_link(DataTypeExt::multihash()));

        let json = json!({
            "opendatafabric.org/type": {
                "kind": "ObjectLink",
                "linkType": {
                    "kind": "Multihash",
                },
            }
        });

        let mut attrs = ExtraAttributes::new();
        attrs.insert(&attr);

        pretty_assertions::assert_eq!(attrs.clone().into_json(), json);

        let attrs2 = ExtraAttributes::new_from_json(json).unwrap();
        let attr2 = attrs2.get::<AttrType>().unwrap();

        assert_eq!(attr2, Some(attr));

        let attrs = ExtraAttributes::new_from_json(json!({
            "opendatafabric.org/type": {
                "kind": "Foobar",
            }
        }))
        .unwrap();
        assert_matches!(attrs.get::<AttrType>(), Err(_));
    }

    #[test]
    fn test_attr_type_core() {
        let attr = AttrType::new(DataTypeExt::core(crate::DataType::string()));

        let json = json!({
            "opendatafabric.org/type": {
                "kind": "String",
            }
        });

        let mut attrs = ExtraAttributes::new();
        attrs.insert(&attr);

        pretty_assertions::assert_eq!(attrs.clone().into_json(), json);

        let attrs2 = ExtraAttributes::new_from_json(json).unwrap();
        let attr2 = attrs2.get::<AttrType>().unwrap();

        assert_eq!(attr2, Some(attr));

        let attrs = ExtraAttributes::new_from_json(json!({
            "opendatafabric.org/type": {
                "kind": "Foobar",
            }
        }))
        .unwrap();
        assert_matches!(attrs.get::<AttrType>(), Err(_));
    }

    #[test]
    fn test_attr_archetype() {
        let desc = AttrArchetype::new(DatasetArchetype::Collection);
        let mut attrs = ExtraAttributes::new();
        attrs.insert(&desc);

        pretty_assertions::assert_eq!(
            attrs.clone().into_json(),
            json!({
                "kamu.dev/archetype": "Collection",
            })
        );

        let attrs2 = ExtraAttributes::new_from_json(attrs.into_json()).unwrap();
        let desc2 = attrs2.get::<AttrArchetype>().unwrap();

        assert_eq!(desc2, Some(desc));

        let attrs = ExtraAttributes::new_from_json(json!({
            "kamu.dev/archetype": "Foobar",
        }))
        .unwrap();
        assert_matches!(attrs.get::<AttrArchetype>(), Err(_));
    }
}
