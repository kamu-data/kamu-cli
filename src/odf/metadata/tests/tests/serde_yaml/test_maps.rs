// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use indoc::indoc;
use opendatafabric_metadata::config::*;
use opendatafabric_metadata::data::*;
use opendatafabric_metadata::serde::yaml as serde;
use opendatafabric_metadata::serde::yaml::IntoDto;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_maps() {
    // String -> Struct
    let data = indoc!(
        r#"
        password:
          value: swordfish
        tls:
          value: aabbcc
          contentEncoding: base64
        username:
          value: 'true'
        "#
    );

    let val = serde_yaml::from_str::<serde::config::Secrets>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    pretty_assertions::assert_eq!(
        val,
        Secrets {
            entries: BTreeMap::from_iter([
                (
                    "password".to_string(),
                    Secret {
                        value: "swordfish".into(),
                        content_encoding: None
                    }
                ),
                (
                    "username".to_string(),
                    Secret {
                        value: "true".into(),
                        content_encoding: None
                    }
                ),
                (
                    "tls".to_string(),
                    Secret {
                        value: "aabbcc".into(),
                        content_encoding: Some("base64".into())
                    }
                )
            ])
        }
    );

    pretty_assertions::assert_eq!(
        serde_yaml::to_string(&serde::config::Secrets::from(val)).unwrap(),
        data
    );

    // String -> Struct + FromString
    let data = indoc!(
        r#"
        password: swordfish
        "#
    );

    let val = serde_yaml::from_str::<serde::config::Secrets>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    pretty_assertions::assert_eq!(
        val,
        Secrets {
            entries: BTreeMap::from_iter([(
                "password".to_string(),
                Secret {
                    value: "swordfish".into(),
                    content_encoding: None
                }
            )])
        }
    );

    pretty_assertions::assert_eq!(
        serde_yaml::to_string(&serde::config::Secrets::from(val)).unwrap(),
        indoc!(
            r#"
            password:
              value: swordfish
            "#
        )
    );

    // String -> AnyJson
    let data = indoc!(
        r#"
        arrow.apache.org/offsetBitWidth: 32
        opendatafabric.org/description: foobar
        opendatafabric.org/type:
          kind: ObjectLink
          linkType:
            kind: Multihash
        "#
    );

    let val = serde_yaml::from_str::<serde::data::ExtraAttributes>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    pretty_assertions::assert_eq!(
        val,
        ExtraAttributes {
            entries: BTreeMap::from_iter([
                ("arrow.apache.org/offsetBitWidth".to_string(), json!(32)),
                (
                    "opendatafabric.org/description".to_string(),
                    json!("foobar")
                ),
                (
                    "opendatafabric.org/type".to_string(),
                    json!({
                        "kind": "ObjectLink",
                        "linkType": { "kind": "Multihash" }
                    })
                )
            ])
        }
    );

    pretty_assertions::assert_eq!(
        serde_yaml::to_string(&serde::data::ExtraAttributes::from(val)).unwrap(),
        data
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
