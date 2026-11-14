// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use indoc::indoc;
use opendatafabric_metadata::data::*;
use opendatafabric_metadata::serde::yaml as serde;
use opendatafabric_metadata::serde::yaml::IntoDto;
use opendatafabric_metadata::sources::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_enum_tagging() {
    let value = ReadStep::NdJson(ReadStepNdJson {
        encoding: Some("utf-8".to_string()),
        ..Default::default()
    });

    // Check produces PascalCase tags on serialize
    assert_eq!(
        serde_yaml::to_string(&serde::sources::ReadStep::from(value.clone())).unwrap(),
        indoc::indoc!(
            "
            kind: NdJson
            encoding: utf-8
            "
        )
    );

    // Check deserialize with exact tag match
    let de_value: serde::sources::ReadStep = serde_yaml::from_str(indoc::indoc!(
        "
        kind: NdJson
        encoding: utf-8
        "
    ))
    .unwrap();
    assert_eq!(de_value.into_dto().unwrap(), value);

    // Check deserialize with old camelCase tag
    let de_value: serde::sources::ReadStep = serde_yaml::from_str(indoc::indoc!(
        "
        kind: ndJson
        encoding: utf-8
        "
    ))
    .unwrap();
    assert_eq!(de_value.into_dto().unwrap(), value);

    // Check deserialize with lowercase tag
    let de_value: serde::sources::ReadStep = serde_yaml::from_str(indoc::indoc!(
        "
        kind: ndjson
        encoding: utf-8
        "
    ))
    .unwrap();
    assert_eq!(de_value.into_dto().unwrap(), value);

    // Check rejects other case permutations
    serde_yaml::from_str::<serde::sources::ReadStep>(indoc::indoc!(
        "
        kind: nDjson
        encoding: utf-8
        "
    ))
    .map(serde::IntoDto::into_dto)
    .expect_err("Should fail");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_string_enum_names() {
    let value = SourceOrdering::ByEventTime;

    // Check produces PascalCase values on serialize as in spec schemas
    assert_eq!(
        serde_yaml::to_string(&serde::sources::SourceOrdering::from(value)).unwrap(),
        indoc::indoc!(
            "
            ByEventTime
            "
        )
    );

    // Can deserialize from camelCase
    let de_value: serde::sources::SourceOrdering = serde_yaml::from_str(indoc::indoc!(
        "
        byEventTime
        "
    ))
    .unwrap();
    assert_eq!(de_value.into_dto().unwrap(), value);

    // Can deserialize from lowercase
    let de_value: serde::sources::SourceOrdering = serde_yaml::from_str(indoc::indoc!(
        "
        byeventtime
        "
    ))
    .unwrap();
    assert_eq!(de_value.into_dto().unwrap(), value);

    // Check rejects other case permutations
    serde_yaml::from_str::<serde::sources::SourceOrdering>(indoc::indoc!(
        "
        bYeventtime
        "
    ))
    .map(serde::IntoDto::into_dto)
    .expect_err("Should fail");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_enum_short_form() {
    let data = indoc!(
        "
        fields:
        - name: long
          type:
            kind: Timestamp
        - name: short
          type: Timestamp
        - name: full
          type:
            kind: Timestamp
            unit: Nanosecond
            timezone: UTC
        "
    );

    // Deserializes fine
    let actual = serde_yaml::from_str::<serde::data::DataSchema>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    let expected = DataSchema::new(vec![
        DataField::new(
            "long",
            DataTypeTimestamp {
                unit: None,
                timezone: None,
            },
        ),
        DataField::new(
            "short",
            DataTypeTimestamp {
                unit: None,
                timezone: None,
            },
        ),
        DataField::new(
            "full",
            DataTypeTimestamp {
                unit: Some(TimeUnit::Nanosecond),
                timezone: Some("UTC".to_owned()),
            },
        ),
    ]);

    pretty_assertions::assert_eq!(expected, actual);

    // Serialization expands to long form, but does not include defaults
    pretty_assertions::assert_eq!(
        serde_yaml::to_string(&serde::data::DataSchema::from(actual)).unwrap(),
        indoc!(
            "
            fields:
            - name: long
              type:
                kind: Timestamp
            - name: short
              type:
                kind: Timestamp
            - name: full
              type:
                kind: Timestamp
                unit: Nanosecond
                timezone: UTC
            "
        )
    );

    // Short form correctly handles invalid types
    let data = indoc!(
        "
        fields:
        - name: short
          type: Invalid
        "
    );

    let res = serde_yaml::from_str::<serde::data::DataSchema>(data).map(serde::IntoDto::into_dto);
    assert_matches!(res, Err(e) if e.to_string().contains("Value 'Invalid' is not a valid DataType"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
