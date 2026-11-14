// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indoc::indoc;
use opendatafabric_metadata::data::*;
use opendatafabric_metadata::serde::yaml as serde;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[rstest::rstest]
#[case(
    indoc!(
        r#"
        type:
          kind: Decimal
          precision: 10
          scale: 8
        "#
    ),
    DataType::decimal(10, 8),
)]
#[case(
    indoc!(
        r#"
        type:
          kind: Float64
        "#
    ),
    DataType::f64(),
)]
fn test_data_type_full_form(#[case] data: &str, #[case] expected: DataType) {
    #[derive(Debug, ::serde::Serialize, ::serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    struct Wrapper {
        r#type: serde::StructOrString<serde::data::DataType>,
    }

    let w = serde_yaml::from_str::<Wrapper>(data).unwrap();
    let val: DataType = w.r#type.try_into().unwrap();
    pretty_assertions::assert_eq!(val, expected);

    let w = Wrapper { r#type: val.into() };
    pretty_assertions::assert_eq!(serde_yaml::to_string(&w).unwrap(), data);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[rstest::rstest]
#[case(
    "type: float64",
    indoc!(
        r#"
        type:
          kind: Float64
        "#
    ),
    DataType::f64(),
)]
#[case(
    "type: float64?",
    indoc!(
        r#"
        type:
          kind: Option
          inner:
            kind: Float64
        "#
    ),
    DataType::f64().optional(),
)]
#[case(
    "type: Option<Float64>",
    indoc!(
        r#"
        type:
          kind: Option
          inner:
            kind: Float64
        "#
    ),
    DataType::f64().optional(),
)]
fn test_subject_short_form(#[case] short: &str, #[case] full: &str, #[case] expected: DataType) {
    #[derive(Debug, ::serde::Serialize, ::serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    struct Wrapper {
        r#type: serde::StructOrString<serde::data::DataType>,
    }

    let w = serde_yaml::from_str::<Wrapper>(short).unwrap();
    let val: DataType = w.r#type.try_into().unwrap();
    pretty_assertions::assert_eq!(val, expected);

    let w = Wrapper { r#type: val.into() };
    pretty_assertions::assert_eq!(serde_yaml::to_string(&w).unwrap(), full);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
