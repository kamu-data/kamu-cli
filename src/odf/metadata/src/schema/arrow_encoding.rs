// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema_impl::UnsupportedSchema;
use crate::dtos::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArrowEncoding {
    #[serde(rename = "arrow.apache.org/bufferEncoding")]
    #[serde(default)]
    pub buffer: Option<ArrowBufferEncoding>,

    #[serde(rename = "arrow.apache.org/dateEncoding")]
    #[serde(default)]
    pub date: Option<ArrowDateEncoding>,

    #[serde(rename = "arrow.apache.org/decimalEncoding")]
    #[serde(default)]
    pub decimal: Option<ArrowDecimalEncoding>,
}

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind")]
#[serde(deny_unknown_fields)]
pub enum ArrowBufferEncoding {
    #[serde(alias = "contiguous")]
    #[serde(rename_all = "camelCase")]
    Contiguous { offset_bit_width: Option<u32> },
    #[serde(alias = "view")]
    #[serde(rename_all = "camelCase")]
    View {
        #[serde(default)]
        offset_bit_width: Option<u32>,
    },
    #[serde(alias = "runend", alias = "runEnd")]
    #[serde(rename_all = "camelCase")]
    RunEnd {
        #[serde(default)]
        run_ends_bit_width: Option<u32>,
    },
}

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ArrowDateEncoding {
    pub unit: ArrowDateUnit,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ArrowDateUnit {
    #[serde(alias = "day")]
    Day,
    #[serde(alias = "millisecond")]
    Millisecond,
}

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ArrowDecimalEncoding {
    pub bit_width: u32,
}

impl ArrowDecimalEncoding {
    pub const MAX_PRECISION_128: u32 = 39;
    pub const MAX_PRECISION_256: u32 = 77;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ArrowEncoding {
    pub const KEY_ARROW_BUFFER_ENCODING: &str = "arrow.apache.org/bufferEncoding";
    pub const KEY_ARROW_DATE_ENCODING: &str = "arrow.apache.org/dateEncoding";
    pub const KEY_ARROW_DECIMAL_ENCODING: &str = "arrow.apache.org/decimalEncoding";

    pub const ALL_KEYS: [&str; 3] = [
        Self::KEY_ARROW_BUFFER_ENCODING,
        Self::KEY_ARROW_DATE_ENCODING,
        Self::KEY_ARROW_DECIMAL_ENCODING,
    ];

    pub fn deserialize_from(
        attrs: &ExtraAttributes,
    ) -> Result<Option<ArrowEncoding>, UnsupportedSchema> {
        if !Self::ALL_KEYS.iter().any(|k| attrs.contains_key(k)) {
            return Ok(None);
        }

        let enc = attrs
            .deserialize_as::<Self>()
            .map_err(|e| UnsupportedSchema::new(format!("Invalid encoding: {e}")))?;

        Ok(Some(enc))
    }
}

impl From<ArrowBufferEncoding> for ArrowEncoding {
    fn from(value: ArrowBufferEncoding) -> Self {
        Self {
            buffer: Some(value),
            ..Default::default()
        }
    }
}

impl From<ArrowDateEncoding> for ArrowEncoding {
    fn from(value: ArrowDateEncoding) -> Self {
        Self {
            date: Some(value),
            ..Default::default()
        }
    }
}

impl From<ArrowDecimalEncoding> for ArrowEncoding {
    fn from(value: ArrowDecimalEncoding) -> Self {
        Self {
            decimal: Some(value),
            ..Default::default()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::*;
    use crate::ExtraAttributes;

    #[test]
    fn test_arrow_encoding_empty() {
        let attrs = ExtraAttributes::new_from_json(json!({})).unwrap();

        let arrow_encoding = ArrowEncoding::deserialize_from(&attrs).unwrap();

        pretty_assertions::assert_eq!(arrow_encoding, None);
    }

    #[test]
    fn test_arrow_encoding_irrelevant() {
        let attrs = ExtraAttributes::new_from_json(json!({
            "foo.com/foo": "x",
            "foo.com/bar": "y",
        }))
        .unwrap();

        let arrow_encoding = ArrowEncoding::deserialize_from(&attrs).unwrap();

        pretty_assertions::assert_eq!(arrow_encoding, None);
    }

    #[test]
    fn test_arrow_encoding_valid_buffer() {
        let attrs = ExtraAttributes::new_from_json(json!({
            "arrow.apache.org/bufferEncoding": {
                "kind": "contiguous",
                "offsetBitWidth": 64,
            },
            "foo.com/offsetBitWidth": "blerb",
        }))
        .unwrap();

        let arrow_encoding = ArrowEncoding::deserialize_from(&attrs).unwrap();

        pretty_assertions::assert_eq!(
            arrow_encoding,
            Some(ArrowEncoding {
                buffer: Some(ArrowBufferEncoding::Contiguous {
                    offset_bit_width: Some(64)
                }),
                ..Default::default()
            })
        );

        let mut attrs = ExtraAttributes::new();
        attrs.merge_serialized(&arrow_encoding);
        pretty_assertions::assert_eq!(
            attrs.into_json(),
            json!({
                "arrow.apache.org/bufferEncoding": {
                    "kind": "Contiguous",
                    "offsetBitWidth": 64,
                }
            }),
        );
    }

    #[test]
    fn test_arrow_encoding_valid_run_end() {
        let attrs = ExtraAttributes::new_from_json(json!({
            "arrow.apache.org/bufferEncoding": {
                "kind": "runEnd",
                "runEndsBitWidth": 32,
            },
            "foo.com/runEndsBitWidth": "blerb",
        }))
        .unwrap();

        let arrow_encoding = ArrowEncoding::deserialize_from(&attrs).unwrap();

        pretty_assertions::assert_eq!(
            arrow_encoding,
            Some(ArrowEncoding {
                buffer: Some(ArrowBufferEncoding::RunEnd {
                    run_ends_bit_width: Some(32)
                }),
                ..Default::default()
            })
        );

        let mut attrs = ExtraAttributes::new();
        attrs.merge_serialized(&arrow_encoding);
        pretty_assertions::assert_eq!(
            attrs.into_json(),
            json!({
                "arrow.apache.org/bufferEncoding": {
                    "kind": "RunEnd",
                    "runEndsBitWidth": 32,
                }
            }),
        );
    }

    #[test]
    fn test_arrow_encoding_valid_date() {
        let attrs = ExtraAttributes::new_from_json(json!({
            "arrow.apache.org/dateEncoding": {
                "unit": "millisecond",
            },
            "foo.com/runEndsBitWidth": "blerb",
        }))
        .unwrap();

        let arrow_encoding = ArrowEncoding::deserialize_from(&attrs).unwrap();

        pretty_assertions::assert_eq!(
            arrow_encoding,
            Some(ArrowEncoding {
                date: Some(ArrowDateEncoding {
                    unit: ArrowDateUnit::Millisecond
                }),
                ..Default::default()
            })
        );

        let mut attrs = ExtraAttributes::new();
        attrs.merge_serialized(&arrow_encoding);
        pretty_assertions::assert_eq!(
            attrs.into_json(),
            json!({
                "arrow.apache.org/dateEncoding": {
                    "unit": "Millisecond",
                },
            }),
        );
    }

    #[test]
    fn test_arrow_encoding_valid_decimal() {
        let attrs = ExtraAttributes::new_from_json(json!({
            "arrow.apache.org/decimalEncoding": {
                "bitWidth": 128,
            },
            "foo.com/runEndsBitWidth": "blerb",
        }))
        .unwrap();

        let arrow_encoding = ArrowEncoding::deserialize_from(&attrs).unwrap();

        pretty_assertions::assert_eq!(
            arrow_encoding,
            Some(ArrowEncoding {
                decimal: Some(ArrowDecimalEncoding { bit_width: 128 }),
                ..Default::default()
            })
        );

        let mut attrs = ExtraAttributes::new();
        attrs.merge_serialized(&arrow_encoding);
        pretty_assertions::assert_eq!(
            attrs.into_json(),
            json!({
                "arrow.apache.org/decimalEncoding": {
                    "bitWidth": 128,
                },
            }),
        );
    }

    #[test]
    fn test_arrow_encoding_invalid() {
        let attrs = ExtraAttributes::new_from_json(json!({
            "arrow.apache.org/bufferEncoding": "blerb",
        }))
        .unwrap();

        std::assert_matches::assert_matches!(ArrowEncoding::deserialize_from(&attrs), Err(_));
    }
}
