// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::dtos_generated::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataEncoding {
    ArrowContiguousBuffer { offset_bit_width: u32 },
    ArrowViewBuffer { offset_bit_width: u32 },
    ArrowRunEnd { run_ends_bit_width: u32 },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataEncoding {
    pub const ALL_KEYS: [&str; 3] = [
        Self::KEY_ARROW_ENCODING,
        Self::KEY_ARROW_OFFSET_BIT_WIDTH,
        Self::KEY_ARROW_RUN_ENDS_BIT_WIDTH,
    ];

    pub const KEY_ARROW_ENCODING: &str = "arrow.apache.org/encoding";
    pub const KEY_ARROW_OFFSET_BIT_WIDTH: &str = "arrow.apache.org/offsetBitWidth";
    pub const KEY_ARROW_RUN_ENDS_BIT_WIDTH: &str = "arrow.apache.org/offsetBitWidth";

    pub const VALUE_ARROW_ENCODING_CONTIGUOUS: &str = "contiguous";
    pub const VALUE_ARROW_ENCODING_VIEW: &str = "view";
    pub const VALUE_ARROW_ENCODING_RUN_END: &str = "runEnd";

    pub fn new_from_extra(extra: &ExtraAttributes) -> Option<Self> {
        match extra.get_str(Self::KEY_ARROW_ENCODING) {
            Some(Self::VALUE_ARROW_ENCODING_CONTIGUOUS) => {
                let offset_bit_width = extra
                    .get_and_parse(Self::KEY_ARROW_OFFSET_BIT_WIDTH)
                    .unwrap()
                    .unwrap_or(32);
                Some(DataEncoding::ArrowContiguousBuffer { offset_bit_width })
            }
            Some(Self::VALUE_ARROW_ENCODING_VIEW) => {
                let offset_bit_width = extra
                    .get_and_parse(Self::KEY_ARROW_OFFSET_BIT_WIDTH)
                    .unwrap()
                    .unwrap_or(32);
                Some(DataEncoding::ArrowViewBuffer { offset_bit_width })
            }
            Some(Self::VALUE_ARROW_ENCODING_RUN_END) => {
                let run_ends_bit_width = extra
                    .get_and_parse(Self::KEY_ARROW_RUN_ENDS_BIT_WIDTH)
                    .unwrap()
                    .unwrap_or(32);
                Some(DataEncoding::ArrowRunEnd { run_ends_bit_width })
            }
            None | Some(_) => None,
        }
    }

    pub fn to_extra(&self) -> Option<ExtraAttributes> {
        match self {
            DataEncoding::ArrowContiguousBuffer { offset_bit_width } => Some(
                ExtraAttributes::new_from_json(serde_json::json!({
                    Self::KEY_ARROW_ENCODING: Self::VALUE_ARROW_ENCODING_CONTIGUOUS,
                    Self::KEY_ARROW_OFFSET_BIT_WIDTH: offset_bit_width.to_string(),
                }))
                .unwrap(),
            ),
            DataEncoding::ArrowViewBuffer { offset_bit_width } => Some(
                ExtraAttributes::new_from_json(serde_json::json!({
                    Self::KEY_ARROW_ENCODING: Self::VALUE_ARROW_ENCODING_VIEW,
                    Self::KEY_ARROW_OFFSET_BIT_WIDTH: offset_bit_width.to_string(),
                }))
                .unwrap(),
            ),
            DataEncoding::ArrowRunEnd { run_ends_bit_width } => Some(
                ExtraAttributes::new_from_json(serde_json::json!({
                    Self::KEY_ARROW_ENCODING: Self::VALUE_ARROW_ENCODING_RUN_END,
                    Self::KEY_ARROW_RUN_ENDS_BIT_WIDTH: run_ends_bit_width.to_string(),
                }))
                .unwrap(),
            ),
        }
    }
}
