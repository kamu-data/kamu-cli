// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::engine::{TransformRequestExt, TransformRequestInputExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Compact form of an elaborated transform, stored to rebuild the same request
/// later.
///
/// It names the heads and the system time the request was elaborated at,
/// instead of copying the request: metadata at a fixed head is immutable, so
/// elaborating at these pins reproduces the request. It holds no data,
/// checkpoint or staging paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename = "TransformPlan", rename_all = "camelCase")]
pub struct TransformPlanPayload {
    /// The derivative dataset
    pub dataset_id: odf::DatasetID,
    /// System time of the new records
    pub system_time: DateTime<Utc>,
    /// Head of the dataset the plan is based on
    pub expected_head: odf::Multihash,
    /// Hash of the `SetTransform` block in effect at `expected_head`
    pub transform_block: odf::Multihash,
    /// Position of each input, in `SetTransform` order
    pub query_inputs: Vec<TransformPlanPayloadInput>,
}

impl TransformPlanPayload {
    pub fn from_request(request: &TransformRequestExt) -> Self {
        Self {
            dataset_id: request.dataset_handle.id.clone(),
            system_time: request.system_time,
            expected_head: request.head.clone(),
            transform_block: request.transform_block.clone(),
            query_inputs: request
                .inputs
                .iter()
                .map(TransformPlanPayloadInput::from_request_input)
                .collect(),
        }
    }

    /// Input heads the plan was elaborated at, keyed by input
    pub fn input_heads(&self) -> HashMap<odf::DatasetID, odf::Multihash> {
        self.query_inputs
            .iter()
            .map(|input| (input.dataset_id.clone(), input.planned_head.clone()))
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Position of one input in a stored plan
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransformPlanPayloadInput {
    pub dataset_id: odf::DatasetID,
    /// Input head the plan was computed against
    pub planned_head: odf::Multihash,
    /// Records of the input the transform consumes, if any
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_interval: Option<TransformPlanOffsetInterval>,
    /// Last watermark among the consumed input blocks, if any
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark: Option<DateTime<Utc>>,
}

impl TransformPlanPayloadInput {
    fn from_request_input(input: &TransformRequestInputExt) -> Self {
        Self {
            dataset_id: input.dataset_handle.id.clone(),
            // Elaboration fails on an input without blocks, so one of the two is set
            planned_head: input
                .new_block_hash
                .as_ref()
                .or(input.prev_block_hash.as_ref())
                .expect("Elaborated input has neither a new nor a previous block")
                .clone(),
            offset_interval: input.new_offset.map(|end| TransformPlanOffsetInterval {
                start: input.prev_offset.map_or(0, |prev| prev + 1),
                end,
            }),
            watermark: input.explicit_watermarks.last().map(|wm| wm.event_time),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Closed interval of record offsets
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransformPlanOffsetInterval {
    pub start: u64,
    pub end: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
