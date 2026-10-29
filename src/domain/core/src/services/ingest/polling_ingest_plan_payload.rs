// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{ErrorIntoInternal, InternalError};
use serde::{Deserialize, Serialize};

use crate::{PollingIngestOptions, PollingIngestPlan};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Compact form of a polling ingest plan, stored before fetching to rebuild the
/// same plan later.
///
/// It names the head the plan was made at; metadata at a fixed head is
/// immutable, so planning at that head reproduces the plan. It holds no
/// environment variable values and no local paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename = "PollingIngestPlan", rename_all = "camelCase")]
pub struct PollingIngestPlanPayload {
    /// The root dataset
    pub dataset_id: odf::DatasetID,
    /// Head of the dataset the plan is based on
    pub expected_head: odf::Multihash,
    /// Hash of the `SetPollingSource` block in effect at `expected_head`
    pub source_block: odf::Multihash,
    /// Source state recorded by the last ingest
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_source_state: Option<PollingIngestPlanSourceState>,
    /// Offset of the last record
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_offset: Option<u64>,
    /// Watermark of the last ingest
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_watermark: Option<DateTime<Utc>>,
    /// Fetch a source even when it has no source state to compare with
    pub fetch_uncacheable: bool,
}

impl PollingIngestPlanPayload {
    pub fn from_plan(
        dataset_id: odf::DatasetID,
        plan: &PollingIngestPlan,
        options: &PollingIngestOptions,
    ) -> Result<Self, InternalError> {
        let state = plan.metadata_state();
        Ok(Self {
            dataset_id,
            expected_head: plan.expected_head().clone(),
            source_block: plan.source_block().clone(),
            previous_source_state: state
                .prev_source_state
                .as_ref()
                .map(PollingIngestPlanSourceState::try_from)
                .transpose()?,
            prev_offset: state.prev_offset,
            prev_watermark: state.prev_watermark,
            fetch_uncacheable: options.fetch_uncacheable,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PollingIngestPlanSourceState {
    pub kind: PollingIngestPlanSourceStateKind,
    pub value: String,
}

impl TryFrom<&odf::metadata::SourceState> for PollingIngestPlanSourceState {
    type Error = InternalError;

    fn try_from(state: &odf::metadata::SourceState) -> Result<Self, Self::Error> {
        let kind = if state.kind == odf::metadata::SourceState::KIND_ETAG {
            PollingIngestPlanSourceStateKind::ETag
        } else if state.kind == odf::metadata::SourceState::KIND_LAST_MODIFIED {
            PollingIngestPlanSourceStateKind::LastModified
        } else {
            return Err(format!("Unsupported polling source state kind {}", state.kind).int_err());
        };

        Ok(Self {
            kind,
            value: state.value.clone(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PollingIngestPlanSourceStateKind {
    ETag,
    LastModified,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
