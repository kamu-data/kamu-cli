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
        let kind = state
            .kind
            .parse::<PollingIngestPlanSourceStateKind>()
            .map_err(|_| {
                format!("Unsupported polling source state kind {}", state.kind).int_err()
            })?;

        Ok(Self {
            kind,
            value: state.value.clone(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Kinds of `odf::metadata::SourceState` a polling ingest plan can carry.
/// Their strings are the `odf::metadata::SourceState::KIND_*` constants.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
pub enum PollingIngestPlanSourceStateKind {
    #[strum(serialize = "odf/etag")]
    ETag,
    #[strum(serialize = "odf/last-modified")]
    LastModified,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;
    use strum::IntoEnumIterator;

    use super::*;

    #[test]
    fn test_source_state_kind_strings_are_odf_constants() {
        let kinds = [
            (
                PollingIngestPlanSourceStateKind::ETag,
                odf::metadata::SourceState::KIND_ETAG,
            ),
            (
                PollingIngestPlanSourceStateKind::LastModified,
                odf::metadata::SourceState::KIND_LAST_MODIFIED,
            ),
        ];

        assert_eq!(
            kinds.len(),
            PollingIngestPlanSourceStateKind::iter().count()
        );
        for (kind, odf_kind) in kinds {
            assert_eq!(<&'static str>::from(kind), odf_kind);
            assert_eq!(
                odf_kind.parse::<PollingIngestPlanSourceStateKind>(),
                Ok(kind)
            );
        }
    }

    #[test]
    fn test_plan_payload_json() {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
        let head = odf::Multihash::from_digest_sha3_256(b"head");
        let source_block = odf::Multihash::from_digest_sha3_256(b"source");
        let payload = PollingIngestPlanPayload {
            dataset_id: dataset_id.clone(),
            expected_head: head.clone(),
            source_block: source_block.clone(),
            previous_source_state: Some(PollingIngestPlanSourceState {
                kind: PollingIngestPlanSourceStateKind::ETag,
                value: "abc".to_string(),
            }),
            prev_offset: Some(9),
            prev_watermark: Some(Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap()),
            fetch_uncacheable: true,
        };

        let value = serde_json::to_value(&payload).unwrap();

        assert_eq!(
            value,
            json!({
                "kind": "PollingIngestPlan",
                "datasetId": dataset_id.to_string(),
                "expectedHead": head.to_string(),
                "sourceBlock": source_block.to_string(),
                "previousSourceState": { "kind": "ETag", "value": "abc" },
                "prevOffset": 9,
                "prevWatermark": "2050-01-01T00:00:00Z",
                "fetchUncacheable": true,
            })
        );
        assert_eq!(
            serde_json::from_value::<PollingIngestPlanPayload>(value).unwrap(),
            payload
        );
    }

    #[test]
    fn test_plan_payload_json_omits_absent_fields() {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
        let head = odf::Multihash::from_digest_sha3_256(b"head");
        let payload = PollingIngestPlanPayload {
            dataset_id: dataset_id.clone(),
            expected_head: head.clone(),
            source_block: head.clone(),
            previous_source_state: None,
            prev_offset: None,
            prev_watermark: None,
            fetch_uncacheable: false,
        };

        let value = serde_json::to_value(&payload).unwrap();

        assert_eq!(
            value,
            json!({
                "kind": "PollingIngestPlan",
                "datasetId": dataset_id.to_string(),
                "expectedHead": head.to_string(),
                "sourceBlock": head.to_string(),
                "fetchUncacheable": false,
            })
        );
        assert_eq!(
            serde_json::from_value::<PollingIngestPlanPayload>(value).unwrap(),
            payload
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
