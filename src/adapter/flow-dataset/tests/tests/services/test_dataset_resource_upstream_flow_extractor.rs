// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_adapter_flow_dataset::*;
use kamu_flow_system as fs;
use kamu_flow_system::FlowActivationCauseUpstreamExtractor;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_upstream_flow_source_yields_flow_id() {
    let harness = ExtractorHarness::new();

    let update = harness.dataset_update(DatasetUpdateSource::UpstreamFlow {
        flow_type: FLOW_TYPE_DATASET_INGEST.to_string(),
        flow_id: fs::FlowID::new(42),
        maybe_flow_config_snapshot: None,
    });

    assert_eq!(harness.extract(&update), Some(fs::FlowID::new(42)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_sources_without_flow_yield_none() {
    let harness = ExtractorHarness::new();

    for source in [
        DatasetUpdateSource::HttpIngest {
            source_name: Some("src".to_string()),
        },
        DatasetUpdateSource::SmartProtocolPush {
            account_name: None,
            is_force: false,
        },
        DatasetUpdateSource::ExternallyDetectedChange,
    ] {
        let update = harness.dataset_update(source.clone());
        assert_eq!(harness.extract(&update), None, "{source:?}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_unreadable_details_yield_none() {
    let harness = ExtractorHarness::new();

    let update = harness.resource_update(serde_json::json!({ "unexpected": "shape" }));

    assert_eq!(harness.extract(&update), None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ExtractorHarness {
    extractor: DatasetResourceUpstreamFlowExtractor,
}

impl ExtractorHarness {
    fn new() -> Self {
        Self {
            extractor: DatasetResourceUpstreamFlowExtractor {},
        }
    }

    fn extract(&self, update: &fs::FlowActivationCauseResourceUpdate) -> Option<fs::FlowID> {
        assert_eq!(self.extractor.resource_type(), DATASET_RESOURCE_TYPE);
        self.extractor.extract_upstream_flow_id(update)
    }

    fn dataset_update(&self, source: DatasetUpdateSource) -> fs::FlowActivationCauseResourceUpdate {
        self.resource_update(
            serde_json::to_value(DatasetResourceUpdateDetails {
                dataset_id: odf::DatasetID::new_seeded_ed25519(b"foo"),
                source,
                old_head_maybe: None,
                new_head: odf::Multihash::from_digest_sha3_256(b"head"),
            })
            .unwrap(),
        )
    }

    fn resource_update(&self, details: serde_json::Value) -> fs::FlowActivationCauseResourceUpdate {
        fs::FlowActivationCauseResourceUpdate {
            activation_time: Utc::now(),
            changes: fs::ResourceChanges::NewData(fs::ResourceDataChanges::default()),
            resource_type: DATASET_RESOURCE_TYPE.to_string(),
            details,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
