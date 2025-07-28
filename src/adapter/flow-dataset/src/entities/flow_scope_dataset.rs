// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system as fs;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowScopeDataset<'a>(&'a fs::FlowScope);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FLOW_SCOPE_TYPE_DATASET: &str = "Dataset";

pub const FLOW_SCOPE_ATTRIBUTE_DATASET_ID: &str = "dataset_id";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'a> FlowScopeDataset<'a> {
    pub fn new(scope: &'a fs::FlowScope) -> Self {
        assert_eq!(scope.scope_type(), FLOW_SCOPE_TYPE_DATASET);
        FlowScopeDataset(scope)
    }

    pub fn make_scope(dataset_id: &odf::DatasetID) -> fs::FlowScope {
        let payload = serde_json::json!({
            fs::FLOW_SCOPE_ATTRIBUTE_TYPE: FLOW_SCOPE_TYPE_DATASET,
            FLOW_SCOPE_ATTRIBUTE_DATASET_ID: dataset_id.to_string(),
        });
        fs::FlowScope::new(payload)
    }

    pub fn query_for_single_dataset(dataset_id: &odf::DatasetID) -> fs::FlowScopeQuery {
        fs::FlowScopeQuery {
            attributes: vec![(
                FLOW_SCOPE_ATTRIBUTE_DATASET_ID.to_string(),
                vec![dataset_id.to_string()],
            )],
        }
    }

    pub fn query_for_multiple_datasets(dataset_ids: &[&odf::DatasetID]) -> fs::FlowScopeQuery {
        fs::FlowScopeQuery {
            attributes: vec![(
                FLOW_SCOPE_ATTRIBUTE_DATASET_ID.to_string(),
                dataset_ids.iter().map(ToString::to_string).collect(),
            )],
        }
    }

    pub fn dataset_id(&self) -> odf::DatasetID {
        Self::maybe_dataset_id_in_scope(self.0).unwrap_or_else(|| {
            panic!("FlowScopeDataset must have a '{FLOW_SCOPE_ATTRIBUTE_DATASET_ID}' attribute")
        })
    }

    pub fn maybe_dataset_id_in_scope(scope: &fs::FlowScope) -> Option<odf::DatasetID> {
        scope
            .get_attribute(FLOW_SCOPE_ATTRIBUTE_DATASET_ID)
            .and_then(|value| value.as_str())
            .and_then(|id_str| odf::DatasetID::from_did_str(id_str).ok())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
