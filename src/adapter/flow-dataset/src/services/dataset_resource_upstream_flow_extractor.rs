// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system as fs;

use crate::{DATASET_RESOURCE_TYPE, DatasetResourceUpdateDetails, DatasetUpdateSource};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowActivationCauseUpstreamExtractor)]
pub struct DatasetResourceUpstreamFlowExtractor {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl fs::FlowActivationCauseUpstreamExtractor for DatasetResourceUpstreamFlowExtractor {
    fn resource_type(&self) -> &'static str {
        DATASET_RESOURCE_TYPE
    }

    fn extract_upstream_flow_id(
        &self,
        update: &fs::FlowActivationCauseResourceUpdate,
    ) -> Option<fs::FlowID> {
        let details: DatasetResourceUpdateDetails =
            match serde_json::from_value(update.details.clone()) {
                Ok(details) => details,
                Err(e) => {
                    tracing::warn!(error = ?e, "Unreadable dataset resource update details");
                    return None;
                }
            };

        match details.source {
            DatasetUpdateSource::UpstreamFlow { flow_id, .. } => Some(flow_id),
            DatasetUpdateSource::HttpIngest { .. }
            | DatasetUpdateSource::SmartProtocolPush { .. }
            | DatasetUpdateSource::ExternallyDetectedChange => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
