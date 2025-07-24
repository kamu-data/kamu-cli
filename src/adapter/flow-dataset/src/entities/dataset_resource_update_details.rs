// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system as fs;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DATASET_RESOURCE_TYPE: &str = "dev.kamu.resource.DatasetResource";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct DatasetResourceUpdateDetails {
    pub dataset_id: odf::DatasetID,
    pub source: DatasetUpdateSource,
    pub old_head_maybe: Option<odf::Multihash>,
    pub new_head: odf::Multihash,
}

impl DatasetResourceUpdateDetails {
    pub fn push_source_name(&self) -> Option<String> {
        match &self.source {
            DatasetUpdateSource::UpstreamFlow { .. }
            | DatasetUpdateSource::SmartProtocolPush { .. } => None,
            DatasetUpdateSource::HttpIngest { source_name } => source_name.clone(),
        }
    }

    pub fn flow_description(&self) -> String {
        match &self.source {
            DatasetUpdateSource::UpstreamFlow { .. } => {
                "Flow activated by upstream flow".to_string()
            }
            DatasetUpdateSource::HttpIngest { source_name } => {
                format!("Flow activated by root dataset ingest via HTTP endpoint: {source_name:?}")
            }
            DatasetUpdateSource::SmartProtocolPush {
                account_name,
                is_force: _,
            } => {
                if let Some(account_name) = account_name {
                    format!(
                        "Flow activated via Smart Transfer Protocol push by account: \
                         {account_name})"
                    )
                } else {
                    "Flow activated via Smart Transfer Protocol push anonymously".to_string()
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum DatasetUpdateSource {
    UpstreamFlow {
        flow_type: String,
        flow_id: fs::FlowID,
        maybe_flow_config_snapshot: Option<fs::FlowConfigurationRule>,
    },
    HttpIngest {
        source_name: Option<String>,
    },
    SmartProtocolPush {
        account_name: Option<odf::AccountName>,
        is_force: bool,
    },
}

impl DatasetUpdateSource {
    pub fn maybe_flow_config_snapshot(&self) -> Option<&fs::FlowConfigurationRule> {
        match self {
            DatasetUpdateSource::UpstreamFlow {
                maybe_flow_config_snapshot,
                ..
            } => maybe_flow_config_snapshot.as_ref(),
            DatasetUpdateSource::HttpIngest { .. }
            | DatasetUpdateSource::SmartProtocolPush { .. } => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
