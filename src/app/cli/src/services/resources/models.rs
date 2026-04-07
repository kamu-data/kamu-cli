// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ResourceTypeCountSummary;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSummaryView {
    pub context: ResourceContextSummaryView,
    pub resource_counts: Vec<ResourceTypeCountSummary>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceContextSummaryView {
    pub active_context_name: String,
    pub context_type: ResourceContextTypeView,
    pub endpoint_url: Option<String>,
    pub server_version: Option<String>,
    pub account_name: Option<String>,
}

impl ResourceContextSummaryView {
    pub fn rows(&self) -> Vec<(&str, String)> {
        let mut rows = vec![
            ("Active context", self.active_context_name.clone()),
            ("Context type", self.context_type.to_string()),
        ];

        if let Some(endpoint_url) = &self.endpoint_url {
            rows.push(("Endpoint URL", endpoint_url.clone()));
        }
        if let Some(server_version) = &self.server_version {
            rows.push(("Server version", server_version.clone()));
        }
        if let Some(account_name) = &self.account_name {
            rows.push(("Account", account_name.clone()));
        }

        rows
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, serde::Serialize, strum::Display)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum ResourceContextTypeView {
    Local,
    Remote,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
