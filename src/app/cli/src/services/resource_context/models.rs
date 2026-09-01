// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const LOCAL_CONTEXT_NAME: &str = "local";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum ResourceContextStoreScope {
    Workspace,
    User,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceContextRecord {
    pub name: String,
    pub backend_url: Url,
}

impl ResourceContextRecord {
    pub fn new(name: impl Into<String>, backend_url: Url) -> Self {
        Self {
            name: name.into(),
            backend_url,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ResourceContextRegistry = Vec<ResourceContextRecord>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedResourceContextRecord {
    pub scope: ResourceContextStoreScope,
    pub context: ResourceContextRecord,
    pub last_test_result: Option<ResourceContextLastTestResult>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CurrentResourceContextState {
    pub current_context_name: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceContextsState {
    #[serde(default)]
    pub contexts: ResourceContextRegistry,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Per-account slice of the runtime state stored under `.kamucontexts.state`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountContextRuntimeState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_context_name: Option<String>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub contexts: BTreeMap<String, ResourceContextLastTestResult>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// File-level runtime state keyed by account name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceContextsRuntimeState {
    #[serde(default)]
    pub accounts: BTreeMap<String, AccountContextRuntimeState>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedResourceContext {
    LocalWorkspace,
    RemoteWorkspace { name: String, backend_url: Url },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ResourceContextLastTestResult {
    pub status: ResourceContextLastTestStatus,
    pub checked_at: DateTime<Utc>,
    #[serde(default)]
    pub detail: Option<String>,
}

impl ResourceContextLastTestResult {
    pub fn from_test_result(
        test_result: &ResourceContextTestResult,
        checked_at: DateTime<Utc>,
    ) -> Self {
        let (status, detail) = match (&test_result.reachable, &test_result.auth_status) {
            (false, _) => (
                ResourceContextLastTestStatus::Unreachable,
                test_result.failure.as_ref().map(ToString::to_string),
            ),
            (
                true,
                ResourceContextTestAuthStatus::Valid | ResourceContextTestAuthStatus::NotChecked,
            ) => (ResourceContextLastTestStatus::ReachableValid, None),
            (true, ResourceContextTestAuthStatus::Missing) => {
                (ResourceContextLastTestStatus::ReachableMissingToken, None)
            }
            (true, ResourceContextTestAuthStatus::Expired) => {
                (ResourceContextLastTestStatus::ReachableExpiredToken, None)
            }
            (true, ResourceContextTestAuthStatus::Invalid) => {
                (ResourceContextLastTestStatus::ReachableInvalidToken, None)
            }
        };

        Self {
            status,
            checked_at,
            detail,
        }
    }

    pub fn label(&self) -> &'static str {
        self.status.label()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum ResourceContextLastTestStatus {
    ReachableValid,
    ReachableMissingToken,
    ReachableExpiredToken,
    ReachableInvalidToken,
    Unreachable,
}

impl ResourceContextLastTestStatus {
    pub fn label(&self) -> &'static str {
        match self {
            Self::ReachableValid => "Valid",
            Self::ReachableMissingToken => "Missing token",
            Self::ReachableExpiredToken => "Expired token",
            Self::ReachableInvalidToken => "Invalid token",
            Self::Unreachable => "Unreachable",
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceContextTestAuthStatus {
    NotChecked,
    Missing,
    Valid,
    Expired,
    Invalid,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ResourceContextTestFailure {
    #[error("request to '{url}' failed: {message}")]
    RequestFailed { url: Url, message: String },

    #[error("unexpected response from '{url}': {status_code} {status_text}")]
    UnexpectedResponse {
        url: Url,
        status_code: u16,
        status_text: String,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceContextTestResult {
    pub name: String,
    pub backend_url: Option<Url>,
    pub reachable: bool,
    pub auth_status: ResourceContextTestAuthStatus,
    pub recommendation: Option<String>,
    pub failure: Option<ResourceContextTestFailure>,
}

impl ResourceContextTestResult {
    pub fn is_healthy(&self) -> bool {
        self.reachable
            && matches!(
                self.auth_status,
                ResourceContextTestAuthStatus::Valid | ResourceContextTestAuthStatus::NotChecked
            )
    }

    pub fn remote_name(&self) -> Option<&str> {
        self.backend_url.as_ref().map(|_| self.name.as_str())
    }

    pub fn summary(&self) -> String {
        match (&self.reachable, &self.auth_status) {
            (false, _) => format!(
                "Context '{}' is unreachable: {}",
                self.name,
                self.failure
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| String::from("unknown error")),
            ),
            (true, ResourceContextTestAuthStatus::Valid) => format!(
                "Context '{}' is reachable and access token is valid",
                self.name
            ),
            (true, ResourceContextTestAuthStatus::Missing) => format!(
                "Context '{}' is reachable, but no access token was found",
                self.name
            ),
            (true, ResourceContextTestAuthStatus::Expired) => format!(
                "Context '{}' is reachable, but the access token has expired",
                self.name
            ),
            (true, ResourceContextTestAuthStatus::Invalid) => format!(
                "Context '{}' is reachable, but the access token is invalid",
                self.name
            ),
            (true, ResourceContextTestAuthStatus::NotChecked) => {
                format!("Context '{}' is available", self.name)
            }
        }
    }

    pub fn warning_message(&self) -> Option<String> {
        if self.is_healthy() {
            return None;
        }

        let mut warning = self.summary();
        if let Some(recommendation) = &self.recommendation {
            warning.push_str(". ");
            warning.push_str(recommendation);
        }

        Some(warning)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
