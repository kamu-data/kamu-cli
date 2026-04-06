// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
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
#[serde(deny_unknown_fields, rename_all = "camelCase")]
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CurrentResourceContextState {
    pub current_context_name: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ResourceContextsState {
    #[serde(default)]
    pub current_context_name: Option<String>,

    #[serde(default)]
    pub contexts: ResourceContextRegistry,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedResourceContext {
    LocalWorkspace,
    RemoteWorkspace { name: String, backend_url: Url },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
