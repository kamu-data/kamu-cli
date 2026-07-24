// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ResourceSchemaId;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResourceExtensionKind {
    Label,
    Annotation,
    Condition,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceExtensionScope {
    AnyResource,
    ResourceContext {
        authority: String,
        context: String,
        version: Option<String>,
    },
    ResourceType {
        schema: ResourceSchemaId,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceExtensionSchemaMeta {
    pub schema_id: &'static str,
    pub kind: ResourceExtensionKind,
    pub document: &'static str,
    pub applications: &'static [ResourceExtensionApplicationMeta],
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceExtensionApplicationMeta {
    pub scope: ResourceExtensionScopeMeta,
    pub preferred_name: Option<&'static str>,
    pub aliases: &'static [&'static str],
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub enum ResourceExtensionScopeMeta {
    AnyResource,
    ResourceContext {
        authority: &'static str,
        context: &'static str,
        version: Option<&'static str>,
    },
    ResourceType {
        schema: &'static str,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct ResourceExtensionValueError {
    message: String,
}

impl ResourceExtensionValueError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn from_error(error: impl std::error::Error) -> Self {
        Self::new(error.to_string())
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
