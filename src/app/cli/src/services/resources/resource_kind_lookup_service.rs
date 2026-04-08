// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ResourceKindDescriptor;

use crate::CLIError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceKindLookupService: Send + Sync {
    async fn list_supported_kinds(
        &self,
        explicit_context_name: Option<&str>,
    ) -> Result<Vec<ResourceKindDescriptor>, CLIError>;

    async fn resolve_kind_descriptor(
        &self,
        explicit_context_name: Option<&str>,
        target: &str,
        error_options: ResourceKindLookupErrorOptions,
    ) -> Result<ResourceKindDescriptor, CLIError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceKindLookupErrorOptions {
    /// Command-specific prefix for the "selector not found" usage error.
    /// Example: "Unsupported list target".
    pub unsupported_prefix: String,

    /// Extra selectors to include in the "Supported targets" hint even if
    /// they are CLI-level aliases rather than real backend resource kinds.
    /// Example: `list` adds `datasets` here.
    pub additional_targets: Vec<String>,
}

impl ResourceKindLookupErrorOptions {
    pub fn new(unsupported_prefix: impl Into<String>) -> Self {
        Self {
            unsupported_prefix: unsupported_prefix.into(),
            additional_targets: Vec::new(),
        }
    }

    pub fn with_additional_targets(
        mut self,
        additional_targets: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.additional_targets = additional_targets.into_iter().map(Into::into).collect();
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
