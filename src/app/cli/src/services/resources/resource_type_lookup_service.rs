// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ResourceTypeDescriptor;

use crate::CLIError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceTypeLookupService: Send + Sync {
    async fn list_supported_resource_types(
        &self,
        explicit_context_name: Option<&str>,
    ) -> Result<Vec<ResourceTypeDescriptor>, CLIError>;

    async fn resolve_type_descriptor(
        &self,
        explicit_context_name: Option<&str>,
        target: &str,
        error_options: ResourceTypeLookupErrorOptions,
    ) -> Result<ResourceTypeDescriptor, CLIError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceTypeLookupErrorOptions {
    /// Command-specific prefix for the "selector not found" usage error.
    /// Example: "Unsupported list target".
    pub unsupported_prefix: String,

    /// Extra selectors to include in the "Supported targets" hint even if
    /// they are CLI-level aliases rather than real backend resource types.
    /// Example: `list` adds `datasets` here.
    pub additional_targets: Vec<String>,
}

impl ResourceTypeLookupErrorOptions {
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
