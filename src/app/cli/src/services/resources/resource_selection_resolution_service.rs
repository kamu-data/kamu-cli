// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    ResourceID,
    ResourceLabelFilterInput,
    ResourceName,
    ResourceSelectorName,
    ResourceTypeDescriptor,
    TypeUri,
};
use kamu_resources_facade::ResourceFacade;

use crate::CLIError;
use crate::resources::ResourceSelectionSyntax;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceSelectionResolutionService: Send + Sync {
    async fn resolve(
        &self,
        selection: ResourceSelectionSyntax,
        resource_facade: &dyn ResourceFacade,
        options: &ResourceSelectionResolutionOptions,
    ) -> Result<ResourceSelectionResolution, CLIError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct ResourceSelectionResolutionOptions {
    pub ignore_not_found: bool,
    pub max_expanded_results: Option<usize>,

    /// Narrows every expansion to resources carrying these labels. Applied
    /// during identifier expansion rather than after it, so `--max-results`
    /// counts only resources that actually match.
    pub label_filter: Option<ResourceLabelFilterInput>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceSelectionResolution {
    pub targets: Vec<ResourceTarget>,
    pub ignored_selectors: Vec<ResourceIgnoredSelector>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceTarget {
    pub canonical_selector: ResourceSelectorName,
    pub schema: TypeUri,
    pub id: ResourceID,
    pub name: ResourceName,
    pub selector_input: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceIgnoredSelector {
    pub type_descriptor: ResourceTypeDescriptor,
    pub selector_input: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
