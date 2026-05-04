// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ResourceKindDescriptor, ResourceName, ResourceUID};
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
        options: ResourceSelectionResolutionOptions,
    ) -> Result<ResourceSelectionResolution, CLIError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct ResourceSelectionResolutionOptions {
    pub ignore_not_found: bool,
    pub max_expanded_results: Option<usize>,
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
    pub kind: String,
    pub api_version: String,
    pub canonical_kind_name: String,
    pub uid: ResourceUID,
    pub name: ResourceName,
    pub selector_input: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceIgnoredSelector {
    pub kind_descriptor: ResourceKindDescriptor,
    pub selector_input: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
