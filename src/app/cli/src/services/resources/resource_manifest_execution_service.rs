// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    ApplyManifestChange,
    ApplyManifestRejection,
    ApplyResourceOutcome,
    ResourceView,
    ResourceWarning,
};
use kamu_resources_facade::{ApplyManifestError, ResourceFacade};

use crate::resources::DiscoveredResourceManifest;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
pub trait ResourceManifestExecutionService: Send + Sync {
    async fn execute(
        &self,
        resource_facade: &dyn ResourceFacade,
        manifest: &DiscoveredResourceManifest,
        dry_run: bool,
    ) -> Result<ExecuteResourceManifestOutcome, ExecuteResourceManifestError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum ExecuteResourceManifestOutcome {
    Accepted(ExecutedResourceManifestResult),
    Rejected(ApplyManifestRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ExecutedResourceManifestResult {
    pub outcome: ApplyResourceOutcome,
    pub resource: ResourceView,
    pub warnings: Vec<ResourceWarning>,
    pub changes: Vec<ApplyManifestChange>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ExecuteResourceManifestError {
    #[error(transparent)]
    Apply(#[from] ApplyManifestError),

    #[error("Failed to read manifest")]
    ReadManifest(#[source] std::io::Error),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
