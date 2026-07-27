// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources_facade::{ApplyManifestRequest, ResourceFacade};

use crate::resources::{
    DiscoveredResourceManifest,
    ExecuteResourceManifestError,
    ExecuteResourceManifestOutcome,
    ResourceManifestExecutionService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceManifestExecutionService)]
pub struct ResourceManifestExecutionServiceImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl ResourceManifestExecutionService for ResourceManifestExecutionServiceImpl {
    async fn execute(
        &self,
        resource_facade: &dyn ResourceFacade,
        manifest: &DiscoveredResourceManifest,
        dry_run: bool,
    ) -> Result<ExecuteResourceManifestOutcome, ExecuteResourceManifestError> {
        let content = manifest
            .source
            .read_content()
            .map_err(ExecuteResourceManifestError::ReadManifest)?;
        let request = ApplyManifestRequest {
            format: manifest.format,
            manifest: content,
        };

        if dry_run {
            let decision = resource_facade.plan_apply_manifest(request).await?;
            return Ok(decision.into());
        }

        let decision = resource_facade.apply_manifest(request).await?;
        Ok(decision.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
