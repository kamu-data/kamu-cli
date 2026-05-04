// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources_facade::{GetResourceError, GetResourceRequest, ResourceFacade};

use crate::CLIError;
use crate::resources::{
    ResourceIgnoredSelector,
    ResourceSelectionResolution,
    ResourceSelectionResolutionService,
    ResourceSelectionSyntax,
    ResourceTarget,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceSelectionResolutionService)]
pub struct ResourceSelectionResolutionServiceImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceSelectionResolutionService for ResourceSelectionResolutionServiceImpl {
    async fn resolve(
        &self,
        selection: ResourceSelectionSyntax,
        resource_facade: &dyn ResourceFacade,
        ignore_not_found: bool,
    ) -> Result<ResourceSelectionResolution, CLIError> {
        let mut targets = Vec::with_capacity(selection.selectors.len());
        let mut ignored_selectors = Vec::new();

        // TODO: Optimize by batching requests
        for selector in selection.selectors {
            let identity = resource_facade
                .get_identity(GetResourceRequest {
                    kind: selector.kind_descriptor.kind.clone(),
                    api_version: Some(selector.kind_descriptor.api_version.clone()),
                    account: None,
                    resource_ref: selector.resource_ref,
                })
                .await;

            match identity {
                Ok(identity) => targets.push(ResourceTarget {
                    kind_descriptor: selector.kind_descriptor,
                    uid: identity.uid,
                    name: identity.name,
                    selector_input: selector.selector_input,
                }),
                Err(GetResourceError::NameNotFound(_) | GetResourceError::UIDNotFound(_))
                    if ignore_not_found =>
                {
                    ignored_selectors.push(ResourceIgnoredSelector {
                        kind_descriptor: selector.kind_descriptor,
                        selector_input: selector.selector_input,
                    });
                }
                Err(error) => return Err(error.into()),
            }
        }

        Ok(ResourceSelectionResolution {
            targets,
            ignored_selectors,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
