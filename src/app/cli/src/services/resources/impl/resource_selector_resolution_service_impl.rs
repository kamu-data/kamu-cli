// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources_facade::GetResourceRef;

use crate::CLIError;
use crate::resources::{ResolvedResourceSelector, ResourceSelectorResolutionService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceSelectorResolutionService)]
pub struct ResourceSelectorResolutionServiceImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceSelectorResolutionService for ResourceSelectorResolutionServiceImpl {
    async fn resolve_single_selector(
        &self,
        selector: &str,
    ) -> Result<ResolvedResourceSelector, CLIError> {
        let resource_ref = match uuid::Uuid::parse_str(selector) {
            Ok(uid) if uid.get_version() == Some(uuid::Version::Random) => {
                GetResourceRef::ById(kamu_resources::ResourceUID::new(uid))
            }
            _ => GetResourceRef::ByName(selector.to_owned()),
        };

        Ok(ResolvedResourceSelector {
            input: selector.to_owned(),
            resource_ref,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
