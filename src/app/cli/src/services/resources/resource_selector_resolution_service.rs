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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceSelectorResolutionService: Send + Sync {
    async fn resolve_single_selector(
        &self,
        selector: &str,
    ) -> Result<ResolvedResourceSelector, CLIError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResolvedResourceSelector {
    pub input: String,
    pub resource_ref: GetResourceRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
