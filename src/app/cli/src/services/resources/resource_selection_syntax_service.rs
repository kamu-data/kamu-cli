// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ResourceKindDescriptor;
use kamu_resources_facade::ResourceRef;

use crate::CLIError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceSelectionSyntaxService: Send + Sync {
    /// Parses positional CLI arguments into a normalized resource selection.
    ///
    /// Accepted forms (Phase 2):
    /// - `kind name` — two positional args, neither contains `/`
    /// - `kind/name` — one positional arg containing exactly one `/`
    async fn parse_get_args(
        &self,
        explicit_context_name: Option<&str>,
        args: &[String],
    ) -> Result<ResourceSelectionSyntax, CLIError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceSelectionSyntax {
    pub selectors: Vec<ResolvedResourceSelectorByKind>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResolvedResourceSelectorByKind {
    pub kind_descriptor: ResourceKindDescriptor,
    pub selector_input: String,
    pub resource_ref: ResourceRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
