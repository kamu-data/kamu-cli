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
    /// Accepted forms:
    /// - `all`
    /// - `kind all` or `kind/all`
    /// - `kind name ...` — same-kind selectors, none containing `/`
    /// - `kind/name ...` — slash selectors, each containing exactly one `/`
    async fn parse_get_args(
        &self,
        explicit_context_name: Option<&str>,
        args: &[String],
    ) -> Result<ResourceSelectionSyntax, CLIError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceSelectionSyntax {
    pub items: Vec<ResourceSelectionItem>,
    pub shadowed_selectors: Vec<ResourceShadowedSelector>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum ResourceSelectionItem {
    All,
    AllByKind {
        kind_descriptor: ResourceKindDescriptor,
        selector_input: String,
    },
    Exact(ResourceExactSelector),
    NamePattern {
        kind_descriptor: ResourceKindDescriptor,
        selector_input: String,
        name_pattern: String,
    },
    KindPatternExactName {
        kind_pattern: String,
        selector_input: String,
        resource_ref: ResourceRef,
    },
    KindPatternAll {
        kind_pattern: String,
        selector_input: String,
    },
    KindPatternNamePattern {
        kind_pattern: String,
        selector_input: String,
        name_pattern: String,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceExactSelector {
    pub kind_descriptor: ResourceKindDescriptor,
    pub selector_input: String,
    pub resource_ref: ResourceRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceShadowedSelector {
    pub selector_input: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
