// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ResourceID, ResourceName, ResourceTypeDescriptor};

use crate::CLIError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How the user spelled one exact resource: as a `UUIDv4`, or as a name.
///
/// Deliberately either/or, and deliberately CLI-owned. The wire-level
/// [`kamu_resources::ResourceRef`] has independently optional `id` and `name`
/// because ODF allows both together as a consistency assertion — but a CLI
/// argument is one token, so it is always exactly one of the two. Keeping that
/// distinction is what lets `kamu get vs/nope` report "not found" while
/// `kamu get vs/nope-%` reports "pattern matched nothing".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExactResourceRef {
    ById(ResourceID),
    ByName(ResourceName),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceSelectionSyntaxService: Send + Sync {
    /// Parses positional CLI arguments into a normalized resource selection.
    ///
    /// Accepted forms:
    /// - `id` — a bare `UUIDv4`, resolved across all supported types
    /// - `type name ...` — same-type selectors, none containing `/`
    /// - `type/name ...` — slash selectors, each containing exactly one `/`
    /// - `type %` or `type/%` — every resource of one type
    /// - `%/%` — every resource of every type
    ///
    /// The `type` part is matched exactly (case-insensitively) against a
    /// canonical selector name or alias, with the single exception of `%`,
    /// which stands for "all types". Wildcards are **not** accepted anywhere
    /// else in the type part — `%set` is a usage error, not a pattern. Names,
    /// by contrast, may use `%` patterns freely.
    async fn parse_get_args(
        &self,
        explicit_context_name: Option<&str>,
        args: &[String],
    ) -> Result<ResourceSelectionSyntax, CLIError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A parsed command's selectors, split into those worth resolving and those
/// already covered by a broader one in the same command.
#[derive(Debug, Clone)]
pub struct ResourceSelectionSyntax {
    pub items: Vec<ResourceSelectionItem>,
    pub shadowed_selectors: Vec<ResourceShadowedSelector>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One normalized selector, as classified from the CLI's positional arguments.
///
/// The variants divide along two axes: whether the *type* is one concrete type
/// or all of them, and whether the *name* is exact, a `%` pattern, or absent
/// (meaning every name). `selector_input` on every variant is the spelling the
/// user typed, kept verbatim for diagnostics.
#[derive(Debug, Clone)]
pub enum ResourceSelectionItem {
    /// Every resource of every type.
    ///
    /// `kamu get %/%`, `kamu get % %`
    All,

    /// Every resource of one type.
    ///
    /// `kamu get vs %`, `kamu get vs/%`, `kamu delete vs --all`
    AllByType {
        type_descriptor: ResourceTypeDescriptor,
        selector_input: String,
    },

    /// One resource of one type, by exact name or ID.
    ///
    /// `kamu get vs my-vars`, `kamu get vs/my-vars`,
    /// `kamu get vs/3d8d6d1c-6f7c-4c62-9f4e-7d8295e8fb69`
    Exact(ResourceExactSelector),

    /// A bare `UUIDv4` with no type given — resolved across all supported
    /// types. Distinct from [`Self::AnyTypeExactRef`] in that the user named no
    /// type at all, so these are prefetched as one batched ID lookup.
    ///
    /// `kamu get 3d8d6d1c-6f7c-4c62-9f4e-7d8295e8fb69`
    ExactAnyType {
        selector_input: String,
        resource_ref: ExactResourceRef,
    },

    /// Resources of one type whose names match a `%` pattern.
    ///
    /// `kamu get vs app-%`, `kamu get vs/app-%`
    NamePattern {
        type_descriptor: ResourceTypeDescriptor,
        selector_input: String,
        name_pattern: String,
    },

    /// One exact name or ID, looked up across all supported types via the
    /// explicit `%` all-types token.
    ///
    /// `kamu get %/my-vars`, `kamu get %/3d8d6d1c-6f7c-4c62-9f4e-7d8295e8fb69`
    AnyTypeExactRef {
        selector_input: String,
        resource_ref: ExactResourceRef,
    },

    /// A `%` name pattern applied across all supported types.
    ///
    /// `kamu get %/app-%`
    AnyTypeNamePattern {
        selector_input: String,
        name_pattern: String,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The payload of [`ResourceSelectionItem::Exact`]: one concrete type plus one
/// exact reference.
///
/// `kamu get vs/my-vars`
#[derive(Debug, Clone)]
pub struct ResourceExactSelector {
    pub type_descriptor: ResourceTypeDescriptor,
    pub selector_input: String,
    pub resource_ref: ExactResourceRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A selector whose targets are already fully covered by a broader selector in
/// the same command, so resolving it would only repeat backend work. Reported
/// to the user rather than silently dropped.
///
/// In `kamu get %/% vs/my-vars`, the `vs/my-vars` part is shadowed by `%/%`.
#[derive(Debug, Clone)]
pub struct ResourceShadowedSelector {
    pub selector_input: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
