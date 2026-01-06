// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use async_graphql::{Context, Guard, Result};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum GqlFeature {
    /// Molecule API v1 (deprecated) enabled
    MoleculeApiV1,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct GqlFeatureFlags {
    enabled: HashSet<GqlFeature>,
}

impl GqlFeatureFlags {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_feature(mut self, feature: GqlFeature) -> Self {
        self.enabled.insert(feature);
        self
    }

    pub fn is_enabled(&self, feature: GqlFeature) -> bool {
        self.enabled.contains(&feature)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FeatureEnabledGuard {
    feature: GqlFeature,
}

impl FeatureEnabledGuard {
    pub fn new(feature: GqlFeature) -> Self {
        Self { feature }
    }

    pub fn is_enabled(&self, ctx: &Context<'_>) -> Result<()> {
        let feature_flags = from_catalog_n!(ctx, GqlFeatureFlags);

        if feature_flags.is_enabled(self.feature) {
            Ok(())
        } else {
            Err(async_graphql::Error::new(format!(
                "Feature '{}' is disabled",
                self.feature
            )))
        }
    }
}

impl Guard for FeatureEnabledGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        self.is_enabled(ctx)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
