// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core as domain;

use crate::prelude::*;

/////////////////////////////////////////////////////////////////////////////////////////

/// Describes
#[derive(SimpleObject)]
pub struct EngineDesc {
    /// A short name of the engine, e.g. "Spark", "Flink".
    /// Intended for use in UI for quick engine identification and selection.
    pub name: String,
    /// Language and dialect this engine is using for queries
    /// Indented for configuring code highlighting and completions.
    pub dialect: QueryDialect,
    /// OCI image repository and a tag of the latest engine image, e.g.
    /// "ghcr.io/kamu-data/engine-datafusion:0.1.2"
    pub latest_image: String,
}

impl From<domain::EngineDesc> for EngineDesc {
    fn from(value: domain::EngineDesc) -> Self {
        Self {
            name: value.name,
            dialect: value.dialect.into(),
            latest_image: value.latest_image,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::enum_variant_names)]
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryDialect {
    SqlSpark,
    SqlFlink,
    SqlDataFusion,
}

impl From<domain::QueryDialect> for QueryDialect {
    fn from(value: domain::QueryDialect) -> Self {
        match value {
            domain::QueryDialect::SqlSpark => QueryDialect::SqlSpark,
            domain::QueryDialect::SqlFlink => QueryDialect::SqlFlink,
            domain::QueryDialect::SqlDataFusion => QueryDialect::SqlDataFusion,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
