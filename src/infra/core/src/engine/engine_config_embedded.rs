// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::prelude::SessionConfig;
use internal_error::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineConfigDatafusionEmbeddedBase;

impl EngineConfigDatafusionEmbeddedBase {
    pub const DEFAULT_SETTINGS: &[(&str, &str)] = &[
        ("datafusion.catalog.information_schema", "true"),
        ("datafusion.catalog.default_catalog", "kamu"),
        ("datafusion.catalog.default_schema", "kamu"),
        // Forcing case-sensitive identifiers in case-insensitive language seems to
        // be a lesser evil than following DataFusion's default behavior of forcing
        // identifiers to lowercase instead of case-insensitive matching.
        //
        // See: https://github.com/apache/datafusion/issues/7460
        // TODO: Consider externalizing this config (e.g. by allowing custom engine
        // options in transform DTOs)
        ("datafusion.sql_parser.enable_ident_normalization", "false"),
    ];

    pub fn new_session_config<I, S>(settings: I) -> Result<SessionConfig, InternalError>
    where
        I: IntoIterator<Item = (S, S)>,
        S: Into<String>,
    {
        SessionConfig::from_string_hash_map(
            &settings
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
        .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Injectable ingest configuration
pub struct EngineConfigDatafusionEmbeddedIngest(pub SessionConfig);

impl EngineConfigDatafusionEmbeddedIngest {
    pub const DEFAULT_OVERRIDES: &[(&str, &str)] = &[
        // Note: We use single partition as ingest currently always reads one file at a
        // time and repartitioning of data likely to hurt performance rather than
        // improve it
        ("datafusion.execution.target_partitions", "1"),
    ];
}

impl Default for EngineConfigDatafusionEmbeddedIngest {
    fn default() -> Self {
        Self(
            EngineConfigDatafusionEmbeddedBase::new_session_config(
                EngineConfigDatafusionEmbeddedBase::DEFAULT_SETTINGS
                    .iter()
                    .chain(Self::DEFAULT_OVERRIDES)
                    .copied(),
            )
            .unwrap(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Injectable batch query configuration
pub struct EngineConfigDatafusionEmbeddedBatchQuery(pub SessionConfig);

impl EngineConfigDatafusionEmbeddedBatchQuery {
    pub const DEFAULT_OVERRIDES: &[(&str, &str)] = &[];
}

impl Default for EngineConfigDatafusionEmbeddedBatchQuery {
    fn default() -> Self {
        Self(
            EngineConfigDatafusionEmbeddedBase::new_session_config(
                EngineConfigDatafusionEmbeddedBase::DEFAULT_SETTINGS
                    .iter()
                    .chain(Self::DEFAULT_OVERRIDES)
                    .copied(),
            )
            .unwrap(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Injectable compaction configuration
pub struct EngineConfigDatafusionEmbeddedCompaction(pub SessionConfig);

impl EngineConfigDatafusionEmbeddedCompaction {
    pub const DEFAULT_OVERRIDES: &[(&str, &str)] = &[
        // TODO: This is a legacy option as compaction service used to inherit its config from
        // ingest. We should validate that compactions work correctly with multiple
        // partitions and remove it eventually.
        ("datafusion.execution.target_partitions", "1"),
    ];
}

impl Default for EngineConfigDatafusionEmbeddedCompaction {
    fn default() -> Self {
        Self(
            EngineConfigDatafusionEmbeddedBase::new_session_config(
                EngineConfigDatafusionEmbeddedBase::DEFAULT_SETTINGS
                    .iter()
                    .chain(Self::DEFAULT_OVERRIDES)
                    .copied(),
            )
            .unwrap(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
