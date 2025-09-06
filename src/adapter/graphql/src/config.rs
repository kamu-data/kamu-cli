// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Config {
    /// Enables compatibility mode where collection and versioned file
    /// archetypes were inferred from push source read schema in addition to
    /// using the schema attributes. This setting will be removed shortly after
    /// migration.
    #[serde(default)]
    pub enable_archetype_inference: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
