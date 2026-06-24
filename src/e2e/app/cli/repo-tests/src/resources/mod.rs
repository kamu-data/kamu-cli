// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared building blocks for resource CLI e2e scenarios: the [`ResourceCtx`]
//! local/remote abstraction, manifest [`fixtures`], and small assert helpers.
//!
//! Scenario bodies live in `crate::commands::test_resources_*` and are written
//! once against [`ResourceCtx`], then wired per-database as local and remote
//! permutations.

mod context;
pub mod fixtures;

pub use context::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Assert helpers for raw command output
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Assert every expected substring appears in a command output.
///
/// Use this for batch/resource-set commands where the CLI may emit stable
/// status lines in backend-dependent order.
pub fn assert_output_contains_all(output: &str, expected_lines: &[&str], command_name: &str) {
    for expected_line in expected_lines {
        assert!(
            output.contains(expected_line),
            "`{command_name}` should contain '{expected_line}', got:\n{output}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
