// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_task_dataset::{TaskResultDatasetHardCompact, TaskResultDatasetResetToMetadata};
use kamu_core::CompactionResult;
use kamu_task_system::TaskResult;
use pretty_assertions::assert_eq;

// Compaction results are stored with finished tasks, so results written
// before the breaking flag existed must still be read

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_hard_compact_result_stored_without_breaking_flag_reads_as_breaking() {
    let task_result =
        Fixture::stored_result(TaskResultDatasetHardCompact::TYPE_ID, "compaction_result");

    assert_eq!(
        Fixture::expected_success(),
        TaskResultDatasetHardCompact::from_task_result(&task_result)
            .unwrap()
            .compaction_result,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_reset_to_metadata_result_stored_without_breaking_flag_reads_as_breaking() {
    let task_result = Fixture::stored_result(
        TaskResultDatasetResetToMetadata::TYPE_ID,
        "compaction_metadata_only_result",
    );

    assert_eq!(
        Fixture::expected_success(),
        TaskResultDatasetResetToMetadata::from_task_result(&task_result)
            .unwrap()
            .compaction_metadata_only_result,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_compaction_result_json_is_stable() {
    assert_eq!(
        serde_json::json!({
            "Success": {
                "old_head": Fixture::old_head().to_string(),
                "new_head": Fixture::new_head().to_string(),
                "old_num_blocks": 5,
                "new_num_blocks": 2,
                "breaking": true,
            }
        }),
        serde_json::to_value(Fixture::expected_success()).unwrap(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Compaction results as stored, and as read back
struct Fixture;

impl Fixture {
    fn stored_result(result_type: &str, field: &str) -> TaskResult {
        TaskResult {
            result_type: result_type.to_string(),
            payload: serde_json::json!({
                field: {
                    "Success": {
                        "old_head": Self::old_head().to_string(),
                        "new_head": Self::new_head().to_string(),
                        "old_num_blocks": 5,
                        "new_num_blocks": 2,
                    }
                }
            }),
        }
    }

    fn expected_success() -> CompactionResult {
        CompactionResult::Success {
            old_head: Self::old_head(),
            new_head: Self::new_head(),
            old_num_blocks: 5,
            new_num_blocks: 2,
            breaking: true,
        }
    }

    fn old_head() -> odf::Multihash {
        odf::Multihash::from_digest_sha3_256(b"old")
    }

    fn new_head() -> odf::Multihash {
        odf::Multihash::from_digest_sha3_256(b"new")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
