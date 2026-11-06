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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_compaction_results_round_trip_through_stored_json() {
    assert_eq!(
        serde_json::json!({
            "Success": {
                "old_head": Fixture::old_head().to_string(),
                "new_head": Fixture::new_head().to_string(),
                "old_num_blocks": 5,
                "new_num_blocks": 2,
            }
        }),
        serde_json::to_value(Fixture::expected_success()).unwrap(),
    );

    let hard_compact =
        Fixture::stored_result(TaskResultDatasetHardCompact::TYPE_ID, "compaction_result");
    assert_eq!(
        Fixture::expected_success(),
        TaskResultDatasetHardCompact::from_task_result(&hard_compact)
            .unwrap()
            .compaction_result,
    );

    let reset_to_metadata = Fixture::stored_result(
        TaskResultDatasetResetToMetadata::TYPE_ID,
        "compaction_metadata_only_result",
    );
    assert_eq!(
        Fixture::expected_success(),
        TaskResultDatasetResetToMetadata::from_task_result(&reset_to_metadata)
            .unwrap()
            .compaction_metadata_only_result,
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
