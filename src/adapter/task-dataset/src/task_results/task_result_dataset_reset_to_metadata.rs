// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::CompactionResult;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_result_struct! {
    pub struct TaskResultDatasetResetToMetadata {
        pub compaction_metadata_only_result: CompactionResult,
    }
    => "ResetToMetadataDatasetResult"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use kamu_task_system::TaskResult;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_compaction_result_round_trips_through_stored_json() {
        let old_head = odf::Multihash::from_digest_sha3_256(b"old");
        let new_head = odf::Multihash::from_digest_sha3_256(b"new");

        let stored = TaskResult {
            result_type: TaskResultDatasetResetToMetadata::TYPE_ID.to_string(),
            payload: json!({
                "compaction_metadata_only_result": {
                    "Success": {
                        "old_head": old_head.to_string(),
                        "new_head": new_head.to_string(),
                        "old_num_blocks": 5,
                        "new_num_blocks": 2,
                    }
                }
            }),
        };

        assert_eq!(
            CompactionResult::Success {
                old_head,
                new_head,
                old_num_blocks: 5,
                new_num_blocks: 2,
            },
            TaskResultDatasetResetToMetadata::from_task_result(&stored)
                .unwrap()
                .compaction_metadata_only_result,
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
