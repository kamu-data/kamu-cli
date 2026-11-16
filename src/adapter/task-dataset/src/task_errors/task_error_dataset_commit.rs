// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_error_enum! {
    /// The task's dataset change could not be committed; a retry plans it again
    pub enum TaskErrorDatasetCommit {
        RefMoved(DatasetRefMovedError),
    }
    => "DatasetCommitError", recoverable: true
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The dataset's reference was no longer where the task had planned from
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatasetRefMovedError {
    pub dataset_id: odf::DatasetID,
    pub block_ref: odf::BlockRef,
    pub expected_block_hash: Option<odf::Multihash>,
    pub actual_block_hash: Option<odf::Multihash>,
}

impl DatasetRefMovedError {
    /// `reported` is `None` when the destination did not report where
    /// `block_ref` was expected and found
    pub fn new(
        dataset_id: odf::DatasetID,
        block_ref: odf::BlockRef,
        reported: Option<odf::dataset::RefCASError>,
    ) -> Self {
        let (expected_block_hash, actual_block_hash) =
            reported.map_or((None, None), |e| (e.expected, e.actual));
        Self {
            dataset_id,
            block_ref,
            expected_block_hash,
            actual_block_hash,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_ref_moved_task_error_is_recoverable_and_stable() {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
        let expected_block_hash = odf::Multihash::from_digest_sha3_256(b"expected");

        let task_error = TaskErrorDatasetCommit::RefMoved(DatasetRefMovedError {
            dataset_id: dataset_id.clone(),
            block_ref: odf::BlockRef::Head,
            expected_block_hash: Some(expected_block_hash.clone()),
            actual_block_hash: None,
        })
        .into_task_error();

        assert!(task_error.recoverable);
        assert_eq!(
            json!({
                "DatasetCommitError": {
                    "RefMoved": {
                        "dataset_id": dataset_id.to_string(),
                        "block_ref": "Head",
                        "expected_block_hash": expected_block_hash.to_string(),
                        "actual_block_hash": null,
                    }
                },
                "recoverable": true,
            }),
            serde_json::to_value(&task_error).unwrap(),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
