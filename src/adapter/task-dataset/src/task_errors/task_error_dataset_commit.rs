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
        HeadMoved(DatasetHeadMovedError),
    }
    => "DatasetCommitError", recoverable: true
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `HEAD` was no longer at the head the task had planned from
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatasetHeadMovedError {
    pub dataset_id: odf::DatasetID,
    pub expected_head: Option<odf::Multihash>,
    pub actual_head: Option<odf::Multihash>,
}

impl DatasetHeadMovedError {
    /// `heads` are `None` when the destination did not report them
    pub fn new(dataset_id: odf::DatasetID, heads: Option<odf::dataset::RefCASError>) -> Self {
        let (expected_head, actual_head) = heads.map_or((None, None), |e| (e.expected, e.actual));
        Self {
            dataset_id,
            expected_head,
            actual_head,
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
    fn test_head_moved_task_error_is_recoverable_and_stable() {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
        let expected_head = odf::Multihash::from_digest_sha3_256(b"expected");

        let task_error = TaskErrorDatasetCommit::HeadMoved(DatasetHeadMovedError {
            dataset_id: dataset_id.clone(),
            expected_head: Some(expected_head.clone()),
            actual_head: None,
        })
        .into_task_error();

        assert!(task_error.recoverable);
        assert_eq!(
            json!({
                "DatasetCommitError": {
                    "HeadMoved": {
                        "dataset_id": dataset_id.to_string(),
                        "expected_head": expected_head.to_string(),
                        "actual_head": null,
                    }
                },
                "recoverable": true,
            }),
            serde_json::to_value(&task_error).unwrap(),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
