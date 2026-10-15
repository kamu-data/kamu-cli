// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, InternalError};
use kamu_datasets::{DatasetIncrementQueryService, GetIncrementError};
use kamu_flow_system as fs;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Changes of a dataset from `old_head` to `new_head`: breaking when
/// `old_head` is not in the history of `new_head`
pub(crate) async fn dataset_changes_between(
    dataset_increment_query_service: &dyn DatasetIncrementQueryService,
    dataset_id: &odf::DatasetID,
    old_head: &odf::Multihash,
    new_head: &odf::Multihash,
) -> Result<fs::ResourceChanges, InternalError> {
    match dataset_increment_query_service
        .get_increment_between(dataset_id, Some(old_head), new_head)
        .await
    {
        Ok(increment) => Ok(fs::ResourceChanges::NewData(fs::ResourceDataChanges {
            blocks_added: increment.num_blocks,
            records_added: increment.num_records,
            new_watermark: increment.updated_watermark,
        })),
        Err(GetIncrementError::InvalidInterval(_)) => Ok(fs::ResourceChanges::Breaking),
        Err(
            e @ (GetIncrementError::DatasetNotFound(_)
            | GetIncrementError::RefNotFound(_)
            | GetIncrementError::BlockNotFound(_)
            | GetIncrementError::Access(_)
            | GetIncrementError::Internal(_)),
        ) => Err(e.int_err()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Where `head` is in the history of a dataset relative to `other`
pub(crate) enum HeadRelation {
    Same,
    /// `head` is an ancestor of `other`
    Before,
    /// `head` is a descendant of `other`
    After,
    /// Neither is in the history of the other
    Diverged,
}

/// The common case, `head` before `other`, is checked first: its walk back
/// through history is the short one
pub(crate) async fn relation_between_heads(
    dataset_increment_query_service: &dyn DatasetIncrementQueryService,
    dataset_id: &odf::DatasetID,
    head: &odf::Multihash,
    other: &odf::Multihash,
) -> Result<HeadRelation, InternalError> {
    if head == other {
        return Ok(HeadRelation::Same);
    }

    match dataset_changes_between(dataset_increment_query_service, dataset_id, head, other).await? {
        fs::ResourceChanges::NewData(_) => Ok(HeadRelation::Before),
        fs::ResourceChanges::Breaking => {
            match dataset_changes_between(dataset_increment_query_service, dataset_id, other, head)
                .await?
            {
                fs::ResourceChanges::NewData(_) => Ok(HeadRelation::After),
                fs::ResourceChanges::Breaking => Ok(HeadRelation::Diverged),
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
