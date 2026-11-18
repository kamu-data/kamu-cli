// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_datasets::{
    DatasetReferenceUpdater,
    DatasetRegistry,
    DatasetRegistryExt,
    ResolvedDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves a job's target by its ID in the current transaction. A job
/// re-resolves its target in each transaction, so a dataset deleted since the
/// previous one is not found
pub(crate) async fn resolve_job_target<E>(
    dataset_registry: &dyn DatasetRegistry,
    target: &odf::DatasetHandle,
) -> Result<ResolvedDataset, E>
where
    E: From<odf::DatasetNotFoundError> + From<InternalError>,
{
    let target_ref = target.id.as_local_ref();
    dataset_registry
        .try_get_dataset_by_ref(&target_ref)
        .await?
        .ok_or_else(|| odf::DatasetNotFoundError::new(target_ref).into())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Moves the job target's `HEAD` from `old_head` to `new_head` in the current
/// transaction, re-resolving the target first: the blocks were written with no
/// transaction open, and the dataset may have been deleted meanwhile. Without
/// `old_head`, `HEAD` must not exist yet
pub(crate) async fn commit_job_head<E>(
    dataset_registry: &dyn DatasetRegistry,
    dataset_reference_updater: &dyn DatasetReferenceUpdater,
    target: &odf::DatasetHandle,
    old_head: Option<&odf::Multihash>,
    new_head: &odf::Multihash,
) -> Result<(), E>
where
    E: From<odf::DatasetNotFoundError> + From<odf::dataset::SetChainRefError> + From<InternalError>,
{
    let dataset = resolve_job_target::<E>(dataset_registry, target).await?;
    dataset_reference_updater
        .update_reference(&dataset, &odf::BlockRef::Head, old_head, new_head)
        .await?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
