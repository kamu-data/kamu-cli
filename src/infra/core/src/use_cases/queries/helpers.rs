// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::ErrorIntoInternal;
use kamu_core::QueryError;
use kamu_datasets::{DatasetAction, ResolvedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn resolve_dataset_for_querying(
    rebac_dataset_registry_facade: &dyn kamu_auth_rebac::RebacDatasetRegistryFacade,
    dataset_ref: &odf::DatasetRef,
) -> Result<ResolvedDataset, QueryError> {
    let resolved_dataset = rebac_dataset_registry_facade
        .resolve_dataset_by_ref(dataset_ref, DatasetAction::Read)
        .await
        .map_err(|e| {
            use kamu_auth_rebac::RebacDatasetRefUnresolvedError as E;
            match e {
                E::NotFound(e) => QueryError::DatasetNotFound(e),
                E::Access(e) => QueryError::Access(e),
                e @ E::Internal(_) => QueryError::Internal(e.int_err()),
            }
        })?;

    Ok(resolved_dataset)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
