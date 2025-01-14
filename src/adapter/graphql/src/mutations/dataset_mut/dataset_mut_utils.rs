// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::DatasetOwnershipService;
use opendatafabric as odf;

use crate::prelude::*;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn ensure_account_owns_dataset(
    ctx: &Context<'_>,
    dataset_handle: &odf::DatasetHandle,
) -> Result<()> {
    let dataset_ownership_service = from_catalog_n!(ctx, dyn DatasetOwnershipService);
    let logged_account = utils::get_logged_account(ctx)?;

    if logged_account.is_admin {
        // Technically, the admin isn't the owner, but that's not a barrier in this case
        return Ok(());
    }

    let not_owner = !dataset_ownership_service
        .is_dataset_owned_by(&dataset_handle.id, &logged_account.account_id)
        .await?;

    if not_owner {
        return Err(Error::new("Only the dataset owner can perform this action").into());
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
