// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::*;
use opendatafabric::{self as odf, AsTypedBlock};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MetadataQueryService)]
pub struct MetadataQueryServiceImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MetadataQueryService for MetadataQueryServiceImpl {
    /// Returns an active polling source, if any
    async fn get_active_polling_source(
        &self,
        target: ResolvedDataset,
    ) -> Result<
        Option<(
            odf::Multihash,
            odf::MetadataBlockTyped<odf::SetPollingSource>,
        )>,
        InternalError,
    > {
        // TODO: Support source evolution
        Ok(target
            .as_metadata_chain()
            .accept_one(SearchSetPollingSourceVisitor::new())
            .await
            .int_err()?
            .into_hashed_block())
    }

    /// Returns the set of active push sources
    async fn get_active_push_sources(
        &self,
        target: ResolvedDataset,
    ) -> Result<Vec<(odf::Multihash, odf::MetadataBlockTyped<odf::AddPushSource>)>, InternalError>
    {
        use futures::TryStreamExt;

        // TODO: Support source disabling and evolution
        let stream = target
            .as_metadata_chain()
            .iter_blocks()
            .filter_map_ok(|(h, b)| b.into_typed().map(|b| (h, b)));

        Ok(stream.try_collect().await.int_err()?)
    }

    /// Returns an active transform, if any
    async fn get_active_transform(
        &self,
        target: ResolvedDataset,
    ) -> Result<Option<(odf::Multihash, odf::MetadataBlockTyped<odf::SetTransform>)>, InternalError>
    {
        // TODO: Support transform evolution
        Ok(target
            .as_metadata_chain()
            .accept_one(SearchSetTransformVisitor::new())
            .await
            .int_err()?
            .into_hashed_block())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
