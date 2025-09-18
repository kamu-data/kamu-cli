// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::*;

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
        target: &ResolvedDataset,
    ) -> Result<Option<PollingSourceBlockInfo>, InternalError> {
        // TODO: Support source evolution
        use odf::dataset::MetadataChainExt;
        Ok(target
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetPollingSourceVisitor::new())
            .await
            .int_err()?
            .into_hashed_block())
    }

    /// Returns the set of active push sources
    async fn get_active_push_sources(
        &self,
        target: &ResolvedDataset,
    ) -> Result<
        Vec<(
            odf::Multihash,
            odf::MetadataBlockTyped<odf::metadata::AddPushSource>,
        )>,
        InternalError,
    > {
        use odf::dataset::MetadataChainExt;

        let visitor = target
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchActivePushSourcesVisitor::new(
                target.get_kind(),
            ))
            .await
            .int_err()?;
        let blocks = visitor.into_hashed_blocks();

        Ok(blocks)
    }

    /// Returns an active transform, if any
    async fn get_active_transform(
        &self,
        target: &ResolvedDataset,
    ) -> Result<
        Option<(
            odf::Multihash,
            odf::MetadataBlockTyped<odf::metadata::SetTransform>,
        )>,
        InternalError,
    > {
        // TODO: Support transform evolution
        use odf::dataset::MetadataChainExt;
        Ok(target
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetTransformVisitor::new())
            .await
            .int_err()?
            .into_hashed_block())
    }

    /// Attempt reading watermark that is currently associated with a dataset
    #[tracing::instrument(level = "info", skip_all)]
    async fn try_get_current_watermark(
        &self,
        resolved_dataset: &ResolvedDataset,
    ) -> Result<Option<DateTime<Utc>>, InternalError> {
        use odf::dataset::MetadataChainExt;
        let mut add_data_visitor = odf::dataset::SearchAddDataVisitor::new();

        resolved_dataset
            .as_metadata_chain()
            .accept(&mut [&mut add_data_visitor])
            .await
            .int_err()?;

        let current_watermark = add_data_visitor.into_event().and_then(|e| e.new_watermark);

        Ok(current_watermark)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
