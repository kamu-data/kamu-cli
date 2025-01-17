// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;

use crate::ResolvedDataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MetadataQueryService: Send + Sync {
    /// Returns an active polling source, if any
    async fn get_active_polling_source(
        &self,
        target: ResolvedDataset,
    ) -> Result<Option<PollingSourceBlockInfo>, InternalError>;

    /// Returns the set of active push sources
    async fn get_active_push_sources(
        &self,
        target: ResolvedDataset,
    ) -> Result<
        Vec<(
            odf::Multihash,
            odf::MetadataBlockTyped<odf::metadata::AddPushSource>,
        )>,
        InternalError,
    >;

    /// Returns an active transform, if any
    async fn get_active_transform(
        &self,
        target: ResolvedDataset,
    ) -> Result<
        Option<(
            odf::Multihash,
            odf::MetadataBlockTyped<odf::metadata::SetTransform>,
        )>,
        InternalError,
    >;

    /// Attempt reading watermark that is currently associated with a dataset
    async fn try_get_current_watermark(
        &self,
        dataset: ResolvedDataset,
    ) -> Result<Option<DateTime<Utc>>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type PollingSourceBlockInfo = (
    odf::Multihash,
    odf::MetadataBlockTyped<odf::metadata::SetPollingSource>,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
