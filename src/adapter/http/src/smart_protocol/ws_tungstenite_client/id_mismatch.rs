// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::*;
use kamu_datasets::ResolvedDataset;
use odf::dataset::MetadataChainExt;
use url::Url;

use super::WsSmartTransferProtocolClient;
use crate::smart_protocol::remote_dataset_id::try_fetch_remote_dataset_id;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WsSmartTransferProtocolClient {
    /// Compares the ID of a remote source with the local destination's.
    /// Returns `None` when they match or the remote ID is unavailable.
    pub(super) async fn find_pull_dataset_id_mismatch(
        http_src_url: &Url,
        maybe_access_token: Option<&str>,
        dst: &ResolvedDataset,
    ) -> Option<DatasetIdMismatchError> {
        let src_dataset_id = try_fetch_remote_dataset_id(http_src_url, maybe_access_token).await?;
        let dst_dataset_id = &dst.get_handle().id;

        (src_dataset_id != *dst_dataset_id).then(|| DatasetIdMismatchError {
            src_dataset_id,
            dst_dataset_id: dst_dataset_id.clone(),
        })
    }

    /// Compares the ID of a local source with the remote destination's.
    /// Returns `None` when they match or either ID is unavailable.
    pub(super) async fn find_push_dataset_id_mismatch(
        &self,
        src: &dyn odf::Dataset,
        http_dst_url: &Url,
    ) -> Option<DatasetIdMismatchError> {
        let src_dataset_id = match src
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSeedVisitor::new())
            .await
        {
            Ok(seed_visitor) => seed_visitor.into_event()?.dataset_id,
            Err(err) => {
                tracing::debug!(error = ?err, "Failed to read source dataset seed");
                return None;
            }
        };

        let maybe_access_token = self
            .dataset_credential_resolver
            .resolve_odf_dataset_access_token(http_dst_url);
        let dst_dataset_id =
            try_fetch_remote_dataset_id(http_dst_url, maybe_access_token.as_deref()).await?;

        (src_dataset_id != dst_dataset_id).then_some(DatasetIdMismatchError {
            src_dataset_id,
            dst_dataset_id,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
