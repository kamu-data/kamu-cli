// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ResolveRemoteSourceError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves the URL of a remote dataset: the source of a pull, or a push
/// destination given by reference. Reads the repository registry and, for an
/// `odf+` repository, may ask the server for the account; it needs no
/// transaction, so a transfer can call it.
#[async_trait::async_trait]
pub trait RemoteDatasetUrlResolver: Send + Sync {
    async fn resolve_dataset_url(
        &self,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<url::Url, ResolveRemoteSourceError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
