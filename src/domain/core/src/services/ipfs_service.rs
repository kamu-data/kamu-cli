// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_datasets::ResolvedDataset;
use thiserror::Error;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Operations on the local IPFS node. Reading datasets from IPFS goes through
/// the HTTP gateway instead, when a dataset is opened by its `ipfs://` or
/// `ipns://` URL.
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait IpfsService: Send + Sync {
    /// Adds the dataset's storage to IPFS and returns the root CID, without
    /// publishing it under any IPNS name
    async fn add_dataset(&self, dataset: ResolvedDataset) -> Result<String, IpfsAddError>;

    /// Finds the node's IPNS key with this ID
    async fn find_ipns_key(&self, key_id: &str) -> Result<Option<IpnsKey>, InternalError>;

    /// Resolves the key's name to a CID on this node only, with a short
    /// timeout; `None` when it does not resolve, as for a key never published
    async fn resolve_ipns_name_locally(
        &self,
        key: &IpnsKey,
    ) -> Result<Option<String>, InternalError>;

    /// Publishes the CID under the key's name. With `check_resolves`, the node
    /// first checks that the CID resolves.
    async fn publish_ipns_name(
        &self,
        key: &IpnsKey,
        cid: &str,
        check_resolves: bool,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A key of the local IPFS node that an IPNS name is published under
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpnsKey {
    pub id: String,
    pub name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum IpfsAddError {
    #[error(transparent)]
    UnsupportedIpfsStorageType(#[from] UnsupportedIpfsStorageTypeError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Only a dataset stored in the local file system can be added to IPFS
#[derive(Error, Debug)]
#[error("Dataset storage type '{}' is unsupported for IPFS operations", url.scheme())]
pub struct UnsupportedIpfsStorageTypeError {
    pub url: Url,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
