// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use internal_error::InternalError;
use kamu_core::{IpfsAddError, IpfsService, IpnsKey, UnsupportedIpfsStorageTypeError};
use kamu_datasets::ResolvedDataset;

use crate::utils::ipfs_wrapper::{AddOptions, IpfsClient, PublishOptions};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct IpfsServiceImpl {
    ipfs_client: Arc<IpfsClient>,
}

#[component(pub)]
#[interface(dyn IpfsService)]
impl IpfsServiceImpl {
    pub fn new(ipfs_client: Arc<IpfsClient>) -> Self {
        Self { ipfs_client }
    }
}

#[async_trait::async_trait]
impl IpfsService for IpfsServiceImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(dataset = %dataset.get_handle()))]
    async fn add_dataset(&self, dataset: ResolvedDataset) -> Result<String, IpfsAddError> {
        let source_url = dataset.get_storage_internal_url();
        let source_path = source_url.to_file_path().map_err(|_| {
            IpfsAddError::UnsupportedIpfsStorageType(UnsupportedIpfsStorageTypeError {
                url: source_url.clone(),
            })
        })?;

        let cid = self
            .ipfs_client
            .add_path(
                source_path,
                AddOptions {
                    ignore: Some(&["/config", "/info"]),
                },
            )
            .await?;

        Ok(cid)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%key_id))]
    async fn find_ipns_key(&self, key_id: &str) -> Result<Option<IpnsKey>, InternalError> {
        let keys = self.ipfs_client.key_list().await?;
        Ok(keys.into_iter().find(|k| k.id == key_id).map(|k| IpnsKey {
            id: k.id,
            name: k.name,
        }))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(key_id = %key.id))]
    async fn resolve_ipns_name_locally(
        &self,
        key: &IpnsKey,
    ) -> Result<Option<String>, InternalError> {
        self.ipfs_client.name_resolve_local(&key.id).await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(key_name = %key.name, %cid, %check_resolves))]
    async fn publish_ipns_name(
        &self,
        key: &IpnsKey,
        cid: &str,
        check_resolves: bool,
    ) -> Result<(), InternalError> {
        self.ipfs_client
            .name_publish(
                cid,
                PublishOptions {
                    key: Some(&key.name),
                    resolve: Some(check_resolves),
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
