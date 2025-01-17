// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use odf_metadata::*;
use odf_storage::*;
use reqwest::Client;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct NamedObjectRepositoryIpfsHttp {
    client: Client,
    base_url: Url,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Similar to `NamedObjectRepositoryHttp` but has error handling special-cased
/// for IPFS gateway
impl NamedObjectRepositoryIpfsHttp {
    pub fn new(client: Client, base_url: Url) -> Self {
        assert!(
            !base_url.cannot_be_a_base()
                && (base_url.path().is_empty() || base_url.path().ends_with('/')),
            "Invalid base url: {base_url}"
        );
        Self { client, base_url }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl NamedObjectRepository for NamedObjectRepositoryIpfsHttp {
    #[tracing::instrument(level = "debug", skip_all, fields(%name))]
    async fn get(&self, name: &str) -> Result<Bytes, GetNamedError> {
        let url = self.base_url.join(name).int_err()?;

        let response = self.client.get(url).send().await.int_err()?;

        let response = match response.error_for_status() {
            Ok(resp) => Ok(resp),
            // When unable to find CID:
            // - Kubo version <= 0.14.0 returns 404
            // - Kubo 0.15.0 <= version < 0.19.0 returns 400 - we consider this a bug and don't
            //   support them
            // - Kubo version >= 0.19.0 returns 500 - very ambiguous, but will have to do :(
            //
            // See: https://github.com/ipfs/kubo/issues/9514
            //
            // Note that we only do this in named repository as found / not-found matters most when
            // we are checking for /refs/head to see if dataset exists. We prefer to get internal
            // error on non-existing block rather than risk confusing missing block with
            // actual server errors from Kubo.
            Err(e)
                if e.status() == Some(http::StatusCode::NOT_FOUND)
                    || e.status() == Some(http::StatusCode::INTERNAL_SERVER_ERROR) =>
            {
                Err(GetNamedError::NotFound(NotFoundError {
                    name: name.to_owned(),
                }))
            }
            Err(e) => Err(e.int_err().into()),
        }?;

        let data = response.bytes().await.int_err()?;

        Ok(data)
    }

    async fn set(&self, _name: &str, _data: &[u8]) -> Result<(), SetNamedError> {
        Err(AccessError::ReadOnly(None).into())
    }

    async fn delete(&self, _name: &str) -> Result<(), DeleteNamedError> {
        Err(AccessError::ReadOnly(None).into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
