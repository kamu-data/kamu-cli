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
use odf_metadata::AccessError;
use odf_storage::*;
use reqwest::Client;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct NamedObjectRepositoryHttp {
    client: Client,
    base_url: Url,
    header_map: http::HeaderMap,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl NamedObjectRepositoryHttp {
    pub fn new(client: Client, base_url: Url, header_map: http::HeaderMap) -> Self {
        assert!(
            !base_url.cannot_be_a_base()
                && (base_url.path().is_empty() || base_url.path().ends_with('/')),
            "Invalid base url: {base_url}"
        );
        Self {
            client,
            base_url,
            header_map,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait]
impl NamedObjectRepository for NamedObjectRepositoryHttp {
    #[tracing::instrument(level = "debug", name = NamedObjectRepositoryHttp_get, skip_all, fields(%name))]
    async fn get(&self, name: &str) -> Result<Bytes, GetNamedError> {
        let url = self.base_url.join(name).int_err()?;

        let response = self
            .client
            .get(url)
            .headers(self.header_map.clone())
            .send()
            .await
            .int_err()?;

        let response = match response.error_for_status() {
            Ok(resp) => Ok(resp),
            Err(e) if e.status() == Some(http::StatusCode::NOT_FOUND) => {
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
