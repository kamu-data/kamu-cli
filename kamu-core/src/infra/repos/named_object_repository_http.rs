// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::repos::named_object_repository::{DeleteError, GetError, SetError};
use crate::domain::*;

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::Client;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct NamedObjectRepositoryHttp {
    client: Client,
    base_url: Url,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl NamedObjectRepositoryHttp {
    pub fn new(client: Client, base_url: Url) -> Self {
        assert!(
            !base_url.cannot_be_a_base()
                && (base_url.path().is_empty() || base_url.path().ends_with('/')),
            "Invalid base url: {}",
            base_url
        );
        Self { client, base_url }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl NamedObjectRepository for NamedObjectRepositoryHttp {
    async fn get(&self, name: &str) -> Result<Bytes, GetError> {
        let url = self.base_url.join(name).int_err()?;

        let response = self.client.get(url).send().await.int_err()?;

        let response = match response.error_for_status() {
            Ok(resp) => Ok(resp),
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                Err(GetError::NotFound(NotFoundError {
                    name: name.to_owned(),
                }))
            }
            Err(e) => Err(e.int_err().into()),
        }?;

        let data = response.bytes().await.int_err()?;

        Ok(data)
    }

    async fn set(&self, _name: &str, _data: &[u8]) -> Result<(), SetError> {
        Err(AccessError::ReadOnly(None).into())
    }

    async fn delete(&self, _name: &str) -> Result<(), DeleteError> {
        Err(AccessError::ReadOnly(None).into())
    }
}
