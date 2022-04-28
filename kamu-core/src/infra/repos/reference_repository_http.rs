// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::repos::reference_repository::InternalError;
use crate::domain::*;
use opendatafabric::Multihash;

use async_trait::async_trait;
use reqwest::Client;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct ReferenceRepositoryHttp {
    client: Client,
    base_url: Url,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl ReferenceRepositoryHttp {
    pub fn new(client: Client, base_url: Url) -> Self {
        Self { client, base_url }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl ReferenceRepository for ReferenceRepositoryHttp {
    async fn get(&self, r: &BlockRef) -> Result<Multihash, GetRefError> {
        let url = self
            .base_url
            .join(&r.to_string())
            .map_err(|e| GetRefError::Internal(e.into()))?;

        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| GetRefError::Internal(e.into()))?;

        let response = match response.error_for_status() {
            Ok(resp) => Ok(resp),
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                Err(GetRefError::NotFound(RefNotFoundError(r.clone())))
            }
            Err(e) => Err(GetRefError::Internal(e.into())),
        }?;

        let multibase = response
            .text()
            .await
            .map_err(|e| GetRefError::Internal(e.into()))?;

        Multihash::from_multibase_str(&multibase).map_err(|e| GetRefError::Internal(e.into()))
    }

    async fn set(&self, _r: &BlockRef, _hash: &Multihash) -> Result<(), InternalError> {
        panic!("Http reference repository is read-only")
    }

    async fn delete(&self, _r: &BlockRef) -> Result<(), InternalError> {
        panic!("Http reference repository is read-only")
    }
}
