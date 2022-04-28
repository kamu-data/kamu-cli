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
use std::path::PathBuf;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct ReferenceRepositoryLocalFS {
    root: PathBuf,
    staging_path: PathBuf,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl ReferenceRepositoryLocalFS {
    pub fn new<P>(root: P) -> Self
    where
        P: Into<PathBuf>,
    {
        let root = root.into();
        Self {
            staging_path: root.join(".pending"),
            root,
        }
    }

    fn get_path(&self, r: &BlockRef) -> PathBuf {
        self.root.join(r.to_string())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl ReferenceRepository for ReferenceRepositoryLocalFS {
    async fn get(&self, r: &BlockRef) -> Result<Multihash, GetRefError> {
        let path = self.get_path(r);
        if !path.exists() {
            return Err(GetRefError::NotFound(RefNotFoundError(r.clone())));
        }

        let multibase = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| GetRefError::Internal(e.into()))?;

        Multihash::from_multibase_str(&multibase).map_err(|e| GetRefError::Internal(e.into()))
    }

    async fn set(&self, r: &BlockRef, hash: &Multihash) -> Result<(), InternalError> {
        let multibase = hash.to_multibase_string();
        tokio::fs::write(&self.staging_path, multibase.as_bytes()).await?;

        // Atomic move/replace
        std::fs::rename(&self.staging_path, self.get_path(r))?;

        Ok(())
    }

    async fn delete(&self, r: &BlockRef) -> Result<(), InternalError> {
        match std::fs::remove_file(self.get_path(r)) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}
