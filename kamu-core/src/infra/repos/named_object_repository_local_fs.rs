// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::repos::named_object_repository::GetError;
use crate::domain::*;

use async_trait::async_trait;
use bytes::Bytes;
use std::path::PathBuf;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct NamedObjectRepositoryLocalFS {
    root: PathBuf,
    staging_path: PathBuf,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl NamedObjectRepositoryLocalFS {
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
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl NamedObjectRepository for NamedObjectRepositoryLocalFS {
    async fn get(&self, name: &str) -> Result<Bytes, GetError> {
        let data = match tokio::fs::read(self.root.join(name)).await {
            Ok(data) => Ok(data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(GetError::NotFound(NotFoundError {
                    name: name.to_owned(),
                }))
            }
            Err(e) => Err(e.into_internal_error().into()),
        }?;

        Ok(Bytes::from(data))
    }

    async fn set(&self, name: &str, data: &[u8]) -> Result<(), InternalError> {
        tokio::fs::write(&self.staging_path, data)
            .await
            .into_internal_error()?;

        // Atomic move/replace
        std::fs::rename(&self.staging_path, self.root.join(name)).into_internal_error()?;

        Ok(())
    }

    async fn delete(&self, name: &str) -> Result<(), InternalError> {
        match std::fs::remove_file(self.root.join(name)) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into_internal_error()),
        }
    }
}
