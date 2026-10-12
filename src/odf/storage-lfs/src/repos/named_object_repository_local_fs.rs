// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use odf_storage::*;

use super::StagingFile;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct NamedObjectRepositoryLocalFS {
    root: PathBuf,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl NamedObjectRepositoryLocalFS {
    pub fn new<P>(root: P) -> Self
    where
        P: Into<PathBuf>,
    {
        let root = root.into();
        Self { root }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait]
impl NamedObjectRepository for NamedObjectRepositoryLocalFS {
    #[tracing::instrument(level = "debug", name = NamedObjectRepositoryLocalFS_get, skip_all, fields(%name))]
    async fn get(&self, name: &str) -> Result<Bytes, GetNamedError> {
        let data = match tokio::fs::read(self.root.join(name)).await {
            Ok(data) => Ok(data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(GetNamedError::NotFound(NotFoundError {
                    name: name.to_owned(),
                }))
            }
            Err(e) => Err(e.int_err().into()),
        }?;

        Ok(Bytes::from(data))
    }

    #[tracing::instrument(level = "debug", name = NamedObjectRepositoryLocalFS_set, skip_all, fields(%name))]
    async fn set(&self, name: &str, data: &[u8]) -> Result<(), SetNamedError> {
        let staging_file = StagingFile::new_in(&self.root).int_err()?;
        tokio::fs::write(staging_file.path(), data)
            .await
            .int_err()?;
        staging_file.commit(&self.root.join(name)).int_err()?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", name = NamedObjectRepositoryLocalFS_delete, skip_all, fields(%name))]
    async fn delete(&self, name: &str) -> Result<(), DeleteNamedError> {
        match std::fs::remove_file(self.root.join(name)) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.int_err().into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
