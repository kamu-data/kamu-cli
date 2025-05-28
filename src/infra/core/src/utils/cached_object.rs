// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use std::path::PathBuf;

use internal_error::{InternalError, ResultIntoInternal};
use tempfile::{TempDir, tempdir};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CachedObject {
    object_state: ObjectState,
}

enum ObjectState {
    Local {
        path: PathBuf,
    },
    Temp {
        _temp_dir: TempDir,
        temp_file_path: PathBuf,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl CachedObject {
    pub async fn from(
        object_hash: &odf::Multihash,
        object_repository: &dyn odf::storage::ObjectRepository,
    ) -> Result<Self, InternalError> {
        let object_file_url = object_repository.get_internal_url(object_hash).await;
        if let Ok(local_path) = object_file_url.to_file_path() {
            Ok(Self {
                object_state: ObjectState::Local {
                    path: local_path.clone(),
                },
            })
        } else {
            let temp_dir = tempdir().int_err()?;
            let temp_file_path = temp_dir.path().join("data");
            let mut temp_file = tokio::fs::File::create(temp_file_path.clone())
                .await
                .int_err()?;

            let mut data_stream = object_repository.get_stream(object_hash).await.int_err()?;

            let mut buf = [0; 4096];
            loop {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};

                let read = data_stream.read(&mut buf).await.int_err()?;
                if read == 0 {
                    temp_file.flush().await.int_err()?;
                    break;
                }

                temp_file.write_all(&buf[..read]).await.int_err()?;
            }

            Ok(Self {
                object_state: ObjectState::Temp {
                    _temp_dir: temp_dir,
                    temp_file_path,
                },
            })
        }
    }

    pub async fn logical_hash(
        &self,
    ) -> Result<odf::Multihash, datafusion::parquet::errors::ParquetError> {
        let path = self.storage_path().clone();
        tokio::task::spawn_blocking(move || odf::utils::data::hash::get_parquet_logical_hash(&path))
            .await
            .unwrap()
    }

    pub async fn physical_hash(&self) -> Result<odf::Multihash, std::io::Error> {
        let path = self.storage_path().clone();
        tokio::task::spawn_blocking(move || odf::utils::data::hash::get_file_physical_hash(&path))
            .await
            .unwrap()
    }

    pub fn storage_path(&self) -> &PathBuf {
        match &self.object_state {
            ObjectState::Local { path } => path,
            ObjectState::Temp {
                _temp_dir,
                temp_file_path,
            } => temp_file_path,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
