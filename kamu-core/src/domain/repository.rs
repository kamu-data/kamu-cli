// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{Multihash, RemoteDatasetName};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use url::Url;

use std::backtrace::Backtrace;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Repository {
    pub url: Url,
}

#[async_trait::async_trait(?Send)]
pub trait RepositoryClient {
    async fn read_ref(
        &self,
        dataset_ref: &RemoteDatasetName,
    ) -> Result<Option<Multihash>, RepositoryError>;

    async fn write(
        &self,
        dataset_ref: &RemoteDatasetName,
        expected_head: &Option<Multihash>,
        new_head: &Multihash,
        blocks: &mut dyn Iterator<Item = (Multihash, Vec<u8>)>,
        data_files: &mut dyn Iterator<Item = &Path>,
        checkpoint_dir: &Path,
    ) -> Result<(), RepositoryError>;

    async fn read(
        &self,
        dataset_ref: &RemoteDatasetName,
        expected_head: &Multihash,
        last_seen_block: &Option<Multihash>,
        tmp_dir: &Path,
    ) -> Result<RepositoryReadResult, RepositoryError>;

    /// Deletes a dataset from the repository.
    ///
    /// Note: Some repos may not permit this operation.
    async fn delete(&self, dataset_ref: &RemoteDatasetName) -> Result<(), RepositoryError>;

    async fn search(&self, query: Option<&str>) -> Result<RepositorySearchResult, RepositoryError>;
}

pub struct RepositoryReadResult {
    pub blocks: Vec<(Multihash, Vec<u8>)>,
    pub data_files: Vec<PathBuf>,
    pub checkpoint_dir: PathBuf,
}

pub struct RepositorySearchResult {
    // TODO: REMOTE ID: Should be a RemoteDatasetHandle
    pub datasets: Vec<RemoteDatasetName>,
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("Dataset does not exist")]
    DoesNotExist,
    #[error("Operation is not allowed")]
    NotAllowed,
    #[error("Local and remote datasets have diverged. Local head: {local_head}, remote head {remote_head}")]
    Diverged {
        local_head: Multihash,
        remote_head: Multihash,
        //uncommon_blocks_in_local: usize,
        //uncommon_blocks_in_remote: usize,
    },
    #[error("Dataset was updated concurrently")]
    UpdatedConcurrently,
    #[error("Repository appears to have corrupted data: {message}")]
    Corrupted {
        message: String,
        #[source]
        source: Option<BoxedError>,
    },
    #[error("IO error: {source}")]
    IOError {
        #[from]
        source: std::io::Error,
        #[backtrace]
        backtrace: Backtrace,
    },
    #[error("Credentials error: {source}")]
    CredentialsError {
        #[source]
        source: BoxedError,
        #[backtrace]
        backtrace: Backtrace,
    },
    #[error("Protocol error: {source}")]
    ProtocolError {
        #[source]
        source: BoxedError,
        #[backtrace]
        backtrace: Backtrace,
    },
}

impl RepositoryError {
    pub fn credentials(e: BoxedError) -> Self {
        Self::CredentialsError {
            source: e,
            backtrace: Backtrace::capture(),
        }
    }

    pub fn protocol(e: BoxedError) -> Self {
        Self::ProtocolError {
            source: e,
            backtrace: Backtrace::capture(),
        }
    }

    pub fn corrupted(message: String) -> Self {
        Self::Corrupted {
            message: message,
            source: None,
        }
    }

    pub fn corrupted_from<E: std::error::Error + Send + Sync + 'static>(
        message: String,
        source: E,
    ) -> Self {
        Self::Corrupted {
            message: message,
            source: Some(source.into()),
        }
    }
}
