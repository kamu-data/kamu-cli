// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use odf_storage::*;

use super::StagingFile;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const LOCK_ATTEMPTS: u32 = 50;
const LOCK_RETRY_DELAY: Duration = Duration::from_millis(20);

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

    /// Compares and writes under a lock file `{name}.lock`, created exclusively
    /// next to the object, the way git locks its references. The new content
    /// is written into the lock file, which is then renamed over the object,
    /// so the lock disappears with the write; a mismatch removes it. Both
    /// steps are portable. The lock orders only writers that take it, not
    /// `set`. A lock still held after the retries is reported with its path:
    /// a writer that was interrupted leaves it behind, and it must be removed
    fn compare_and_set_blocking(
        root: &Path,
        name: &str,
        expected: Option<&[u8]>,
        data: &[u8],
    ) -> Result<(), CompareAndSetNamedError> {
        std::fs::create_dir_all(root).int_err()?;

        let (lock, mut lock_file) = Self::acquire_lock(&root.join(format!("{name}.lock")))?;

        let target = root.join(name);
        let actual = match std::fs::read(&target) {
            Ok(actual) => Some(actual),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.int_err().into()),
        };
        if actual.as_deref() != expected {
            return Err(CompareAndSetNamedError::Mismatch(
                NamedObjectMismatchError {
                    name: name.to_owned(),
                },
            ));
        }

        lock_file.write_all(data).int_err()?;
        // Closed before the rename, which some platforms refuse on an open file
        drop(lock_file);
        lock.commit(&target).int_err()?;

        Ok(())
    }

    fn acquire_lock(
        lock_path: &Path,
    ) -> Result<(StagingFile, std::fs::File), CompareAndSetNamedError> {
        for _ in 0..LOCK_ATTEMPTS {
            match StagingFile::create_new(lock_path.to_path_buf()) {
                Ok(lock) => return Ok(lock),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    std::thread::sleep(LOCK_RETRY_DELAY);
                }
                Err(e) => return Err(e.int_err().into()),
            }
        }

        Err(format!(
            "Lock file {} is held by another writer; if no writer is running, it was left by an \
             interrupted one and can be removed",
            lock_path.display()
        )
        .int_err()
        .into())
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

    #[tracing::instrument(level = "debug", name = NamedObjectRepositoryLocalFS_compare_and_set, skip_all, fields(%name))]
    async fn compare_and_set(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        data: &[u8],
    ) -> Result<(), CompareAndSetNamedError> {
        let root = self.root.clone();
        let name = name.to_owned();
        let expected = expected.map(<[u8]>::to_vec);
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || {
            Self::compare_and_set_blocking(&root, &name, expected.as_deref(), &data)
        })
        .await
        .int_err()?
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
