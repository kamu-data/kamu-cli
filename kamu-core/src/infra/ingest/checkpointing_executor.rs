// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::Manifest;

use std::fs::File;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct ExecutionResult<C> {
    pub was_up_to_date: bool,
    pub checkpoint: C,
}

pub struct CheckpointingExecutor;

impl CheckpointingExecutor {
    pub fn new() -> Self {
        Self {}
    }

    pub fn execute<C, F, E>(
        &self,
        checkpoint_path: &Path,
        checkpoint_kind: &str,
        fun: F,
    ) -> Result<Result<ExecutionResult<C>, E>, CheckpointingError>
    where
        C: Clone + serde::de::DeserializeOwned + serde::ser::Serialize,
        F: FnOnce(Option<C>) -> Result<ExecutionResult<C>, E>,
        E: std::error::Error,
    {
        let old_checkpoint = self.read_checkpoint(checkpoint_path, checkpoint_kind)?;

        match fun(old_checkpoint) {
            Ok(exec_res) => {
                if !exec_res.was_up_to_date {
                    self.write_checkpoint(
                        checkpoint_path,
                        exec_res.checkpoint.clone(),
                        checkpoint_kind,
                    )?;
                }
                Ok(Ok(exec_res))
            }
            err @ Err(_) => Ok(err),
        }
    }

    pub fn read_checkpoint<C: serde::de::DeserializeOwned>(
        &self,
        path: &Path,
        kind: &str,
    ) -> Result<Option<C>, CheckpointingError> {
        if !path.exists() {
            Ok(None)
        } else {
            let file = File::open(&path)?;
            let manifest: Manifest<C> = serde_yaml::from_reader(file)?;
            assert_eq!(manifest.kind, kind);
            Ok(Some(manifest.content))
        }
    }

    fn write_checkpoint<C: serde::ser::Serialize>(
        &self,
        path: &Path,
        checkpoint: C,
        kind: &str,
    ) -> Result<(), CheckpointingError> {
        let manifest = Manifest {
            kind: kind.to_owned(),
            version: 1,
            content: checkpoint,
        };
        let file = File::create(path)?;
        serde_yaml::to_writer(file, &manifest)?;
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum CheckpointingError {
    #[error("{0}")]
    IOError(#[from] std::io::Error),
    #[error("{0}")]
    SerdeError(#[from] serde_yaml::Error),
}
