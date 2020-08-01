use crate::infra::serde::yaml::*;

use std::fs::File;
use std::path::Path;
use thiserror::Error;

#[derive(Debug)]
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
        fun: F,
    ) -> Result<Result<ExecutionResult<C>, E>, CheckpointingError>
    where
        C: Clone + serde::de::DeserializeOwned + serde::ser::Serialize,
        F: FnOnce(Option<C>) -> Result<ExecutionResult<C>, E>,
        E: std::error::Error,
    {
        let checkpoint_kind = std::any::type_name::<C>();
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

    fn read_checkpoint<C: serde::de::DeserializeOwned>(
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
            api_version: 1,
            kind: kind.to_owned(),
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
