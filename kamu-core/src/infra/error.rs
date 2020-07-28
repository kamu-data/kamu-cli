use crate::domain::DomainError;

use std::backtrace::Backtrace;
use std::convert::From;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum InfraError {
    #[error("IO error: {source}")]
    IOError {
        #[from]
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[error("{0}")]
    SerdeError(#[from] SerdeError),
}

#[derive(Error, Debug)]
pub enum SerdeError {
    #[error("Yaml serialization error: {source}")]
    SerdeYamlError {
        #[from]
        source: serde_yaml::Error,
        backtrace: Backtrace,
    },
}

impl From<serde_yaml::Error> for InfraError {
    fn from(v: serde_yaml::Error) -> Self {
        Self::from(SerdeError::from(v))
    }
}

impl std::convert::Into<DomainError> for InfraError {
    fn into(self) -> DomainError {
        DomainError::InfraError(Box::new(self))
    }
}
