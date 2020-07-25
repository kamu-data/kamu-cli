use std::backtrace::Backtrace;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{msg}")]
    UsageError { msg: String },
    #[error("{0}")]
    DomainError(#[from] kamu::domain::DomainError),
    #[error("Directory is already a kamu workspace")]
    AlreadyInWorkspace,
    #[error("Directory is not a kamu workspace")]
    NotInWorkspace,
    #[error("Operation aborted")]
    Aborted,
    #[error("IO error: {source}")]
    IOError {
        #[from]
        source: std::io::Error,
        backtrace: Backtrace,
    },
}
