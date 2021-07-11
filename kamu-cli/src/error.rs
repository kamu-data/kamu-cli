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
    /// Indicates that an operation was aborted and no changes were made
    #[error("Operation aborted")]
    Aborted,
    /// Indicates that an operation has failed while some changes were already applied
    #[error("Partial failure")]
    PartialFailure,
    #[error("IO error: {source}")]
    IOError {
        #[from]
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[error("Unsuccessful exit status: {source}")]
    ExitStatusError {
        #[from]
        source: std::process::ExitStatusError,
        backtrace: Backtrace,
    },
}
