use std::backtrace::Backtrace;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    DomainError(#[from] kamu::domain::Error),
    #[error("Directory is already a kamu workspace")]
    AlreadyInWorkspace,
    #[error("Directory is not a kamu workspace")]
    NotInWorkspace,
    #[error("Runtime IO error: {source}")]
    RuntimeIOError {
        #[from]
        source: std::io::Error,
        backtrace: Backtrace,
    },
}
