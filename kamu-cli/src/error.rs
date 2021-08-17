use kamu::domain::DomainError;
use std::{backtrace::Backtrace, fmt::Display};
use thiserror::Error;

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum CLIError {
    #[error("{0}")]
    UsageError(UsageError),
    /// Indicates that an operation was aborted and no changes were made
    #[error("Operation aborted")]
    Aborted,
    #[error("{source}")]
    Failure {
        source: BoxedError,
        backtrace: Backtrace,
    },
    /// Indicates that an operation has failed while some changes could've already been applied
    #[error("Partial failure")]
    PartialFailure,
    #[error("{source}")]
    CriticalFailure {
        source: BoxedError,
        backtrace: Backtrace,
    },
}

impl CLIError {
    pub fn usage_error<S: Into<String>>(msg: S) -> Self {
        Self::UsageError(UsageError {
            msg: Some(msg.into()),
            source: None,
            backtrace: Backtrace::capture(),
        })
    }

    pub fn usage_error_from(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::UsageError(UsageError {
            msg: None,
            source: Some(e.into()),
            backtrace: Backtrace::capture(),
        })
    }

    pub fn failure(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Failure {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }

    pub fn critical(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::CriticalFailure {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<std::io::Error> for CLIError {
    fn from(e: std::io::Error) -> Self {
        Self::failure(e)
    }
}

impl From<DomainError> for CLIError {
    fn from(e: DomainError) -> Self {
        Self::failure(e)
    }
}

impl From<dill::InjectionError> for CLIError {
    fn from(e: dill::InjectionError) -> Self {
        Self::critical(e)
    }
}

impl From<CommandInterpretationFailed> for CLIError {
    fn from(e: CommandInterpretationFailed) -> Self {
        Self::critical(e)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub struct UsageError {
    msg: Option<String>,
    source: Option<BoxedError>,
    backtrace: Backtrace,
}

impl Display for UsageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(source) = &self.source {
            source.fmt(f)
        } else {
            f.write_str(self.msg.as_ref().unwrap())
        }
    }
}

#[derive(Debug, Error)]
#[error("Directory is already a kamu workspace")]
pub struct AlreadyInWorkspace;

#[derive(Debug, Error)]
#[error("Directory is not a kamu workspace")]
pub struct NotInWorkspace;

#[derive(Debug, Error)]
#[error("Command interpretation failed")]
pub struct CommandInterpretationFailed;
