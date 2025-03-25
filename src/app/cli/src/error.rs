// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;
use std::fmt::Display;
use std::path::PathBuf;

use internal_error::{BoxedError, InternalError};
use kamu::domain::engine::normalize_logs;
use kamu::domain::*;
use kamu_auth_rebac::RebacDatasetRefUnresolvedError;
use kamu_datasets::DeleteDatasetError;
use odf::utils::data::format::WriterError;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct BatchError {
    pub summary: String,
    pub errors_with_context: Vec<(BoxedError, String)>,
}

impl BatchError {
    pub fn new<S, I, C, E>(summary: S, errors_with_context: I) -> Self
    where
        S: Into<String>,
        I: IntoIterator<Item = (E, C)>,
        C: Into<String>,
        E: Into<BoxedError>,
    {
        Self {
            summary: summary.into(),
            errors_with_context: errors_with_context
                .into_iter()
                .map(|(e, c)| (e.into(), c.into()))
                .collect(),
        }
    }
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.summary)
    }
}

impl std::error::Error for BatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    /// Indicates that an operation has failed while some changes could've
    /// already been applied
    #[error("Partial failure")]
    PartialFailure,
    #[error("{source}")]
    CriticalFailure {
        source: BoxedError,
        backtrace: Backtrace,
    },
    #[error(transparent)]
    BatchError(
        #[from]
        #[backtrace]
        BatchError,
    ),
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

    pub fn missed_env_var<T>(var_name: T) -> Self
    where
        T: Into<String>,
    {
        Self::usage_error_from(RequiredEnvVarNotSet {
            var_name: var_name.into(),
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

    pub fn pretty(&self, include_backtraces: bool) -> impl Display + '_ {
        super::error_fmt::PrettyCLIError {
            error: self,
            include_backtraces,
        }
    }
}

impl From<std::io::Error> for CLIError {
    fn from(e: std::io::Error) -> Self {
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

impl From<MultiTenantRefUnexpectedError> for CLIError {
    fn from(e: MultiTenantRefUnexpectedError) -> Self {
        Self::usage_error_from(e)
    }
}

impl From<WriterError> for CLIError {
    fn from(e: WriterError) -> Self {
        Self::failure(e)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TODO: Replace with traits that distinguish critical and non-critical errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::DatasetRefUnresolvedError> for CLIError {
    fn from(v: odf::DatasetRefUnresolvedError) -> Self {
        match v {
            e @ odf::DatasetRefUnresolvedError::NotFound(_) => Self::failure(e),
            e @ odf::DatasetRefUnresolvedError::Internal(_) => Self::critical(e),
        }
    }
}

impl From<GetAliasesError> for CLIError {
    fn from(v: GetAliasesError) -> Self {
        match v {
            e @ GetAliasesError::Internal(_) => Self::critical(e),
        }
    }
}

impl From<DeleteDatasetError> for CLIError {
    fn from(v: DeleteDatasetError) -> Self {
        match v {
            e @ (DeleteDatasetError::NotFound(_)
            | DeleteDatasetError::DanglingReference(_)
            | DeleteDatasetError::Access(_)) => Self::failure(e),
            e @ DeleteDatasetError::Internal(_) => Self::critical(e),
        }
    }
}

impl From<odf::dataset::GetSummaryError> for CLIError {
    fn from(e: odf::dataset::GetSummaryError) -> Self {
        Self::critical(e)
    }
}

impl From<odf::GetRefError> for CLIError {
    fn from(e: odf::GetRefError) -> Self {
        Self::critical(e)
    }
}

impl From<odf::IterBlocksError> for CLIError {
    fn from(v: odf::IterBlocksError) -> Self {
        match v {
            e @ odf::IterBlocksError::BlockVersion(_) => Self::failure(e),
            _ => Self::critical(v),
        }
    }
}

impl From<InternalError> for CLIError {
    fn from(e: InternalError) -> Self {
        Self::critical(e)
    }
}

impl From<ExportError> for CLIError {
    fn from(e: ExportError) -> Self {
        match e {
            ExportError::Internal(_) => Self::critical(e),
        }
    }
}

impl From<RebacDatasetRefUnresolvedError> for CLIError {
    fn from(e: RebacDatasetRefUnresolvedError) -> Self {
        use RebacDatasetRefUnresolvedError as E;
        match e {
            E::NotFound(e) => Self::failure(e),
            E::Access(e) => Self::failure(e),
            e @ E::Internal(_) => Self::critical(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Directory is already a kamu workspace")]
pub struct AlreadyInWorkspace;

#[derive(Debug, Error)]
#[error("Directory is not a kamu workspace")]
pub struct NotInWorkspace;

#[derive(Debug, Error)]
#[error("Directory is not a multi-tenant kamu workspace")]
pub struct NotInMultiTenantWorkspace;

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Multi-tenant reference is unexpected in single-tenant workspace: {dataset_ref_pattern}")]
pub struct MultiTenantRefUnexpectedError {
    pub dataset_ref_pattern: odf::DatasetRefPattern,
}

#[derive(Debug, Error)]
#[error("Command interpretation failed")]
pub struct CommandInterpretationFailed;

#[derive(Debug, Error)]
#[error(
    "Workspace needs to be upgraded before continuing - please run `kamu system upgrade-workspace`"
)]
pub struct WorkspaceUpgradeRequired;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Environment variable {var_name} is not set")]
pub struct RequiredEnvVarNotSet {
    pub var_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CommandRunError {
    #[error(transparent)]
    SubprocessError(#[from] SubprocessError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

#[derive(Error, Debug)]
pub struct SubprocessError {
    pub source: BoxedError,
    pub log_files: Vec<PathBuf>,
}

impl SubprocessError {
    pub fn new(log_files: Vec<PathBuf>, e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            log_files: normalize_logs(log_files),
            source: e.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
