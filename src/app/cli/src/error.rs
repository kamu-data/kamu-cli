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

use graphql_http::GraphqlHttpRequestError;
use internal_error::{BoxedError, InternalError};
use kamu::domain::engine::normalize_logs;
use kamu::domain::*;
use kamu_auth_rebac::RebacDatasetRefUnresolvedError;
use kamu_datasets::{
    DeleteDatasetError,
    DeleteDatasetPlanEvaluationError,
    DeleteDatasetPlanningError,
};
use kamu_resources_facade::{
    ApplyManifestError,
    BatchResourceError,
    DeleteResourceError,
    GetResourceError,
    ListAllResourcesError,
    ListResourcesError,
    ListSupportedResourceKindsError,
    RenderResourceManifestError,
    ResourceLookupProblem,
    ResourcesSummaryError,
};
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
            e @ DeleteDatasetError::Internal(_) => Self::critical(e),
        }
    }
}

impl From<DeleteDatasetPlanningError> for CLIError {
    fn from(v: DeleteDatasetPlanningError) -> Self {
        match v {
            e @ DeleteDatasetPlanningError::Internal(_) => Self::critical(e),
        }
    }
}

impl From<DeleteDatasetPlanEvaluationError> for CLIError {
    fn from(v: DeleteDatasetPlanEvaluationError) -> Self {
        match v {
            e @ (DeleteDatasetPlanEvaluationError::NotFound(_)
            | DeleteDatasetPlanEvaluationError::DanglingReference(_)
            | DeleteDatasetPlanEvaluationError::Access(_)) => Self::failure(e),
            e @ DeleteDatasetPlanEvaluationError::Internal(_) => Self::critical(e),
        }
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

impl From<GraphqlHttpRequestError> for CLIError {
    fn from(e: GraphqlHttpRequestError) -> Self {
        match e {
            GraphqlHttpRequestError::Transport { .. }
            | GraphqlHttpRequestError::HttpStatus { .. }
            | GraphqlHttpRequestError::Graphql { .. } => Self::failure(e),
            GraphqlHttpRequestError::Internal(e) => Self::critical(e),
        }
    }
}

impl From<ResourcesSummaryError> for CLIError {
    fn from(e: ResourcesSummaryError) -> Self {
        use ResourcesSummaryError as E;
        match e {
            e @ E::BadAccount(_) => Self::failure(e),
            E::RemoteRequest(err) => Self::from(err),
            E::Internal(err) => Self::critical(err),
        }
    }
}

impl From<ListSupportedResourceKindsError> for CLIError {
    fn from(e: ListSupportedResourceKindsError) -> Self {
        match e {
            ListSupportedResourceKindsError::RemoteRequest(err) => Self::from(err),
            ListSupportedResourceKindsError::Internal(err) => Self::critical(err),
        }
    }
}

impl From<ListResourcesError> for CLIError {
    fn from(e: ListResourcesError) -> Self {
        use ListResourcesError as E;

        match e {
            e @ (E::UnsupportedDescriptor(_) | E::BadAccount(_) | E::InvalidSearchQuery(_)) => {
                Self::failure(e)
            }
            E::RemoteRequest(err) => Self::from(err),
            E::Internal(err) => Self::critical(err),
        }
    }
}

impl From<ListAllResourcesError> for CLIError {
    fn from(e: ListAllResourcesError) -> Self {
        use ListAllResourcesError as E;

        match e {
            e @ E::BadAccount(_) => Self::failure(e),
            E::RemoteRequest(err) => Self::from(err),
            E::Internal(err) => Self::critical(err),
        }
    }
}

impl From<BatchResourceError> for CLIError {
    fn from(e: BatchResourceError) -> Self {
        use BatchResourceError as E;

        match e {
            e @ (E::UnsupportedDescriptor(_) | E::BadAccount(_)) => Self::failure(e),
            E::RemoteRequest(err) => Self::from(err),
            E::Internal(err) => Self::critical(err),
        }
    }
}

impl From<DeleteResourceError> for CLIError {
    fn from(e: DeleteResourceError) -> Self {
        use DeleteResourceError as E;

        match e {
            E::LookupProblem(problem) => Self::failure(ResourceLookupCliError::from(problem)),
            e @ (E::UnsupportedDescriptor(_) | E::BadAccount(_)) => Self::failure(e),
            E::RemoteRequest(err) => Self::from(err),
            E::Internal(err) => Self::critical(err),
        }
    }
}

impl From<GetResourceError> for CLIError {
    fn from(e: GetResourceError) -> Self {
        use GetResourceError as E;

        match e {
            E::LookupProblem(problem) => Self::failure(ResourceLookupCliError::from(problem)),
            e @ (E::UnsupportedDescriptor(_) | E::BadAccount(_)) => Self::failure(e),
            E::RemoteRequest(err) => Self::from(err),
            E::Internal(err) => Self::critical(err),
        }
    }
}

impl From<RenderResourceManifestError> for CLIError {
    fn from(e: RenderResourceManifestError) -> Self {
        use RenderResourceManifestError as E;

        match e {
            E::LookupProblem(problem) => Self::failure(ResourceLookupCliError::from(problem)),
            e @ (E::UnsupportedDescriptor(_) | E::BadAccount(_)) => Self::failure(e),
            E::RemoteRequest(err) => Self::from(err),
            E::Internal(err) => Self::critical(err),
        }
    }
}

impl From<ApplyManifestError> for CLIError {
    fn from(e: ApplyManifestError) -> Self {
        use ApplyManifestError as E;
        match e {
            e @ (E::ParseManifest(_)
            | E::UnsupportedDescriptor(_)
            | E::BadAccount(_)
            | E::InvalidHeaders(_)
            | E::InvalidSpec(_)
            | E::IDNotFound(_)
            | E::TypeMismatch(_)
            | E::ConcurrentModification(_)) => Self::failure(e),
            E::RemoteRequest(err) => Self::from(err),
            E::Internal(err) => Self::critical(err),
        }
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
enum ResourceLookupCliError {
    #[error("Resource with id {0} was not found")]
    IDNotFound(kamu_resources::ResourceID),

    #[error("Resource '{name}' of kind '{kind}' was not found")]
    NameNotFound { kind: String, name: String },

    #[error("Resource id {id} refers to schema '{actual_schema}', expected '{expected_schema}'")]
    SchemaMismatch {
        id: kamu_resources::ResourceID,
        expected_schema: String,
        actual_schema: String,
    },
}

impl From<ResourceLookupProblem> for ResourceLookupCliError {
    fn from(problem: ResourceLookupProblem) -> Self {
        match problem {
            ResourceLookupProblem::IDNotFound(err) => Self::IDNotFound(err.0),
            ResourceLookupProblem::NameNotFound(err) => Self::NameNotFound {
                kind: kamu_resources::ResourceSchema::display_name(&err.kind).to_string(),
                name: err.name,
            },
            ResourceLookupProblem::SchemaMismatch(err) => Self::SchemaMismatch {
                id: err.id,
                expected_schema: kamu_resources::ResourceSchema::display_name(&err.expected_schema)
                    .to_string(),
                actual_schema: kamu_resources::ResourceSchema::display_name(&err.actual_schema)
                    .to_string(),
            },
        }
    }
}

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
