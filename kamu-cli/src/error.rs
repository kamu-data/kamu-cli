// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use std::backtrace::{Backtrace, BacktraceStatus};
use std::error::Error;
use std::fmt::Display;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////

// TODO: Replace with Display
pub fn display_cli_error(err: &CLIError) {
    match err {
        CLIError::BatchError(b) => print_batch_error(b),
        e => print_error(e),
    }

    if let CLIError::CriticalFailure { .. } = err {
        eprintln!(
            "\nHelp us by reporting this problem at https://github.com/kamu-data/kamu-cli/issues"
        );
    }
}

///////////////////////////////////////////////////////////////////////////////

pub fn print_error(error: &dyn Error) {
    let mut error_chain = Vec::new();
    let mut err_ref: Option<&dyn Error> = Some(error);
    while let Some(e) = err_ref {
        error_chain.push(e);
        err_ref = e.source();
    }

    let mut error_messages: Vec<_> = error_chain.iter().map(|e| format!("{}", e)).collect();
    unchain_error_messages(&mut error_messages);

    if error_messages.len() == 1 {
        eprintln!(
            "{}: {}",
            console::style("Error").red().bold(),
            error_messages[0]
        );
    } else {
        eprintln!("{}:", console::style("Error").red().bold());

        for (i, e) in error_messages.iter().enumerate() {
            eprintln!("  {} {}", console::style(format!("{}:", i)).dim(), e);
        }
    }

    // Print the inner most backtrace
    for e in error_chain.iter().rev() {
        if let Some(bt) = e.backtrace() {
            if bt.status() == BacktraceStatus::Captured {
                eprintln!();
                eprintln!("Backtrace:");
                eprintln!("{}", console::style(bt).dim());
                break;
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

pub fn print_batch_error(batch: &BatchError) {
    eprintln!(
        "{}: {}",
        console::style("Error").red().bold(),
        console::style(&batch.summary)
    );
    eprintln!();
    eprintln!("Summary of individual errors:");

    for (err, ctx) in &batch.errors_with_context {
        eprintln!();

        let mut error_chain = Vec::new();
        let mut err_ref: Option<&dyn Error> = Some(err.as_ref());
        while let Some(e) = err_ref {
            error_chain.push(e);
            err_ref = e.source();
        }

        let mut error_messages: Vec<_> = error_chain.iter().map(|e| format!("{}", e)).collect();
        unchain_error_messages(&mut error_messages);

        if error_messages.len() == 1 {
            eprintln!(
                "{} {}: {}",
                console::style(">").green(),
                console::style(ctx).bold(),
                error_messages[0]
            );
        } else {
            eprintln!(
                "{} {}:",
                console::style(">").green(),
                console::style(ctx).bold()
            );

            for (i, e) in error_messages.iter().enumerate() {
                eprintln!("  {} {}", console::style(format!("{}:", i)).dim(), e);
            }
        }

        // Print the inner most backtrace
        for e in error_chain.iter().rev() {
            if let Some(bt) = e.backtrace() {
                if bt.status() == BacktraceStatus::Captured {
                    eprintln!();
                    eprintln!("Backtrace:");
                    eprintln!("{}", console::style(bt).dim());
                    break;
                }
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

// Ugly workaround that deals with the fact that many libraries have errors that
// format their messages as `{outer error message}: {inner error message}` that
// results in a lot of duplication when printing the entire error chain.
fn unchain_error_messages(errors: &mut Vec<String>) {
    use itertools::Itertools;
    for (first, second) in (0..errors.len()).tuple_windows() {
        if errors[first] == errors[second] {
            errors[first].truncate(0);
        } else {
            let suffix = format!(": {}", &errors[second]);
            if errors[first].ends_with(&suffix) {
                let new_len = errors[first].len() - suffix.len();
                errors[first].truncate(new_len);
            }
        }
    }
    errors.retain(|s| !s.is_empty());
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct BatchError {
    pub summary: String,
    pub errors_with_context: Vec<(BoxedError, String)>,
}

impl BatchError {
    pub fn new<S, I, C, E>(summary: S, errors_with_context: I) -> Self
    where
        S: Into<String>,
        I: Iterator<Item = (E, C)>,
        C: Into<String>,
        E: Into<BoxedError>,
    {
        Self {
            summary: summary.into(),
            errors_with_context: errors_with_context
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

    fn backtrace(&self) -> Option<&Backtrace> {
        None
    }
}

///////////////////////////////////////////////////////////////////////////////
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
// TODO: Replace with traits that distinguish critical and non-critical errors
/////////////////////////////////////////////////////////////////////////////////////////

impl From<GetDatasetError> for CLIError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            e @ GetDatasetError::NotFound(_) => Self::failure(e),
            e @ GetDatasetError::Internal(_) => Self::critical(e),
        }
    }
}

impl From<GetAliasesError> for CLIError {
    fn from(v: GetAliasesError) -> Self {
        match v {
            e @ GetAliasesError::DatasetNotFound(_) => Self::failure(e),
            e @ GetAliasesError::Internal(_) => Self::critical(e),
        }
    }
}

impl From<DeleteDatasetError> for CLIError {
    fn from(v: DeleteDatasetError) -> Self {
        match v {
            e @ DeleteDatasetError::NotFound(_) => Self::failure(e),
            e @ DeleteDatasetError::DanglingReference(_) => Self::failure(e),
            e @ DeleteDatasetError::Internal(_) => Self::critical(e),
        }
    }
}

impl From<GetSummaryError> for CLIError {
    fn from(v: GetSummaryError) -> Self {
        match v {
            e @ GetSummaryError::EmptyDataset => Self::critical(e),
            e @ GetSummaryError::Access(_) => Self::critical(e),
            e @ GetSummaryError::Internal(_) => Self::critical(e),
        }
    }
}

impl From<GetRefError> for CLIError {
    fn from(v: GetRefError) -> Self {
        match v {
            e @ GetRefError::NotFound(_) => Self::critical(e),
            e @ GetRefError::Access(_) => Self::critical(e),
            e @ GetRefError::Internal(_) => Self::critical(e),
        }
    }
}

impl From<IterBlocksError> for CLIError {
    fn from(v: IterBlocksError) -> Self {
        match v {
            e => Self::critical(e),
        }
    }
}

impl From<InternalError> for CLIError {
    fn from(e: InternalError) -> Self {
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
