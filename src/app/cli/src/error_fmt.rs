// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::{Backtrace, BacktraceStatus};
use std::error::Error;

use super::error::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct PrettyCLIError<'a> {
    pub error: &'a CLIError,
    pub include_backtraces: bool,
}

impl PrettyCLIError<'_> {
    fn write_error(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut error_chain = Vec::new();
        let mut err_ref: Option<&dyn Error> = Some(self.error as &dyn Error);
        while let Some(e) = err_ref {
            error_chain.push(e);
            err_ref = e.source();
        }

        let mut error_messages: Vec<_> = error_chain.iter().map(|e| format!("{e}")).collect();
        Self::unchain_error_messages(&mut error_messages);

        if error_messages.len() == 1 {
            writeln!(
                f,
                "{}: {}",
                console::style("Error").red().bold(),
                error_messages[0]
            )?;
        } else {
            writeln!(f, "{}:", console::style("Error").red().bold())?;

            for (i, e) in error_messages.iter().enumerate() {
                writeln!(f, "  {} {}", console::style(format!("{i}:")).dim(), e)?;
            }
        }

        if self.include_backtraces {
            // Print the innermost backtrace
            for e in error_chain.iter().rev() {
                if let Some(bt) = core::error::request_ref::<Backtrace>(e)
                    && bt.status() == BacktraceStatus::Captured
                {
                    writeln!(f)?;
                    writeln!(f, "Backtrace:")?;
                    writeln!(f, "{}", console::style(bt).dim())?;
                    break;
                }
            }
        }

        if let CLIError::CriticalFailure { .. } = self.error {
            writeln!(
                f, "{}",
                console::style("Help us by reporting this problem at \
                https://github.com/kamu-data/kamu-cli/issues").bold()
            )?;
        }

        Ok(())
    }

    pub fn write_batch_error(
        &self,
        batch: &BatchError,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        writeln!(
            f,
            "{}: {}",
            console::style("Error").red().bold(),
            console::style(&batch.summary)
        )?;
        writeln!(f)?;
        writeln!(f, "Summary of individual errors:")?;

        for (err, ctx) in &batch.errors_with_context {
            writeln!(f)?;

            let mut error_chain = Vec::new();
            let mut err_ref: Option<&dyn Error> = Some(err.as_ref());
            while let Some(e) = err_ref {
                error_chain.push(e);
                err_ref = e.source();
            }

            let mut error_messages: Vec<_> = error_chain.iter().map(|e| format!("{e}")).collect();
            Self::unchain_error_messages(&mut error_messages);

            if error_messages.len() == 1 {
                writeln!(
                    f,
                    "{} {}: {}",
                    console::style(">").green(),
                    console::style(ctx).bold(),
                    error_messages[0]
                )?;
            } else {
                writeln!(
                    f,
                    "{} {}:",
                    console::style(">").green(),
                    console::style(ctx).bold()
                )?;

                for (i, e) in error_messages.iter().enumerate() {
                    writeln!(f, "  {} {}", console::style(format!("{i}:")).dim(), e)?;
                }
            }

            if self.include_backtraces {
                // Print the innermost backtrace
                for e in error_chain.iter().rev() {
                    if let Some(bt) = core::error::request_ref::<Backtrace>(e)
                        && bt.status() == BacktraceStatus::Captured
                    {
                        writeln!(f)?;
                        writeln!(f, "Backtrace:")?;
                        writeln!(f, "{}", console::style(bt).dim())?;
                        break;
                    }
                }
            }
        }

        Ok(())
    }

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for PrettyCLIError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.error {
            CLIError::BatchError(batch) => self.write_batch_error(batch, f),
            _ => self.write_error(f),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for SubprocessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to run subprocess: \n {}", self.source)?;

        if !self.log_files.is_empty() {
            writeln!(f, ", see log files for details:")?;
            for path in &self.log_files {
                writeln!(f, "- {}", path.display())?;
            }
        }

        Ok(())
    }
}
