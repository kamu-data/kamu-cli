// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use background_agent_host::BackgroundWorkFinalizer;
use internal_error::InternalError;
use tracing::Instrument;

use crate::error::CLIError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Completes the background work a command left behind, then runs one batch
/// of each housekeeping job ([`BackgroundWorkFinalizer`]), and returns the
/// command's result combined with the outcome
pub(crate) async fn finalize_background_work_after_command(
    background_catalog: &dill::Catalog,
    command_result: Result<(), CLIError>,
) -> Result<(), CLIError> {
    let finalization_result = BackgroundWorkFinalizer::with_housekeeping()
        .finalize(background_catalog)
        .instrument(tracing::debug_span!(
            "Finalize the background work of the command"
        ))
        .await;

    command_with_finalization_result(command_result, finalization_result)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The result of a command once its background work was finalized: the
/// command's own error wins, and a finalization failure fails a command that
/// succeeded
fn command_with_finalization_result(
    command_result: Result<(), CLIError>,
    finalization_result: Result<(), InternalError>,
) -> Result<(), CLIError> {
    match (command_result, finalization_result) {
        (command_result, Ok(())) => command_result,
        (Ok(()), Err(e)) => Err(e.into()),
        (Err(command_error), Err(e)) => {
            tracing::error!(
                error = ?e,
                error_msg = %e,
                "Background work failed after a failed command"
            );
            Err(command_error)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use internal_error::InternalError;

    use super::command_with_finalization_result;
    use crate::CLIError;

    #[test]
    fn test_finalization_failure_fails_a_successful_command() {
        let result = command_with_finalization_result(Ok(()), Err(InternalError::new("outbox")));

        assert_matches!(
            result,
            Err(CLIError::CriticalFailure { source, .. }) if format!("{source:?}").contains("outbox")
        );
    }

    #[test]
    fn test_command_error_wins_over_finalization_failure() {
        let result = command_with_finalization_result(
            Err(CLIError::usage_error("command")),
            Err(InternalError::new("outbox")),
        );

        assert_matches!(result, Err(CLIError::UsageError(_)));
    }

    #[test]
    fn test_finalization_success_keeps_the_command_result() {
        assert_matches!(command_with_finalization_result(Ok(()), Ok(())), Ok(()));
        assert_matches!(
            command_with_finalization_result(Err(CLIError::usage_error("command")), Ok(())),
            Err(CLIError::UsageError(_))
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
