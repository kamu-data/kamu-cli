// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_resources_services::BackgroundWorkFinalizer;
use messaging_outbox::register_message_dispatcher;
use tracing::Instrument;

use crate::error::CLIError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Catalog
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn register_outbox_components(b: &mut dill::CatalogBuilder) {
    b.add_builder(messaging_outbox::OutboxImmediateImpl::builder(
        messaging_outbox::ConsumerFilter::ImmediateConsumers,
    ));
    b.add::<messaging_outbox::OutboxTransactionalImpl>();
    b.add::<messaging_outbox::OutboxDispatchingImpl>();
    b.bind::<dyn messaging_outbox::Outbox, messaging_outbox::OutboxDispatchingImpl>();
    b.add::<messaging_outbox::OutboxAgentImpl>();
    b.add::<messaging_outbox::OutboxAgentMetrics>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Dispatchers of the messages produced by the components of the base catalog
pub(crate) fn register_base_message_dispatchers(b: &mut dill::CatalogBuilder) {
    register_message_dispatcher::<kamu_accounts::AccountLifecycleMessage>(
        b,
        kamu_accounts::MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    );

    register_message_dispatcher::<kamu_datasets::DatasetLifecycleMessage>(
        b,
        kamu_datasets::MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    );

    register_message_dispatcher::<kamu_datasets::DatasetReferenceMessage>(
        b,
        kamu_datasets::MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    );

    register_message_dispatcher::<kamu_datasets::DatasetDependenciesMessage>(
        b,
        kamu_datasets::MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
    );

    register_message_dispatcher::<kamu_datasets::DatasetKeyBlocksMessage>(
        b,
        kamu_datasets::MESSAGE_PRODUCER_KAMU_DATASET_KEY_BLOCK_UPDATE_HANDLER,
    );

    register_message_dispatcher::<kamu_datasets::DatasetExternallyChangedMessage>(
        b,
        kamu_datasets::MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
    );

    register_message_dispatcher::<kamu_auth_rebac::RebacDatasetPropertiesMessage>(
        b,
        kamu_auth_rebac::MESSAGE_PRODUCER_KAMU_REBAC_DATASET_PROPERTIES_SERVICE,
    );

    register_message_dispatcher::<kamu_auth_rebac::RebacDatasetRelationsMessage>(
        b,
        kamu_auth_rebac::MESSAGE_PRODUCER_KAMU_REBAC_DATASET_RELATIONS_SERVICE,
    );

    register_message_dispatcher::<kamu_resources::ResourceLifecycleMessage>(
        b,
        kamu_resources::MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Dispatchers of the messages produced by the components of the server catalog
pub(crate) fn register_server_message_dispatchers(b: &mut dill::CatalogBuilder) {
    register_message_dispatcher::<kamu_flow_system::FlowConfigurationUpdatedMessage>(
        b,
        kamu_flow_system::MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
    );
    register_message_dispatcher::<kamu_flow_system::FlowTriggerUpdatedMessage>(
        b,
        kamu_flow_system::MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
    );
    register_message_dispatcher::<kamu_flow_system::FlowProcessLifecycleMessage>(
        b,
        kamu_flow_system::MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR,
    );
    register_message_dispatcher::<kamu_task_system::TaskProgressMessage>(
        b,
        kamu_task_system::MESSAGE_PRODUCER_KAMU_TASK_AGENT,
    );

    register_message_dispatcher::<kamu_accounts::AccessTokenLifecycleMessage>(
        b,
        kamu_accounts::MESSAGE_PRODUCER_KAMU_ACCESS_TOKEN_SERVICE,
    );

    register_message_dispatcher::<kamu_webhooks::WebhookSubscriptionLifecycleMessage>(
        b,
        kamu_webhooks::MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_SERVICE,
    );
    register_message_dispatcher::<kamu_webhooks::WebhookSubscriptionEventChangesMessage>(
        b,
        kamu_webhooks::MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_EVENT_CHANGES_SERVICE,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// After a command
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Consumes the Outbox messages accumulated by a command, and returns the
/// command's result combined with the outcome
pub(crate) async fn process_outbox_after_command(
    background_catalog: &dill::Catalog,
    command_result: Result<(), CLIError>,
) -> Result<(), CLIError> {
    let outbox_result = BackgroundWorkFinalizer
        .finalize(background_catalog)
        .instrument(tracing::debug_span!(
            "Consume accumulated the Outbox messages"
        ))
        .await;

    command_with_outbox_result(command_result, outbox_result)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The result of a command once the Outbox was processed after it: the
/// command's own error wins, and an Outbox failure fails a command that
/// succeeded
fn command_with_outbox_result(
    command_result: Result<(), CLIError>,
    outbox_result: Result<(), InternalError>,
) -> Result<(), CLIError> {
    match (command_result, outbox_result) {
        (command_result, Ok(())) => command_result,
        (Ok(()), Err(e)) => Err(e.into()),
        (Err(command_error), Err(e)) => {
            tracing::error!(error = ?e, error_msg = %e, "Outbox processing failed after a failed command");
            Err(command_error)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use internal_error::InternalError;

    use super::command_with_outbox_result;
    use crate::CLIError;

    #[test]
    fn test_outbox_failure_fails_a_successful_command() {
        let result = command_with_outbox_result(Ok(()), Err(InternalError::new("outbox")));

        assert_matches!(
            result,
            Err(CLIError::CriticalFailure { source, .. }) if format!("{source:?}").contains("outbox")
        );
    }

    #[test]
    fn test_command_error_wins_over_outbox_failure() {
        let result = command_with_outbox_result(
            Err(CLIError::usage_error("command")),
            Err(InternalError::new("outbox")),
        );

        assert_matches!(result, Err(CLIError::UsageError(_)));
    }

    #[test]
    fn test_outbox_success_keeps_the_command_result() {
        assert_matches!(command_with_outbox_result(Ok(()), Ok(())), Ok(()));
        assert_matches!(
            command_with_outbox_result(Err(CLIError::usage_error("command")), Ok(())),
            Err(CLIError::UsageError(_))
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
