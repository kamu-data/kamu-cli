// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu_flow_system::*;
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowConfigurationServiceImpl {
    event_store: Arc<dyn FlowConfigurationEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowConfigurationService)]
#[interface(dyn FlowScopeRemovalHandler)]
impl FlowConfigurationServiceImpl {
    pub fn new(
        event_store: Arc<dyn FlowConfigurationEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            event_store,
            time_source,
            outbox,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowConfigurationService for FlowConfigurationServiceImpl {
    #[tracing::instrument(level = "info", skip_all, fields(?flow_binding))]
    async fn find_configuration(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowConfigurationState>, FindFlowConfigurationError> {
        let maybe_flow_configuration =
            FlowConfiguration::try_load(flow_binding, self.event_store.as_ref()).await?;
        Ok(maybe_flow_configuration
            .filter(|flow_configuration| flow_configuration.is_active())
            .map(Into::into))
    }

    #[tracing::instrument(level = "info", skip_all, fields(?flow_binding))]
    async fn set_configuration(
        &self,
        flow_binding: FlowBinding,
        rule: FlowConfigurationRule,
        retry_policy: Option<RetryPolicy>,
    ) -> Result<FlowConfigurationState, SetFlowConfigurationError> {
        tracing::info!(
            flow_binding = ?flow_binding,
            rule = ?rule,
            retry_policy = ?retry_policy,
            "Setting flow configuration"
        );

        let maybe_flow_configuration =
            FlowConfiguration::try_load(&flow_binding, self.event_store.as_ref()).await?;

        // Pending flows only need to hear about an actual change
        let is_changed = maybe_flow_configuration
            .as_ref()
            .is_none_or(|flow_configuration| {
                !flow_configuration.is_active()
                    || flow_configuration.rule != rule
                    || flow_configuration.retry_policy != retry_policy
            });

        let now = self.time_source.now();
        let mut flow_configuration = match maybe_flow_configuration {
            // Modification
            Some(mut flow_configuration) => {
                flow_configuration
                    .modify_configuration(now, rule, retry_policy)
                    .int_err()?;

                flow_configuration
            }
            // New configuration
            None => FlowConfiguration::new(now, flow_binding, rule, retry_policy),
        };

        flow_configuration
            .save(self.event_store.as_ref())
            .await
            .int_err()?;

        if is_changed {
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
                    FlowConfigurationUpdatedMessage {
                        event_time: now,
                        flow_binding: flow_configuration.flow_binding.clone(),
                        rule: flow_configuration.rule.clone(),
                        retry_policy: flow_configuration.retry_policy,
                    },
                )
                .await?;
        }

        Ok(flow_configuration.into())
    }

    fn list_active_configurations(&self) -> FlowConfigurationStateStream<'_> {
        // Note: terribly inefficient - walks over events multiple times
        Box::pin(async_stream::try_stream! {
            use futures::stream::{self, StreamExt, TryStreamExt};
            let flow_bindings: Vec<_> = self.event_store.stream_all_existing_flow_bindings().try_collect().await.int_err()?;

            let flow_configurations = FlowConfiguration::load_multi_simple(&flow_bindings, self.event_store.as_ref()).await.int_err()?;
            let stream = stream::iter(flow_configurations)
                .filter_map(|flow_configuration| async {
                if flow_configuration.is_active() {
                    Some(Ok::<_, InternalError>(flow_configuration.into()))
                } else {
                    None
                }
            });

            for await item in stream {
                yield item?;
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowScopeRemovalHandler for FlowConfigurationServiceImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowConfigurationServiceImpl::handle_flow_scope_removal"
    )]
    async fn handle_flow_scope_removal(&self, flow_scope: &FlowScope) -> Result<(), InternalError> {
        let flow_bindings = self.event_store.all_bindings_for_scope(flow_scope).await?;

        let now = self.time_source.now();

        let mut flow_configurations = Vec::with_capacity(flow_bindings.len());
        for load_result in
            FlowConfiguration::try_load_multi(&flow_bindings, self.event_store.as_ref()).await
        {
            match load_result {
                Ok(mut flow_configuration) => {
                    flow_configuration.notify_scope_removed(now).int_err()?;
                    flow_configurations.push(flow_configuration);
                }
                Err(LoadError::NotFound(_)) => {}
                Err(e @ (LoadError::ProjectionError(_) | LoadError::Internal(_))) => {
                    return Err(e.int_err());
                }
            }
        }

        FlowConfiguration::save_multi(&mut flow_configurations, self.event_store.as_ref())
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
