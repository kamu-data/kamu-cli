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
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowConfigurationServiceImpl {
    event_store: Arc<dyn FlowConfigurationEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowConfigurationService)]
#[interface(dyn FlowScopeRemovalHandler)]
impl FlowConfigurationServiceImpl {
    pub fn new(
        event_store: Arc<dyn FlowConfigurationEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            event_store,
            time_source,
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
        Ok(maybe_flow_configuration.map(Into::into))
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

        let mut flow_configuration = match maybe_flow_configuration {
            // Modification
            Some(mut flow_configuration) => {
                flow_configuration
                    .modify_configuration(self.time_source.now(), rule, retry_policy)
                    .int_err()?;

                flow_configuration
            }
            // New configuration
            None => {
                FlowConfiguration::new(self.time_source.now(), flow_binding, rule, retry_policy)
            }
        };

        flow_configuration
            .save(self.event_store.as_ref())
            .await
            .int_err()?;

        Ok(flow_configuration.into())
    }

    fn list_active_configurations(&self) -> FlowConfigurationStateStream {
        // Note: terribly inefficient - walks over events multiple times
        Box::pin(async_stream::try_stream! {
            use futures::stream::{self, StreamExt, TryStreamExt};
            let flow_bindings: Vec<_> = self.event_store.stream_all_existing_flow_bindings().try_collect().await.int_err()?;

            let flow_configurations = FlowConfiguration::load_multi_simple(flow_bindings, self.event_store.as_ref()).await.int_err()?;
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

        for flow_binding in flow_bindings {
            let maybe_flow_configuration =
                FlowConfiguration::try_load(&flow_binding, self.event_store.as_ref())
                    .await
                    .int_err()?;

            if let Some(mut flow_configuration) = maybe_flow_configuration {
                flow_configuration
                    .notify_scope_removed(self.time_source.now())
                    .int_err()?;

                flow_configuration
                    .save(self.event_store.as_ref())
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
