// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_adapter_flow_dataset::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    DatasetUpdateSource,
    FlowScopeDataset,
};
use kamu_datasets::{
    DatasetIncrementQueryService,
    DatasetReferenceService,
    GetDatasetReferenceError,
};
use kamu_flow_system::{self as fs, FlowSensorSensitizationError};
use kamu_webhooks::WebhookSubscriptionID;

use crate::{
    DatasetRefUpdatedWatermarkProvider,
    FLOW_TYPE_WEBHOOK_DELIVER,
    FlowScopeSubscription,
    dataset_changes_between,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetUpdatedWebhookSensor {
    webhook_flow_scope: fs::FlowScope,
    subscription_id: WebhookSubscriptionID,
    dataset_id: odf::DatasetID,
    state: Arc<Mutex<State>>,
}

struct State {
    reactive_rule: fs::ReactiveRule,
    /// Head delivered by the last catch-up, until an update made after it
    /// arrives: updates it already covers may still be dispatched afterwards
    caught_up_head: Option<odf::Multihash>,
}

impl State {
    fn new(reactive_rule: fs::ReactiveRule) -> Self {
        Self {
            reactive_rule,
            caught_up_head: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetUpdatedWebhookSensor {
    pub fn new(webhook_flow_scope: fs::FlowScope, reactive_rule: fs::ReactiveRule) -> Self {
        let subscription_scope = FlowScopeSubscription::new(&webhook_flow_scope);
        Self {
            subscription_id: subscription_scope.subscription_id(),
            dataset_id: subscription_scope.maybe_dataset_id().unwrap(),
            webhook_flow_scope,
            state: Arc::new(Mutex::new(State::new(reactive_rule))),
        }
    }

    #[inline]
    pub fn reactive_rule(&self) -> fs::ReactiveRule {
        self.state.lock().unwrap().reactive_rule
    }

    fn caught_up_head(&self) -> Option<odf::Multihash> {
        self.state.lock().unwrap().caught_up_head.clone()
    }

    fn set_caught_up_head(&self, head: Option<odf::Multihash>) {
        self.state.lock().unwrap().caught_up_head = head;
    }

    async fn changes_between(
        &self,
        catalog: &dill::Catalog,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<fs::ResourceChanges, InternalError> {
        let dataset_increment_query_service = catalog
            .get_one::<dyn DatasetIncrementQueryService>()
            .unwrap();
        dataset_changes_between(
            dataset_increment_query_service.as_ref(),
            &self.dataset_id,
            old_head,
            new_head,
        )
        .await
    }

    /// Whether the update was already delivered by the last catch-up. Forgets
    /// the caught-up head once an update made after it arrives
    async fn is_covered_by_catch_up(
        &self,
        catalog: &dill::Catalog,
        details: &DatasetResourceUpdateDetails,
    ) -> Result<bool, InternalError> {
        let Some(caught_up_head) = self.caught_up_head() else {
            return Ok(false);
        };

        // References move by compare-and-swap, so the first change after the
        // catch-up starts from the caught-up head, a reset included. Any other
        // update was made before it, and is covered if it ends in the history
        // the catch-up delivered
        let covered = if details.old_head_maybe.as_ref() == Some(&caught_up_head) {
            false
        } else if details.new_head == caught_up_head {
            true
        } else {
            match self
                .changes_between(catalog, &details.new_head, &caught_up_head)
                .await?
            {
                fs::ResourceChanges::NewData(_) => true,
                fs::ResourceChanges::Breaking => false,
            }
        };

        if !covered {
            self.set_caught_up_head(None);
        }
        Ok(covered)
    }

    /// Runs a delivery flow for a dataset update, unless the rule skips
    /// breaking changes
    async fn run_delivery_flow(
        &self,
        catalog: &dill::Catalog,
        update: &fs::FlowActivationCauseResourceUpdate,
    ) -> Result<(), InternalError> {
        let reactive_rule = self.reactive_rule();

        match (update.changes, reactive_rule.for_breaking_change) {
            (fs::ResourceChanges::Breaking, fs::BreakingChangeRule::NoAction) => {
                tracing::warn!(
                    dataset_id = %self.dataset_id,
                    webhook_flow_scope = ?self.webhook_flow_scope,
                    "Skipping breaking change",
                );
                return Ok(());
            }
            (fs::ResourceChanges::Breaking, fs::BreakingChangeRule::Recover)
            | (
                fs::ResourceChanges::NewData(_),
                fs::BreakingChangeRule::NoAction | fs::BreakingChangeRule::Recover,
            ) => { /* Allow webhook flow to run */ }
        }

        let flow_run_service = catalog.get_one::<dyn fs::FlowRunService>().unwrap();
        let target_flow_binding =
            fs::FlowBinding::new(FLOW_TYPE_WEBHOOK_DELIVER, self.webhook_flow_scope.clone());
        flow_run_service
            .run_flow_automatically(
                update.activation_time,
                &target_flow_binding,
                vec![fs::FlowActivationCause::ResourceUpdate(update.clone())],
                Some(fs::FlowTriggerRule::Reactive(reactive_rule)),
                None,
            )
            .await
            .int_err()?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl fs::FlowSensor for DatasetUpdatedWebhookSensor {
    fn flow_scope(&self) -> &fs::FlowScope {
        &self.webhook_flow_scope
    }

    async fn get_sensitive_to_scopes(&self, _: &dill::Catalog) -> Vec<fs::FlowScope> {
        vec![FlowScopeDataset::make_scope(&self.dataset_id)]
    }

    fn update_rule(&self, new_rule: fs::ReactiveRule) {
        let mut state = self.state.lock().unwrap();
        state.reactive_rule = new_rule;
    }

    /// Delivers the changes the receiver has not seen: updates made while the
    /// trigger was inactive or before the sensor was registered
    async fn on_activated(
        &self,
        catalog: &dill::Catalog,
        activation_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        tracing::info!(?self.webhook_flow_scope, "DatasetUpdatedWebhookSensor activated");

        let watermark_provider = catalog
            .get_one::<DatasetRefUpdatedWatermarkProvider>()
            .unwrap();
        let Some(delivered_head) = watermark_provider
            .find_delivered_head(self.subscription_id)
            .await?
        else {
            tracing::info!("No delivered head known, nothing to catch up");
            return Ok(());
        };

        let dataset_reference_service = catalog.get_one::<dyn DatasetReferenceService>().unwrap();
        let head = match dataset_reference_service
            .get_reference(&self.dataset_id, &odf::BlockRef::Head)
            .await
        {
            Ok(head) => head,
            // A deleted dataset's triggers stay until its subscriptions' removal is handled
            Err(GetDatasetReferenceError::NotFound(_)) => {
                tracing::warn!(dataset_id = %self.dataset_id, "Dataset has no head, nothing to catch up");
                return Ok(());
            }
            Err(GetDatasetReferenceError::Internal(e)) => return Err(e),
        };
        if head == delivered_head {
            tracing::info!("Receiver is up to date, nothing to catch up");
            return Ok(());
        }

        tracing::info!(%delivered_head, %head, "Catching up on undelivered changes");
        let update = fs::FlowActivationCauseResourceUpdate {
            activation_time,
            changes: self
                .changes_between(catalog, &delivered_head, &head)
                .await?,
            resource_type: DATASET_RESOURCE_TYPE.to_string(),
            details: serde_json::to_value(DatasetResourceUpdateDetails {
                dataset_id: self.dataset_id.clone(),
                source: DatasetUpdateSource::ExternallyDetectedChange,
                new_head: head.clone(),
                old_head_maybe: Some(delivered_head),
            })
            .int_err()?,
        };

        self.set_caught_up_head(Some(head));
        self.run_delivery_flow(catalog, &update).await
    }

    async fn on_sensitized(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &fs::FlowBinding,
        activation_cause: &fs::FlowActivationCause,
    ) -> Result<(), FlowSensorSensitizationError> {
        tracing::info!(?self.webhook_flow_scope, ?input_flow_binding, ?activation_cause, "DatasetUpdatedWebhookSensor sensitized");

        // Ensure sensitized for right dataset id
        let input_dataset_id = FlowScopeDataset::new(&input_flow_binding.scope).dataset_id();
        if input_dataset_id != self.dataset_id {
            return Err(FlowSensorSensitizationError::InvalidInputFlowBinding {
                binding: input_flow_binding.clone(),
            });
        }

        // React to dataset updates
        let fs::FlowActivationCause::ResourceUpdate(update) = activation_cause else {
            return Err(InternalError::new(format!(
                "Invalid activation cause for DatasetUpdatedWebhookSensor: {activation_cause:?}",
            ))
            .into());
        };
        if update.resource_type != DATASET_RESOURCE_TYPE {
            return Err(InternalError::new(format!(
                "Unexpected resource type: {}",
                update.resource_type
            ))
            .into());
        }

        let details =
            serde_json::from_value::<DatasetResourceUpdateDetails>(update.details.clone())
                .int_err()?;
        if self.is_covered_by_catch_up(catalog, &details).await? {
            tracing::info!(new_head = %details.new_head, "Update already delivered by catch-up");
            return Ok(());
        }

        self.run_delivery_flow(catalog, update).await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
