// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::{
    DatasetRegistry,
    DatasetRegistryExt,
    DependencyGraphService,
    GetDependenciesError,
    ResolvedDataset,
};
use kamu_datasets::{
    DatasetDependenciesMessage,
    DatasetDependencyRepository,
    DatasetReferenceMessage,
    DatasetReferenceMessageUpdated,
    MESSAGE_CONSUMER_KAMU_DATASET_DEPENDENCY_GRAPH_IMMEDIATE_LISTENER,
    MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
    MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
};
use messaging_outbox::*;

use crate::{extract_modified_dependencies_in_interval, DependencyChange};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetReferenceMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_DEPENDENCY_GRAPH_IMMEDIATE_LISTENER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Immediate,
})]
pub struct DependencyGraphImmediateListener {
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    dependency_graph_repo: Arc<dyn DatasetDependencyRepository>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    outbox: Arc<dyn Outbox>,
}

impl DependencyGraphImmediateListener {
    async fn handle_derived_dependency_updates(
        &self,
        target: ResolvedDataset,
        message: &DatasetReferenceMessageUpdated,
    ) -> Result<(), InternalError> {
        // Compute if there are modified dependencies
        let dependency_change = extract_modified_dependencies_in_interval(
            target.as_metadata_chain(),
            &message.new_block_hash,
            message.maybe_prev_block_hash.as_ref(),
        )
        .await?;

        // What's the situation now?
        let new_upstream_ids = match dependency_change {
            // No dependency changes => Nothing to do
            DependencyChange::Unchanged => return Ok(()),

            // Explicit changes: having new, will be compared with old
            DependencyChange::Changed(new_upstream_ids) => new_upstream_ids,

            // Dropped changes: no new, but the old would be deleted
            DependencyChange::Dropped => HashSet::new(),
        };

        // Extract current dependencies known by the graph
        let existing_upstream_ids: HashSet<_> = match self
            .dependency_graph_service
            .get_upstream_dependencies(&message.dataset_id)
            .await
        {
            Ok(dependencies) => {
                use futures::StreamExt;
                dependencies.collect().await
            }
            Err(GetDependenciesError::DatasetNotFound(_)) => HashSet::new(),
            Err(GetDependenciesError::Internal(e)) => return Err(e),
        };

        // No real changes => Nothing to do
        if new_upstream_ids == existing_upstream_ids {
            return Ok(());
        }

        // Classify changes: removed, added
        let removed_dependencies: Vec<_> = existing_upstream_ids
            .difference(&new_upstream_ids)
            .collect();
        let added_dependencies: Vec<_> = new_upstream_ids
            .difference(&existing_upstream_ids)
            .collect();

        // Update database dependencies
        self.dependency_graph_repo
            .remove_upstream_dependencies(&message.dataset_id, &removed_dependencies)
            .await
            .int_err()?;

        self.dependency_graph_repo
            .add_upstream_dependencies(&message.dataset_id, &added_dependencies)
            .await
            .int_err()?;

        // Send outbox message, so that in-memory graph version is synchronized
        // once the main transaction completes successfully
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
                DatasetDependenciesMessage::updated(
                    &message.dataset_id,
                    added_dependencies.into_iter().cloned().collect(),
                    removed_dependencies.into_iter().cloned().collect(),
                ),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DependencyGraphImmediateListener {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetReferenceMessage> for DependencyGraphImmediateListener {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DependencyGraphImmediateListener[DatasetReferenceMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetReferenceMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset reference message");

        match message {
            DatasetReferenceMessage::Updated(updated_message) => {
                // For now, react only on Head updates
                if updated_message.block_ref != odf::dataset::BlockRef::Head {
                    return Ok(());
                }

                // Resolve dataset
                let target = self
                    .dataset_registry
                    .get_dataset_by_id(&updated_message.dataset_id)
                    .await
                    .int_err()?;

                // Skip non-derived datasets
                if target.get_kind() != odf::DatasetKind::Derivative {
                    return Ok(());
                }

                // Deal with potential upstream changes
                self.handle_derived_dependency_updates(target, updated_message)
                    .await?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
