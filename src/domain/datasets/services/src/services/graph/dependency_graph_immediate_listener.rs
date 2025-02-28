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
    MESSAGE_CONSUMER_KAMU_DATASET_DEPENDENCY_GRAPH_IMMEDIATE_LISTENER,
    MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
    MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
};
use messaging_outbox::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DependencyGraphImmediateListener {
    dependency_graph_service: Arc<dyn DependencyGraphService>,
}

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
#[scope(Singleton)]
impl DependencyGraphImmediateListener {
    pub fn new(dependency_graph_service: Arc<dyn DependencyGraphService>) -> Self {
        Self {
            dependency_graph_service,
        }
    }

    async fn handle_derived_dependency_updates(
        &self,
        dependency_graph_repo: &dyn DatasetDependencyRepository,
        outbox: &dyn Outbox,
        target: ResolvedDataset,
        message: &DatasetReferenceMessage,
    ) -> Result<(), InternalError> {
        // Compute if there are modified dependencies
        let dependency_change = self
            .extract_modified_dependencies_in_interval(
                (*target).as_ref(),
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

        // Classify changes: obsolete, added
        let obsolete_dependencies: Vec<_> = existing_upstream_ids
            .difference(&new_upstream_ids)
            .collect();
        let added_dependencies: Vec<_> = new_upstream_ids
            .difference(&existing_upstream_ids)
            .collect();

        // Update database dependencies
        dependency_graph_repo
            .remove_upstream_dependencies(&message.dataset_id, &obsolete_dependencies)
            .await
            .int_err()?;

        dependency_graph_repo
            .add_upstream_dependencies(&message.dataset_id, &added_dependencies)
            .await
            .int_err()?;

        // Send outbox message, so that in-memory graph version is synchronized
        // once the main transaction completes successfully
        outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
                DatasetDependenciesMessage {
                    dataset_id: message.dataset_id.clone(),
                    obsolete_upstream_ids: obsolete_dependencies.into_iter().cloned().collect(),
                    added_upstream_ids: added_dependencies.into_iter().cloned().collect(),
                },
            )
            .await?;

        Ok(())
    }

    async fn extract_modified_dependencies_in_interval(
        &self,
        dataset: &dyn odf::Dataset,
        head: &odf::Multihash,
        maybe_tail: Option<&odf::Multihash>,
    ) -> Result<DependencyChange, InternalError> {
        let mut new_upstream_ids: HashSet<odf::DatasetID> = HashSet::new();

        let mut set_transform_visitor = odf::dataset::SearchSetTransformVisitor::new();
        let mut seed_visitor = odf::dataset::SearchSeedVisitor::new();

        use odf::dataset::MetadataChainExt;
        dataset
            .as_metadata_chain()
            .accept_by_interval(
                &mut [&mut set_transform_visitor, &mut seed_visitor],
                Some(head),
                maybe_tail,
            )
            .await
            .int_err()?;

        if let Some(event) = set_transform_visitor.into_event() {
            for new_input in &event.inputs {
                if let Some(id) = new_input.dataset_ref.id() {
                    new_upstream_ids.insert(id.clone());
                }
            }
        }

        // 3 cases:
        //  - we see `SetTransform` event that is relevant now (changed)
        //  - we don't see it and stop where asked (unchanged)
        //  - we don't see it and reach seed (dropped)
        if !new_upstream_ids.is_empty() {
            Ok(DependencyChange::Changed(new_upstream_ids))
        } else if seed_visitor.into_event().is_some() {
            Ok(DependencyChange::Dropped)
        } else {
            Ok(DependencyChange::Unchanged)
        }
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
        transaction_catalog: &Catalog,
        message: &DatasetReferenceMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset reference message");

        // For now, react only on Head updates
        if message.block_ref != odf::dataset::BlockRef::Head {
            return Ok(());
        }

        // Resolve dataset
        let dataset_registry = transaction_catalog
            .get_one::<dyn DatasetRegistry>()
            .unwrap();
        let target = dataset_registry
            .get_dataset_by_id(&message.dataset_id)
            .await
            .int_err()?;

        // Skip non-derived datasets
        let summary = target
            .get_summary(odf::dataset::GetSummaryOpts::default())
            .await
            .int_err()?;
        if summary.kind != odf::DatasetKind::Derivative {
            return Ok(());
        }

        // Deal with potential upstream changes
        self.handle_derived_dependency_updates(
            transaction_catalog
                .get_one::<dyn DatasetDependencyRepository>()
                .unwrap()
                .as_ref(),
            transaction_catalog
                .get_one::<dyn Outbox>()
                .unwrap()
                .as_ref(),
            target,
            message,
        )
        .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum DependencyChange {
    Unchanged,
    Dropped,
    Changed(HashSet<odf::DatasetID>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
