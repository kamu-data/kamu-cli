// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use chrono::Utc;
use dill::Component;
use internal_error::InternalError;
use kamu_adapter_flow_dataset::{
    DATASET_RESOURCE_TYPE,
    FlowDatasetsEventBridge,
    FlowScopeDataset,
    ingest_dataset_binding,
};
use kamu_datasets::*;
use kamu_datasets_services::testing::MockDatasetIncrementQueryService;
use kamu_flow_system::*;
use messaging_outbox::*;
use odf::dataset::MetadataChainIncrementInterval;
use serde_json::json;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_dependency_update() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");

    let mock_flow_sensor_dispatcher = MockFlowSensorDispatcher::with_refresh_sensor_dependencies(
        FlowScopeDataset::make_scope(&foo_id),
    );

    let harness = FlowDatasetsEventBridgeHarness::new(
        &foo_id,
        FlowDatasetEventBridgeHarnessOverrides {
            mock_flow_sensor_dispatcher: Some(mock_flow_sensor_dispatcher),
            ..Default::default()
        },
    );
    harness
        .define_dataset_dependencies(&foo_id, std::slice::from_ref(&bar_id))
        .await;

    harness.scope_remover.expecting_removals_count(0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_deleted() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = FlowDatasetsEventBridgeHarness::new(&foo_id, Default::default());
    harness.mimic_dataset_deleted(&foo_id).await;

    harness.scope_remover.expecting_removals_count(1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_http_ingest() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let old_hash = odf::Multihash::from_digest_sha3_256(b"old_head");
    let new_hash = odf::Multihash::from_digest_sha3_256(b"new_head");

    let mock_dataset_increment_query_service =
        MockDatasetIncrementQueryService::with_increment_between_for_args(
            foo_id.clone(),
            Some(old_hash.clone()),
            new_hash.clone(),
            MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            },
        );

    let mock_sensor_dispatcher = MockFlowSensorDispatcher::with_dispatch_for_resource_update_cause(
        ingest_dataset_binding(&foo_id),
        FlowActivationCauseResourceUpdate {
            activation_time: Utc::now(),
            changes: ResourceChanges::NewData(ResourceDataChanges {
                blocks_added: 1,
                records_added: 5,
                new_watermark: None,
            }),
            resource_type: DATASET_RESOURCE_TYPE.to_string(),
            details: json!({
                "dataset_id": foo_id.to_string(),
                "new_head": new_hash.to_string(),
                "old_head_maybe": old_hash.to_string(),
                "source": {
                    "HttpIngest": {
                        "source_name": serde_json::Value::Null,
                    }
                }
            }),
        },
    );

    let harness = FlowDatasetsEventBridgeHarness::new(
        &foo_id,
        FlowDatasetEventBridgeHarnessOverrides {
            mock_dataset_increment_query_service: Some(mock_dataset_increment_query_service),
            mock_flow_sensor_dispatcher: Some(mock_sensor_dispatcher),
        },
    );
    harness
        .mimic_http_ingest(&foo_id, Some(&old_hash), &new_hash)
        .await;

    harness.scope_remover.expecting_removals_count(0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_protocol_push() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let old_hash = odf::Multihash::from_digest_sha3_256(b"old_head");
    let new_hash = odf::Multihash::from_digest_sha3_256(b"new_head");

    let mock_dataset_increment_query_service =
        MockDatasetIncrementQueryService::with_increment_between_for_args(
            foo_id.clone(),
            Some(old_hash.clone()),
            new_hash.clone(),
            MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            },
        );

    let mock_sensor_dispatcher = MockFlowSensorDispatcher::with_dispatch_for_resource_update_cause(
        ingest_dataset_binding(&foo_id),
        FlowActivationCauseResourceUpdate {
            activation_time: Utc::now(),
            changes: ResourceChanges::NewData(ResourceDataChanges {
                blocks_added: 1,
                records_added: 5,
                new_watermark: None,
            }),
            resource_type: DATASET_RESOURCE_TYPE.to_string(),
            details: json!({
                "dataset_id": foo_id.to_string(),
                "new_head": new_hash.to_string(),
                "old_head_maybe": old_hash.to_string(),
                "source": {
                    "SmartProtocolPush": {
                        "account_name": serde_json::Value::Null,
                        "is_force": false,
                    }
                }
            }),
        },
    );

    let harness = FlowDatasetsEventBridgeHarness::new(
        &foo_id,
        FlowDatasetEventBridgeHarnessOverrides {
            mock_dataset_increment_query_service: Some(mock_dataset_increment_query_service),
            mock_flow_sensor_dispatcher: Some(mock_sensor_dispatcher),
        },
    );
    harness
        .mimic_smart_protocol_push(&foo_id, Some(&old_hash), &new_hash, false)
        .await;

    harness.scope_remover.expecting_removals_count(0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_protocol_push_with_force() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let old_hash = odf::Multihash::from_digest_sha3_256(b"old_head");
    let new_hash = odf::Multihash::from_digest_sha3_256(b"new_head");

    let mock_dataset_increment_query_service =
        MockDatasetIncrementQueryService::with_increment_error_invalid_interval(
            foo_id.clone(),
            old_hash.clone(),
            new_hash.clone(),
        );

    let mock_sensor_dispatcher = MockFlowSensorDispatcher::with_dispatch_for_resource_update_cause(
        ingest_dataset_binding(&foo_id),
        FlowActivationCauseResourceUpdate {
            activation_time: Utc::now(),
            changes: ResourceChanges::Breaking,
            resource_type: DATASET_RESOURCE_TYPE.to_string(),
            details: json!({
                "dataset_id": foo_id.to_string(),
                "new_head": new_hash.to_string(),
                "old_head_maybe": old_hash.to_string(),
                "source": {
                    "SmartProtocolPush": {
                        "account_name": serde_json::Value::Null,
                        "is_force": true,
                    }
                }
            }),
        },
    );

    let harness = FlowDatasetsEventBridgeHarness::new(
        &foo_id,
        FlowDatasetEventBridgeHarnessOverrides {
            mock_dataset_increment_query_service: Some(mock_dataset_increment_query_service),
            mock_flow_sensor_dispatcher: Some(mock_sensor_dispatcher),
        },
    );
    harness
        .mimic_smart_protocol_push(&foo_id, Some(&old_hash), &new_hash, true)
        .await;

    harness.scope_remover.expecting_removals_count(0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowDatasetsEventBridgeHarness {
    outbox: Arc<dyn Outbox>,
    scope_remover: Arc<FlowScopeTestRemover>,
}

#[derive(Default)]
struct FlowDatasetEventBridgeHarnessOverrides {
    mock_flow_sensor_dispatcher: Option<MockFlowSensorDispatcher>,
    mock_dataset_increment_query_service: Option<MockDatasetIncrementQueryService>,
}

impl FlowDatasetsEventBridgeHarness {
    fn new(dataset_id: &odf::DatasetID, overrides: FlowDatasetEventBridgeHarnessOverrides) -> Self {
        let mock_flow_sensor_dispatcher = overrides.mock_flow_sensor_dispatcher.unwrap_or_default();
        let mock_dataset_increment_query_service = overrides
            .mock_dataset_increment_query_service
            .unwrap_or_default();

        let mut b = dill::CatalogBuilder::new();
        b.add::<FlowDatasetsEventBridge>();
        b.add::<SystemTimeSourceDefault>();
        b.add_value(mock_flow_sensor_dispatcher);
        b.bind::<dyn FlowSensorDispatcher, MockFlowSensorDispatcher>();
        b.add_value(mock_dataset_increment_query_service);
        b.bind::<dyn DatasetIncrementQueryService, MockDatasetIncrementQueryService>();
        b.add_builder(FlowScopeTestRemover::builder().with_removed_dataset_id(dataset_id.clone()));
        b.add_builder(
            OutboxImmediateImpl::builder()
                .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
        );
        b.bind::<dyn Outbox, OutboxImmediateImpl>();

        register_message_dispatcher::<DatasetDependenciesMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
        );

        register_message_dispatcher::<DatasetLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        );

        register_message_dispatcher::<DatasetExternallyChangedMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
        );

        let catalog = b.build();
        Self {
            outbox: catalog.get_one::<dyn Outbox>().unwrap(),
            scope_remover: catalog.get_one::<FlowScopeTestRemover>().unwrap(),
        }
    }

    async fn define_dataset_dependencies(
        &self,
        upstream_dataset_id: &odf::DatasetID,
        downstream_dataset_ids: &[odf::DatasetID],
    ) {
        let message = DatasetDependenciesMessage::updated(
            upstream_dataset_id,
            downstream_dataset_ids.to_vec(),
            vec![],
        );
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
                message,
            )
            .await
            .unwrap();
    }

    async fn mimic_dataset_deleted(&self, dataset_id: &odf::DatasetID) {
        let message = DatasetLifecycleMessage::deleted(Utc::now(), dataset_id.clone());
        self.outbox
            .post_message(MESSAGE_PRODUCER_KAMU_DATASET_SERVICE, message)
            .await
            .unwrap();
    }

    async fn mimic_http_ingest(
        &self,
        dataset_id: &odf::DatasetID,
        old_hash: Option<&odf::Multihash>,
        new_hash: &odf::Multihash,
    ) {
        let message =
            DatasetExternallyChangedMessage::ingest_http(dataset_id, None, old_hash, new_hash);

        self.outbox
            .post_message(MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER, message)
            .await
            .unwrap();
    }

    async fn mimic_smart_protocol_push(
        &self,
        dataset_id: &odf::DatasetID,
        old_hash: Option<&odf::Multihash>,
        new_hash: &odf::Multihash,
        is_force: bool,
    ) {
        let message = DatasetExternallyChangedMessage::smart_transfer_protocol_sync(
            dataset_id, old_hash, new_hash, None, is_force,
        );

        self.outbox
            .post_message(MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER, message)
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowScopeTestRemover {
    removed_dataset_id: odf::DatasetID,
    invoked_removals: AtomicUsize,
}

#[dill::component(pub)]
#[dill::interface(dyn FlowScopeRemovalHandler)]
#[dill::scope(dill::Singleton)]
impl FlowScopeTestRemover {
    fn new(removed_dataset_id: odf::DatasetID) -> Self {
        Self {
            removed_dataset_id,
            invoked_removals: AtomicUsize::new(0),
        }
    }

    fn expecting_removals_count(&self, expected_count: usize) {
        let actual_removals = self
            .invoked_removals
            .load(std::sync::atomic::Ordering::SeqCst);

        assert_eq!(actual_removals, expected_count,);
    }
}

#[async_trait::async_trait]
impl FlowScopeRemovalHandler for FlowScopeTestRemover {
    async fn handle_flow_scope_removal(&self, flow_scope: &FlowScope) -> Result<(), InternalError> {
        assert_eq!(
            FlowScopeDataset::new(flow_scope).dataset_id(),
            self.removed_dataset_id,
            "Flow scope removal handler received unexpected flow scope"
        );
        self.invoked_removals
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
