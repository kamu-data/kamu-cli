// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use kamu::MetadataQueryServiceImpl;
use kamu_datasets::*;
use kamu_flow_system_inmem::*;
use odf::metadata::testing::MetadataFactory;

use crate::utils::{
    BaseGQLDatasetHarness,
    MutationRequest,
    PredefinedAccountOpts,
    authentication_catalogs,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
pub struct BaseGQLFlowHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    pub catalog_anonymous: dill::Catalog,
    pub catalog_authorized: dill::Catalog,
}

impl BaseGQLFlowHarness {
    pub async fn new(base_gql_harness: BaseGQLDatasetHarness, catalog: dill::Catalog) -> Self {
        let (catalog_anonymous, catalog_authorized) =
            authentication_catalogs(&catalog, PredefinedAccountOpts::default()).await;

        Self {
            base_gql_harness,
            catalog_anonymous,
            catalog_authorized,
        }
    }

    pub fn make_base_gql_flow_catalog(base_gql_harness: &BaseGQLDatasetHarness) -> dill::Catalog {
        let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

        b.add::<MetadataQueryServiceImpl>()
            .add::<InMemoryFlowEventStore>()
            .add::<InMemoryFlowTriggerEventStore>()
            .add::<InMemoryFlowConfigurationEventStore>()
            .add::<InMemoryFlowSystemEventBridge>()
            .add::<InMemoryFlowProcessState>();

        b.build()
    }

    pub async fn create_root_dataset(
        &self,
        dataset_alias: odf::DatasetAlias,
    ) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(odf::DatasetKind::Root)
                    .name(dataset_alias)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub async fn create_root_dataset_no_source(
        &self,
        dataset_alias: odf::DatasetAlias,
    ) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(odf::DatasetKind::Root)
                    .name(dataset_alias)
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub async fn create_derived_dataset(
        &self,
        dataset_alias: odf::DatasetAlias,
        inputs: &[odf::DatasetAlias],
    ) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(dataset_alias)
                    .kind(odf::DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(inputs)
                            .build(),
                    )
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub async fn create_derived_dataset_no_transform(
        &self,
        dataset_alias: odf::DatasetAlias,
    ) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(dataset_alias)
                    .kind(odf::DatasetKind::Derivative)
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub fn set_time_delta_trigger(
        &self,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
        time_delta: (u64, &str),
    ) -> MutationRequest {
        let mutation_code = r#"
            mutation($datasetId: ID!, $flowType: String!, $timeDelta: TimeDeltaInput!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            triggers {
                                setTrigger (
                                    datasetFlowType: $flowType,
                                    triggerRuleInput: {
                                        schedule: {
                                            timeDelta: $timeDelta
                                        }
                                    }
                                    triggerStopPolicyInput: {
                                        afterConsecutiveFailures: {
                                            maxFailures: 3
                                        }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowTriggerSuccess {
                                        trigger {
                                            __typename
                                            paused
                                            schedule {
                                                __typename
                                                ... on TimeDelta {
                                                    every
                                                    unit
                                                }
                                            }
                                            reactive {
                                                __typename
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "#;

        MutationRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "flowType": flow_type,
                "timeDelta": {
                    "every": time_delta.0,
                    "unit": time_delta.1,
                },
            })),
        )
    }

    pub fn set_cron_trigger(
        &self,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
        cron_expression: &str,
    ) -> MutationRequest {
        let mutation_code = r#"
            mutation($datasetId: ID!, $flowType: String!, $cronExpression: String!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            triggers {
                                setTrigger (
                                    datasetFlowType: $flowType,
                                    triggerRuleInput: {
                                        schedule: {
                                            cron5ComponentExpression: $cronExpression
                                        }
                                    }
                                    triggerStopPolicyInput: {
                                        never: { dummy: true }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowTriggerSuccess {
                                        trigger {
                                            __typename,
                                            paused
                                            schedule {
                                                __typename
                                                ... on Cron5ComponentExpression {
                                                    cron5ComponentExpression
                                                }
                                            }
                                            reactive {
                                                __typename
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "#;

        MutationRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "flowType": flow_type,
                "cronExpression": cron_expression,
            })),
        )
    }

    pub fn set_reactive_trigger_buffering(
        &self,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
        min_records_to_await: u64,
        max_batching_interval: (u32, &str),
        recover_from_breaking_changes: bool,
    ) -> MutationRequest {
        let mutation_code = r#"
            mutation(
                $datasetId: DatasetID!,
                $flowType: String!,
                $minRecordsToAwait: Int!,
                $maxBatchingIntervalEvery: Int!,
                $maxBatchingIntervalUnit: String!,
                $forBreakingChange: String!
            ) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            triggers {
                                setTrigger (
                                    datasetFlowType: $flowType,
                                    triggerRuleInput: {
                                        reactive: {
                                            forNewData: {
                                                buffering: {
                                                    minRecordsToAwait: $minRecordsToAwait,
                                                    maxBatchingInterval: {
                                                        every: $maxBatchingIntervalEvery,
                                                        unit: $maxBatchingIntervalUnit
                                                    }
                                                }
                                            },
                                            forBreakingChange: $forBreakingChange
                                        }
                                    },
                                    triggerStopPolicyInput: {
                                        never: { dummy: true }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowTriggerSuccess {
                                        __typename,
                                        message
                                        ... on SetFlowTriggerSuccess {
                                            trigger {
                                                __typename
                                                paused
                                                schedule {
                                                    __typename
                                                }
                                                reactive {
                                                    __typename
                                                    forNewData {
                                                        __typename
                                                        ... on FlowTriggerBatchingRuleBuffering {
                                                            minRecordsToAwait
                                                            maxBatchingInterval {
                                                                every
                                                                unit
                                                            }
                                                        }
                                                    }
                                                    forBreakingChange
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "#;

        MutationRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "flowType": flow_type,
                "minRecordsToAwait": min_records_to_await,
                "maxBatchingIntervalEvery": max_batching_interval.0,
                "maxBatchingIntervalUnit": max_batching_interval.1,
                "forBreakingChange": if recover_from_breaking_changes { "RECOVER" } else { "NO_ACTION" },
            })),
        )
    }

    pub fn set_reactive_trigger_immediate(
        &self,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
        recover_from_breaking_changes: bool,
    ) -> MutationRequest {
        let mutation_code = r#"
            mutation($datasetId: DatasetID!, $flowType: String!, $forBreakingChange: String!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            triggers {
                                setTrigger (
                                    datasetFlowType: $flowType,
                                    triggerRuleInput: {
                                        reactive: {
                                            forNewData: {
                                                immediate: { dummy: false }
                                            },
                                            forBreakingChange: $forBreakingChange
                                        }
                                    },
                                    triggerStopPolicyInput: {
                                        never: { dummy: true }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowTriggerSuccess {
                                        __typename,
                                        message
                                        ... on SetFlowTriggerSuccess {
                                            trigger {
                                                __typename
                                                paused
                                                schedule {
                                                    __typename
                                                }
                                                reactive {
                                                    __typename
                                                    forNewData {
                                                        __typename
                                                    }
                                                    forBreakingChange
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "#;

        MutationRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "flowType": flow_type,
                "forBreakingChange": if recover_from_breaking_changes { "RECOVER" } else { "NO_ACTION" },
            })),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
