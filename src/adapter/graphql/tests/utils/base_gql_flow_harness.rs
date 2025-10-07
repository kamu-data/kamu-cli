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
use kamu_flow_system_inmem::domain::{RetryBackoffType, RetryPolicy};
use kamu_flow_system_inmem::*;
use odf::metadata::testing::MetadataFactory;

use crate::utils::{
    BaseGQLDatasetHarness,
    GraphQLQueryRequest,
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

    #[allow(clippy::needless_pass_by_value)]
    pub fn set_time_delta_trigger(
        &self,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
        time_delta: (u64, &str),
        stop_policy: Option<async_graphql::Value>,
    ) -> GraphQLQueryRequest {
        let stop_policy = stop_policy.unwrap_or_else(|| {
            value!(
                {
                    "afterConsecutiveFailures": {
                        "maxFailures": 1_i32
                    }
                }
            )
        });

        let mutation_code = r#"
            mutation(
                $datasetId: DatasetID!,
                $flowType: String!,
                $timeDelta: TimeDeltaInput!,
                $stopPolicy: FlowTriggerStopPolicyInput!
            ) {
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
                                    triggerStopPolicyInput: $stopPolicy
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
                                            stopPolicy {
                                                __typename
                                                ... on FlowTriggerStopPolicyAfterConsecutiveFailures {
                                                    maxFailures
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

        let mut vars = value!({
            "datasetId": dataset_id.to_string(),
            "flowType": flow_type.to_string(),
            "timeDelta": {
                "every": i32::try_from(time_delta.0).unwrap(),
                "unit": time_delta.1,
            },
        });

        use async_graphql::*;
        if let Value::Object(ref mut map) = vars {
            map.insert(Name::new("stopPolicy"), stop_policy);
        }

        GraphQLQueryRequest::new(mutation_code, Variables::from_value(vars))
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn set_cron_trigger(
        &self,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
        cron_expression: &str,
        stop_policy: Option<async_graphql::Value>,
    ) -> GraphQLQueryRequest {
        let stop_policy = stop_policy.unwrap_or_else(|| {
            value!(
                {
                    "afterConsecutiveFailures": {
                        "maxFailures": 1_i32
                    }
                }
            )
        });

        let mutation_code = r#"
            mutation(
                $datasetId: DatasetID!,
                $flowType: String!,
                $cronExpression: String!,
                $stopPolicy: FlowTriggerStopPolicyInput!
            ) {
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
                                    triggerStopPolicyInput: $stopPolicy
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
                                            stopPolicy {
                                                __typename
                                                ... on FlowTriggerStopPolicyAfterConsecutiveFailures {
                                                    maxFailures
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

        let mut vars = value!({
            "datasetId": dataset_id.to_string(),
            "flowType": flow_type.to_string(),
            "cronExpression": cron_expression,
        });

        use async_graphql::*;
        if let Value::Object(ref mut map) = vars {
            map.insert(Name::new("stopPolicy"), stop_policy);
        }

        GraphQLQueryRequest::new(mutation_code, Variables::from_value(vars))
    }

    pub fn set_reactive_trigger_buffering(
        &self,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
        min_records_to_await: u64,
        max_batching_interval: (u32, &str),
        recover_from_breaking_changes: bool,
    ) -> GraphQLQueryRequest {
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

        GraphQLQueryRequest::new(
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
    ) -> GraphQLQueryRequest {
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

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "flowType": flow_type,
                "forBreakingChange": if recover_from_breaking_changes { "RECOVER" } else { "NO_ACTION" },
            })),
        )
    }

    pub fn pause_flow_trigger(
        &self,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
    ) -> GraphQLQueryRequest {
        let mutation_code = r#"
            mutation($datasetId: DatasetID!, $flowType: String!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            triggers {
                                pauseFlow (
                                    datasetFlowType: $flowType
                                )
                            }
                        }
                    }
                }
            }
            "#;

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "flowType": flow_type,
            })),
        )
    }

    pub fn resume_flow_trigger(
        &self,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
    ) -> GraphQLQueryRequest {
        let mutation_code = r#"
            mutation($datasetId: DatasetID!, $flowType: String!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            triggers {
                                resumeFlow (
                                    datasetFlowType: $flowType
                                )
                            }
                        }
                    }
                }
            }
            "#;

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "flowType": flow_type,
            })),
        )
    }

    pub fn pause_all_flow_triggers(&self, dataset_id: &odf::DatasetID) -> GraphQLQueryRequest {
        let mutation_code = r#"
            mutation($datasetId: DatasetID!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            triggers {
                                pauseFlows
                            }
                        }
                    }
                }
            }
            "#;

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
            })),
        )
    }

    pub fn resume_all_flow_triggers(&self, dataset_id: &odf::DatasetID) -> GraphQLQueryRequest {
        let mutation_code = r#"
            mutation($datasetId: DatasetID!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            triggers {
                                resumeFlows
                            }
                        }
                    }
                }
            }
            "#;

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
            })),
        )
    }

    pub fn set_ingest_config(
        &self,
        dataset_id: &odf::DatasetID,
        fetch_uncacheable: bool,
    ) -> GraphQLQueryRequest {
        let mutation_code = r#"
            mutation($datasetId: DatasetID!, $fetchUncacheable: Boolean!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            configs {
                                setIngestConfig (
                                    ingestConfigInput : {
                                        fetchUncacheable: $fetchUncacheable,
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowConfigSuccess {
                                        config {
                                            __typename
                                            rule {
                                                __typename
                                                ... on FlowConfigRuleIngest {
                                                    fetchUncacheable
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

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "fetchUncacheable": fetch_uncacheable,
            })),
        )
    }

    pub fn set_compaction_config(
        &self,
        dataset_id: &odf::DatasetID,
        max_slice_size: u64,
        max_slice_records: u64,
    ) -> GraphQLQueryRequest {
        let mutation_code = r#"
            mutation($datasetId: DatasetID!, $maxSliceSize: Int!, $maxSliceRecords: Int!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            configs {
                                setCompactionConfig (
                                    compactionConfigInput: {
                                        maxSliceSize: $maxSliceSize,
                                        maxSliceRecords: $maxSliceRecords,
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowConfigSuccess {
                                        __typename,
                                        message
                                        ... on SetFlowConfigSuccess {
                                            config {
                                                __typename
                                                rule {
                                                    __typename
                                                    ... on FlowConfigRuleCompaction {
                                                        maxSliceSize
                                                        maxSliceRecords
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
            }
            "#;

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "maxSliceSize": max_slice_size,
                "maxSliceRecords": max_slice_records,
            })),
        )
    }

    pub fn set_ingest_config_with_retries(
        &self,
        dataset_id: &odf::DatasetID,
        fetch_uncacheable: bool,
        retry_policy: &RetryPolicy,
    ) -> GraphQLQueryRequest {
        let mutation_code = r#"
            mutation(
                $datasetId: DatasetID!,
                $fetchUncacheable: Boolean!,
                $retryMaxAttempts: Int!,
                $retryMinDelayMinutes: Int!,
                $retryBackoffType: String!
            ) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            configs {
                                setIngestConfig (
                                    ingestConfigInput : {
                                        fetchUncacheable: $fetchUncacheable,
                                    },
                                    retryPolicyInput: {
                                        maxAttempts: $retryMaxAttempts,
                                        minDelay: {
                                            every: $retryMinDelayMinutes,
                                            unit: "MINUTES"
                                        },
                                        backoffType: $retryBackoffType
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowConfigSuccess {
                                        config {
                                            __typename
                                            rule {
                                                __typename
                                                ... on FlowConfigRuleIngest {
                                                    fetchUncacheable
                                                }
                                            }
                                            retryPolicy {
                                                __typename
                                                maxAttempts
                                                minDelay {
                                                    every
                                                    unit
                                                }
                                                backoffType
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

        let retry_backoff_str = match retry_policy.backoff_type {
            RetryBackoffType::Fixed => "FIXED",
            RetryBackoffType::Linear => "LINEAR",
            RetryBackoffType::Exponential => "EXPONENTIAL",
            RetryBackoffType::ExponentialWithJitter => "EXPONENTIAL_WITH_JITTER",
        };

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "fetchUncacheable": fetch_uncacheable,
                "retryMaxAttempts": retry_policy.max_attempts,
                "retryMinDelayMinutes": retry_policy.min_delay_seconds / 60,
                "retryBackoffType": retry_backoff_str,
            })),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
