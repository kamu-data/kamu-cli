// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{TimeZone, Utc};
use internal_error::InternalError;
use kamu::testing::BaseRepoHarness;
use kamu::*;
use kamu_core::*;
use kamu_datasets::{
    CreateDatasetResult,
    DatasetEnvVar,
    DatasetEnvVarResolver,
    DatasetEnvVarValue,
    GetDatasetEnvVarError,
    ResolvedDataset,
    SecretsEncryptionConfig,
};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_plan_ingest_at_current_head() {
    let harness = PollingIngestPlannerHarness::new();
    let foo = harness
        .create_root_dataset(&BaseRepoHarness::alias("foo"))
        .await;

    let plan = harness.plan(&foo).await.unwrap();

    assert_eq!(plan.expected_head(), &foo.head);
    // The snapshot's last event is the polling source
    assert_eq!(plan.source_block(), &foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_plan_ingest_without_polling_source() {
    for with_push_source in [false, true] {
        let harness = PollingIngestPlannerHarness::new();
        let alias_foo = BaseRepoHarness::alias("foo");
        let foo = if with_push_source {
            harness
                .create_root_dataset_with_push_source(&alias_foo)
                .await
        } else {
            harness.create_root_dataset_without_source(&alias_foo).await
        };

        let res = harness.plan(&foo).await;

        assert_matches!(
            res,
            Err(PollingIngestPlanError::SourceNotDefined(_)),
            "with push source: {with_push_source}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rebuild_ingest_at_expected_head() {
    let harness = PollingIngestPlannerHarness::new();
    let foo = harness
        .create_root_dataset(&BaseRepoHarness::alias("foo"))
        .await;
    harness
        .append_schema_and_watermark(&foo, Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap())
        .await;
    let stored = harness.payload(&foo).await;

    let rebuilt = harness.rebuild(&foo, &stored).await.unwrap();

    assert_eq!(harness.payload_of(&foo, &rebuilt), stored);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rebuild_ingest_after_head_moved() {
    let harness = PollingIngestPlannerHarness::new();
    let foo = harness
        .create_root_dataset(&BaseRepoHarness::alias("foo"))
        .await;
    let stored = harness.payload(&foo).await;
    let new_head = harness.move_head_concurrently(&foo).await;

    let res = harness.rebuild(&foo, &stored).await;

    assert_matches!(
        res,
        Err(PollingIngestRebuildError::HeadMoved(e))
            if e.expected_head == foo.head && e.actual_head == new_head
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rebuild_ingest_that_differs_from_stored_plan() {
    let harness = PollingIngestPlannerHarness::new();
    let foo = harness
        .create_root_dataset(&BaseRepoHarness::alias("foo"))
        .await;
    let stored = PollingIngestPlanPayload {
        prev_offset: Some(10),
        ..harness.payload(&foo).await
    };

    let res = harness.rebuild(&foo, &stored).await;

    assert_matches!(res, Err(PollingIngestRebuildError::Mismatch(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rebuild_ingest_of_plan_for_another_dataset() {
    let harness = PollingIngestPlannerHarness::new();
    let foo = harness
        .create_root_dataset(&BaseRepoHarness::alias("foo"))
        .await;
    let bar = harness
        .create_root_dataset(&BaseRepoHarness::alias("bar"))
        .await;
    let stored = harness.payload(&foo).await;

    let res = harness.rebuild(&bar, &stored).await;

    assert_matches!(res, Err(PollingIngestRebuildError::Mismatch(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rebuild_ingest_without_polling_source() {
    let harness = PollingIngestPlannerHarness::new();
    let foo = harness
        .create_root_dataset(&BaseRepoHarness::alias("foo"))
        .await;
    let bar = harness
        .create_root_dataset_without_source(&BaseRepoHarness::alias("bar"))
        .await;
    // A plan pinned to a head that has no polling source
    let stored = PollingIngestPlanPayload {
        dataset_id: bar.dataset_handle.id.clone(),
        expected_head: bar.head.clone(),
        ..harness.payload(&foo).await
    };

    let res = harness.rebuild(&bar, &stored).await;

    assert_matches!(res, Err(PollingIngestRebuildError::Mismatch(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_resolve_env_vars_when_disabled() {
    let harness = PollingIngestPlannerHarness::with_env_vars(SecretsEncryptionConfig {
        enabled: false,
        encryption_key: None,
    });
    let foo = harness
        .create_root_dataset(&BaseRepoHarness::alias("foo"))
        .await;

    let env_vars = harness.resolve_env_vars(&foo).await;

    assert!(env_vars.as_map().is_empty());
    assert_eq!(harness.env_var_resolver_calls(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_resolve_env_vars_when_enabled() {
    let harness = PollingIngestPlannerHarness::with_env_vars(SecretsEncryptionConfig::sample());
    let foo = harness
        .create_root_dataset(&BaseRepoHarness::alias("foo"))
        .await;

    let env_vars = harness.resolve_env_vars(&foo).await;

    assert_eq!(
        env_vars.as_map().keys().collect::<Vec<_>>(),
        vec![FakeDatasetEnvVarResolver::KEY]
    );
    assert_eq!(harness.env_var_resolver_calls(), 1);
    // Values never show in logs
    assert_eq!(format!("{env_vars:?}"), r#"IngestEnvVars(["API_KEY"])"#);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct PollingIngestPlannerHarness {
    base_repo_harness: BaseRepoHarness,
    planner: Arc<dyn PollingIngestPlanner>,
    env_vars_resolver: Arc<IngestEnvVarsResolver>,
    env_var_resolver_calls: Arc<AtomicUsize>,
}

impl PollingIngestPlannerHarness {
    fn new() -> Self {
        Self::build(None)
    }

    /// Wires dataset environment variables, with a resolver that knows one
    /// variable for every dataset
    fn with_env_vars(config: SecretsEncryptionConfig) -> Self {
        Self::build(Some(config))
    }

    fn build(secrets_encryption_config: Option<SecretsEncryptionConfig>) -> Self {
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();
        let env_var_resolver_calls = Arc::new(AtomicUsize::new(0));

        let mut b = dill::CatalogBuilder::new_chained(base_repo_harness.catalog());
        b.add::<PollingIngestPlannerImpl>()
            .add::<IngestEnvVarsResolver>();
        if let Some(config) = secrets_encryption_config {
            b.add_value(config)
                .add_value(FakeDatasetEnvVarResolver {
                    calls: env_var_resolver_calls.clone(),
                })
                .bind::<dyn DatasetEnvVarResolver, FakeDatasetEnvVarResolver>();
        }
        let catalog = b.build();

        Self {
            base_repo_harness,
            planner: catalog.get_one().unwrap(),
            env_vars_resolver: catalog.get_one().unwrap(),
            env_var_resolver_calls,
        }
    }

    async fn plan(
        &self,
        created: &CreateDatasetResult,
    ) -> Result<PollingIngestPlan, PollingIngestPlanError> {
        self.planner
            .plan_ingest(ResolvedDataset::from_created(created))
            .await
    }

    async fn rebuild(
        &self,
        created: &CreateDatasetResult,
        stored: &PollingIngestPlanPayload,
    ) -> Result<PollingIngestPlan, PollingIngestRebuildError> {
        self.planner
            .rebuild_ingest(ResolvedDataset::from_created(created), stored)
            .await
    }

    /// The stored form of a plan made now
    async fn payload(&self, created: &CreateDatasetResult) -> PollingIngestPlanPayload {
        let plan = self.plan(created).await.unwrap();
        self.payload_of(created, &plan)
    }

    fn payload_of(
        &self,
        created: &CreateDatasetResult,
        plan: &PollingIngestPlan,
    ) -> PollingIngestPlanPayload {
        PollingIngestPlanPayload::from_plan(
            created.dataset_handle.id.clone(),
            plan,
            &PollingIngestOptions::default(),
        )
        .unwrap()
    }

    async fn resolve_env_vars(&self, created: &CreateDatasetResult) -> IngestEnvVars {
        self.env_vars_resolver
            .resolve(&created.dataset_handle.id)
            .await
            .unwrap()
    }

    fn env_var_resolver_calls(&self) -> usize {
        self.env_var_resolver_calls.load(Ordering::SeqCst)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FakeDatasetEnvVarResolver {
    calls: Arc<AtomicUsize>,
}

impl FakeDatasetEnvVarResolver {
    const KEY: &str = "API_KEY";
}

#[async_trait::async_trait]
impl DatasetEnvVarResolver for FakeDatasetEnvVarResolver {
    async fn resolve_effective_env_vars(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashMap<String, DatasetEnvVar>, InternalError> {
        self.calls.fetch_add(1, Ordering::SeqCst);

        let env_var = DatasetEnvVar::new(
            Self::KEY,
            Utc::now(),
            &DatasetEnvVarValue::Regular("secret-value".to_string()),
            dataset_id,
            "",
        )
        .unwrap();
        Ok(HashMap::from([(Self::KEY.to_string(), env_var)]))
    }

    async fn get_env_var_by_entry_key(
        &self,
        _dataset_id: &odf::DatasetID,
        _entry_key: &str,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        unreachable!("Ingest planning resolves the whole map")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
