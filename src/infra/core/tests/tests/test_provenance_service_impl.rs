// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use internal_error::InternalError;
use kamu::ProvenanceServiceImpl;
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu_accounts::Principal;
use kamu_core::{
    GetLineageError,
    LineageOptions,
    LineageVisitor,
    MockDidGenerator,
    NodeInfo,
    ProvenanceService,
};
use kamu_datasets::MockDatasetPermissionChecker;
use kamu_datasets_services::{DependencyGraphIndexer, DependencyGraphServiceImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_lineage_over_unreadable_upstream_is_access_error() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let id_foo = odf::DatasetID::new_seeded_ed25519(b"foo");
    let id_bar = odf::DatasetID::new_seeded_ed25519(b"bar");

    let harness = ProvenanceHarness::new(
        vec![id_foo.clone(), id_bar.clone()],
        MockDatasetPermissionChecker::new()
            .expect_check_read_dataset(&id_bar, 1, true)
            .expect_check_read_dataset(&id_foo, 1, false),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    harness.index_dataset_dependencies().await;

    let (result, entered) = harness.get_lineage(&alias_bar).await;

    // The readable derivative is entered before its upstream is checked
    assert_matches!(result, Err(GetLineageError::Access(_)));
    assert_eq!(entered, vec![alias_bar]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct ProvenanceHarness {
    base_use_case_harness: BaseUseCaseHarness,
    provenance_service: Arc<dyn ProvenanceService>,
    dependency_graph_indexer: Arc<DependencyGraphIndexer>,
    principal: Arc<Principal>,
}

impl ProvenanceHarness {
    /// Datasets get `dataset_ids` in the order they are created
    fn new(
        dataset_ids: Vec<odf::DatasetID>,
        mock_dataset_permission_checker: MockDatasetPermissionChecker,
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_mock_did_generator(Some(MockDidGenerator::predefined_dataset_ids(
                    dataset_ids,
                )))
                .with_maybe_permission_checker(Some(mock_dataset_permission_checker)),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        kamu_datasets_inmem::register_dependencies(&mut b);
        b.add::<ProvenanceServiceImpl>()
            .add::<DependencyGraphServiceImpl>()
            .add::<DependencyGraphIndexer>();
        database_common::NoOpDatabasePlugin::init_database_components(&mut b);
        let catalog = b.build();

        Self {
            base_use_case_harness,
            provenance_service: catalog.get_one().unwrap(),
            dependency_graph_indexer: catalog.get_one().unwrap(),
            principal: catalog.get_one().unwrap(),
        }
    }

    async fn index_dataset_dependencies(&self) {
        use init_on_startup::InitOnStartup;
        self.dependency_graph_indexer
            .run_initialization()
            .await
            .unwrap();
    }

    /// The lineage of `alias` and the datasets its visit entered, in order
    async fn get_lineage(
        &self,
        alias: &odf::DatasetAlias,
    ) -> (Result<(), GetLineageError>, Vec<odf::DatasetAlias>) {
        let mut visitor = EnteredRecordingVisitor::default();
        let result = self
            .provenance_service
            .get_dataset_lineage(
                &self.principal,
                &alias.as_local_ref(),
                &mut visitor,
                LineageOptions {},
            )
            .await;
        (result, visitor.entered)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Records the datasets a lineage visit enters, and enters every one
#[derive(Default)]
struct EnteredRecordingVisitor {
    entered: Vec<odf::DatasetAlias>,
}

impl LineageVisitor for EnteredRecordingVisitor {
    fn begin(&mut self) {}

    fn enter(&mut self, dataset: &NodeInfo<'_>) -> bool {
        self.entered.push(dataset.alias().clone());
        true
    }

    fn exit(&mut self, _dataset: &NodeInfo<'_>) {}

    fn done(&mut self) -> Result<(), InternalError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
