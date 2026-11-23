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
use kamu::TransformPlannerImpl;
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu_adapter_flow_dataset::{TransformFlowEvaluator, TransformFlowEvaluatorImpl};
use kamu_core::TransformStatus;
use kamu_datasets::CreateDatasetResult;
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_with_changed_inputs_is_up_to_date() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let harness = TransformFlowEvaluatorHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    let baz = harness.create_root_dataset(&alias_baz).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    harness.transform_consuming(&bar, &foo).await;
    harness.change_inputs(&bar, &[&foo, &baz]).await;

    let status = harness.evaluate_transform_status(&bar).await;

    assert_matches!(status, Ok(TransformStatus::UpToDate));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_of_root_dataset_is_an_internal_error() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let harness = TransformFlowEvaluatorHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;

    let status = harness.evaluate_transform_status(&foo).await;

    assert_matches!(status, Err(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct TransformFlowEvaluatorHarness {
    base_use_case_harness: BaseUseCaseHarness,
    evaluator: Arc<dyn TransformFlowEvaluator>,
}

impl TransformFlowEvaluatorHarness {
    fn new() -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new());

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<TransformFlowEvaluatorImpl>()
            .add::<TransformPlannerImpl>();

        let catalog = b.build();

        Self {
            base_use_case_harness,
            evaluator: catalog.get_one().unwrap(),
        }
    }

    async fn evaluate_transform_status(
        &self,
        created: &CreateDatasetResult,
    ) -> Result<TransformStatus, InternalError> {
        self.evaluator
            .evaluate_transform_status(&created.dataset_handle.id)
            .await
    }

    /// Records a transform of `derived` that consumed `input`
    async fn transform_consuming(
        &self,
        derived: &CreateDatasetResult,
        input: &CreateDatasetResult,
    ) {
        self.append_event(
            derived,
            MetadataFactory::execute_transform()
                .empty_query_inputs_from_particular_ids([input.dataset_handle.id.clone()])
                .some_new_watermark()
                .build()
                .into(),
        )
        .await;
    }

    /// Declares `inputs` as the inputs of the transform of `derived`
    async fn change_inputs(&self, derived: &CreateDatasetResult, inputs: &[&CreateDatasetResult]) {
        self.append_event(
            derived,
            MetadataFactory::set_transform()
                .inputs_from_refs_and_aliases(inputs.iter().map(|input| {
                    (
                        input.dataset_handle.id.clone(),
                        input.dataset_handle.alias.to_string(),
                    )
                }))
                .build()
                .into(),
        )
        .await;
    }

    async fn append_event(&self, created: &CreateDatasetResult, event: odf::MetadataEvent) {
        let chain = created.dataset.as_metadata_chain();
        let head = self.get_head(created).await;
        let head_block = chain.get_block(&head).await.unwrap();
        chain
            .append(
                MetadataFactory::metadata_block(event)
                    .prev(&head, head_block.sequence_number)
                    .system_time(self.system_time_source().now())
                    .build(),
                odf::dataset::AppendOpts::default(),
            )
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
