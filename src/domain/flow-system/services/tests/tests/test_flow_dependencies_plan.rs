// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{DEFAULT_MAX_SLICE_RECORDS, DEFAULT_MAX_SLICE_SIZE};
use kamu_flow_system::{
    CompactingRule,
    DatasetFlowType,
    FlowConfigSnapshot,
    FlowKey,
    FlowKeyDataset,
    FlowResult,
    FlowResultDatasetCompact,
};
use kamu_flow_system_services::{DownstreamDependencyFlowPlan, FlowTriggerContext};
use opendatafabric::*;

use super::{FlowHarness, FlowHarnessOverrides};

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_make_downstream_dependecies_flow_plans() {
    let foo_account_name = AccountName::new_unchecked("foo");
    let bar_account_name = AccountName::new_unchecked("bar");
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        is_multi_tenant: true,
        ..Default::default()
    });
    harness.eager_dependencies_graph_init().await;
    let foo_root_dataset_name = DatasetName::new_unchecked("foo");
    let derived_foo_bar_dataset_name = DatasetName::new_unchecked("foo.bar");
    let derived_foo_baz_dataset_name = DatasetName::new_unchecked("foo.baz");

    let root_created_dataset = harness
        .create_root_dataset(DatasetAlias {
            account_name: Some(foo_account_name.clone()),
            dataset_name: foo_root_dataset_name.clone(),
        })
        .await;

    let derived_foo_bar_created_dataset = harness
        .create_derived_dataset(
            DatasetAlias {
                account_name: Some(foo_account_name.clone()),
                dataset_name: derived_foo_bar_dataset_name.clone(),
            },
            vec![root_created_dataset.clone()],
        )
        .await;

    let derived_foo_baz_created_dataset = harness
        .create_derived_dataset(
            DatasetAlias {
                account_name: Some(foo_account_name.clone()),
                dataset_name: derived_foo_baz_dataset_name.clone(),
            },
            vec![root_created_dataset.clone()],
        )
        .await;

    let bar_root_dataset_name = DatasetName::new_unchecked("bar");
    let derived_bar_bar_dataset_name = DatasetName::new_unchecked("bar.bar");

    let bar_root_created_dataset = harness
        .create_root_dataset(DatasetAlias {
            account_name: Some(bar_account_name.clone()),
            dataset_name: bar_root_dataset_name.clone(),
        })
        .await;

    // Create derived dataset for bar account with root dataset from foo account
    let _bar_bar_derived_created_dataset = harness
        .create_derived_dataset(
            DatasetAlias {
                account_name: Some(bar_account_name.clone()),
                dataset_name: derived_bar_bar_dataset_name.clone(),
            },
            vec![
                root_created_dataset.clone(),
                bar_root_created_dataset.clone(),
            ],
        )
        .await;

    let filtered_foo_downstream_dataset_plans = harness
        .flow_service_impl
        .make_downstream_dependecies_flow_plans(
            &root_created_dataset,
            &FlowResult::DatasetCompact(FlowResultDatasetCompact {
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice"),
                old_num_blocks: 5,
                new_num_blocks: 1,
            }),
            Some(FlowConfigSnapshot::Compacting(
                CompactingRule::new_checked(
                    DEFAULT_MAX_SLICE_SIZE,
                    DEFAULT_MAX_SLICE_RECORDS,
                    true,
                )
                .unwrap(),
            )),
        )
        .await
        .unwrap();

    let expected_result: Vec<_> = [
        derived_foo_baz_created_dataset.clone(),
        derived_foo_bar_created_dataset.clone(),
    ]
    .iter()
    .map(|dataset_id| DownstreamDependencyFlowPlan {
        flow_key: FlowKey::Dataset(FlowKeyDataset {
            dataset_id: dataset_id.clone(),
            flow_type: DatasetFlowType::HardCompacting,
        }),
        flow_trigger_context: FlowTriggerContext::Unconditional,
        config_snapshot: Some(FlowConfigSnapshot::Compacting(
            CompactingRule::new_checked(DEFAULT_MAX_SLICE_SIZE, DEFAULT_MAX_SLICE_RECORDS, true)
                .unwrap(),
        )),
    })
    .collect();
    assert_eq!(filtered_foo_downstream_dataset_plans, expected_result);

    // Should return empty array without keep_metadata_only or batching rules
    let filtered_foo_downstream_dataset_plans = harness
        .flow_service_impl
        .make_downstream_dependecies_flow_plans(
            &root_created_dataset,
            &FlowResult::DatasetCompact(FlowResultDatasetCompact {
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice"),
                old_num_blocks: 5,
                new_num_blocks: 1,
            }),
            Some(FlowConfigSnapshot::Compacting(
                CompactingRule::new_checked(
                    DEFAULT_MAX_SLICE_SIZE,
                    DEFAULT_MAX_SLICE_RECORDS,
                    false,
                )
                .unwrap(),
            )),
        )
        .await
        .unwrap();

    assert!(filtered_foo_downstream_dataset_plans.is_empty());
}

/////////////////////////////////////////////////////////////////////////////////////////
