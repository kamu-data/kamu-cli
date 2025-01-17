// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use kamu::CreateDatasetUseCaseImpl;
use kamu_core::CreateDatasetUseCase;
use messaging_outbox::MockOutbox;
use odf::metadata::testing::MetadataFactory;

use crate::tests::use_cases::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_root_dataset() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let mut mock_outbox = MockOutbox::new();
    expect_outbox_dataset_created(&mut mock_outbox, 1);

    let harness = CreateUseCaseHarness::new(mock_outbox);

    harness
        .use_case
        .execute(
            &alias_foo,
            MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
                .build_typed(),
            Default::default(),
        )
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_harness)]
struct CreateUseCaseHarness {
    base_harness: BaseUseCaseHarness,
    use_case: Arc<dyn CreateDatasetUseCase>,
}

impl CreateUseCaseHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let base_harness =
            BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new().with_outbox(mock_outbox));

        let catalog = dill::CatalogBuilder::new_chained(base_harness.catalog())
            .add::<CreateDatasetUseCaseImpl>()
            .build();

        let use_case = catalog.get_one::<dyn CreateDatasetUseCase>().unwrap();

        Self {
            base_harness,
            use_case,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
