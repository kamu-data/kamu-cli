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

use dill::Catalog;
use kamu::testing::MetadataFactory;
use kamu::CreateDatasetUseCaseImpl;
use kamu_core::{CreateDatasetUseCase, GetDatasetError, TenancyConfig};
use messaging_outbox::{MockOutbox, Outbox};
use opendatafabric::{DatasetAlias, DatasetKind, DatasetName};

use crate::tests::use_cases::*;
use crate::BaseRepoHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_root_dataset() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let mut mock_outbox = MockOutbox::new();
    expect_outbox_dataset_created(&mut mock_outbox, 1);

    let harness = CreateUseCaseHarness::new(mock_outbox);

    harness
        .use_case
        .execute(
            &alias_foo,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
            Default::default(),
        )
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CreateUseCaseHarness {
    base_repo_harness: BaseRepoHarness,
    _catalog: Catalog,
    use_case: Arc<dyn CreateDatasetUseCase>,
}

impl CreateUseCaseHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let base_repo_harness = BaseRepoHarness::new(TenancyConfig::SingleTenant);

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<CreateDatasetUseCaseImpl>()
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>()
            .build();

        let use_case = catalog.get_one::<dyn CreateDatasetUseCase>().unwrap();

        Self {
            base_repo_harness,
            _catalog: catalog,
            use_case,
        }
    }

    #[inline]
    async fn check_dataset_exists(&self, alias: &DatasetAlias) -> Result<(), GetDatasetError> {
        self.base_repo_harness.check_dataset_exists(alias).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
