// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu::testing::{BaseRepoHarness, MockDatasetActionAuthorizer};
use kamu_core::auth::DatasetActionAuthorizer;
use kamu_core::{MockDidGenerator, TenancyConfig};
use messaging_outbox::{MockOutbox, Outbox};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct BaseUseCaseHarnessOptions {
    tenancy_config: TenancyConfig,
    mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
    mock_outbox: MockOutbox,
    maybe_mock_did_generator: Option<MockDidGenerator>,
}

impl BaseUseCaseHarnessOptions {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_authorizer(
        mut self,
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
    ) -> Self {
        self.mock_dataset_action_authorizer = mock_dataset_action_authorizer;
        self
    }

    pub(crate) fn with_outbox(mut self, mock_outbox: MockOutbox) -> Self {
        self.mock_outbox = mock_outbox;
        self
    }

    pub(crate) fn with_maybe_mock_did_generator(
        mut self,
        mock_did_generator: Option<MockDidGenerator>,
    ) -> Self {
        self.maybe_mock_did_generator = mock_did_generator;
        self
    }
}

impl Default for BaseUseCaseHarnessOptions {
    fn default() -> Self {
        Self {
            tenancy_config: TenancyConfig::SingleTenant,
            mock_dataset_action_authorizer: MockDatasetActionAuthorizer::new(),
            mock_outbox: MockOutbox::new(),
            maybe_mock_did_generator: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
pub(crate) struct BaseUseCaseHarness {
    base_repo_harness: BaseRepoHarness,
    catalog: Catalog,
}

impl BaseUseCaseHarness {
    pub(crate) fn new(options: BaseUseCaseHarnessOptions) -> Self {
        let base_repo_harness =
            BaseRepoHarness::new(options.tenancy_config, options.maybe_mock_did_generator);

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add_value(options.mock_dataset_action_authorizer)
            .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
            .add_value(options.mock_outbox)
            .bind::<dyn Outbox, MockOutbox>()
            .build();

        Self {
            base_repo_harness,
            catalog,
        }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
