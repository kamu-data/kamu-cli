// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu_core::auth::DatasetActionAuthorizer;
use kamu_core::{MockDidGenerator, TenancyConfig};
use messaging_outbox::{MockOutbox, Outbox};

use crate::testing::{BaseRepoHarness, MockDatasetActionAuthorizer};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct BaseUseCaseHarnessOptions {
    tenancy_config: TenancyConfig,
    maybe_mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
    mock_outbox: MockOutbox,
    maybe_mock_did_generator: Option<MockDidGenerator>,
    current_account_subject: CurrentAccountSubject,
}

impl BaseUseCaseHarnessOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_tenancy_config(mut self, tenancy_config: TenancyConfig) -> Self {
        self.tenancy_config = tenancy_config;
        self
    }

    pub fn with_maybe_authorizer(
        mut self,
        mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
    ) -> Self {
        self.maybe_mock_dataset_action_authorizer = mock_dataset_action_authorizer;
        self
    }

    pub fn with_outbox(mut self, mock_outbox: MockOutbox) -> Self {
        self.mock_outbox = mock_outbox;
        self
    }

    pub fn with_maybe_mock_did_generator(
        mut self,
        mock_did_generator: Option<MockDidGenerator>,
    ) -> Self {
        self.maybe_mock_did_generator = mock_did_generator;
        self
    }

    pub fn with_current_account_subject(
        mut self,
        current_account_subject: CurrentAccountSubject,
    ) -> Self {
        self.current_account_subject = current_account_subject;
        self
    }
}

impl Default for BaseUseCaseHarnessOptions {
    fn default() -> Self {
        Self {
            tenancy_config: TenancyConfig::SingleTenant,
            maybe_mock_dataset_action_authorizer: None,
            mock_outbox: MockOutbox::new(),
            maybe_mock_did_generator: None,
            current_account_subject: CurrentAccountSubject::new_test(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
pub struct BaseUseCaseHarness {
    base_repo_harness: BaseRepoHarness,
    catalog: Catalog,
}

impl BaseUseCaseHarness {
    pub fn new(options: BaseUseCaseHarnessOptions) -> Self {
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(options.tenancy_config)
            .maybe_mock_did_generator(options.maybe_mock_did_generator)
            .current_account_subject(options.current_account_subject)
            .build();

        let catalog = {
            let mut b = dill::CatalogBuilder::new_chained(base_repo_harness.catalog());
            b.add_value(options.mock_outbox)
                .bind::<dyn Outbox, MockOutbox>();

            if let Some(mock_dataset_action_authorizer) =
                options.maybe_mock_dataset_action_authorizer
            {
                b.add_value(mock_dataset_action_authorizer)
                    .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>();
            }

            b.build()
        };

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
