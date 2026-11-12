// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use database_common::{NoOpDatabasePlugin, PaginationOpts};
use dill::CatalogBuilder;
use kamu_accounts::{
    AccountConfig,
    AccountPropertyName,
    AnonymousAccountReason,
    CurrentAccountSubject,
    CurrentAccountSubjectCatalogExt as _,
    DidSecretEncryptionConfig,
    JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
    PredefinedAccountsConfig,
    Principal,
};
use kamu_accounts_inmem::{
    InMemoryAccountQuotaEventStore,
    InMemoryAccountRepository,
    InMemoryDidSecretKeyRepository,
};
use kamu_accounts_services::{
    AccountQuotaServiceImpl,
    AccountServiceImpl,
    CreateAccountUseCaseImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
    UpdateAccountUseCaseImpl,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    DeleteDatasetRebacPropertiesUseCaseImpl,
    RebacServiceImpl,
    SetDatasetRebacPropertiesUseCaseImpl,
};
use kamu_configuration_inmem::{
    InMemorySecretSetProjectionRepository,
    InMemoryVariableSetProjectionRepository,
};
use kamu_datasets::SecretsEncryptionConfig;
use kamu_resources::{
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceControllerRuntimeConfig,
    ResourceLifecycleMessage,
    ResourceOwnerRef,
    ResourceSelector,
    ResourceVisibility,
};
use kamu_resources_facade::{ListResourcesError, ResourceFacade, SearchResourcesRequest};
use kamu_resources_services::{BackgroundWorkFinalizer, ResourceControllerRuntimeImpl};
use messaging_outbox::{OutboxAgent, OutboxProvider, register_message_dispatcher};
use strum::IntoEnumIterator;
use time_source::SystemTimeSourceProvider;
use wakeup_listener::{WakeupListenerConfig, WakeupListenerMetrics};

use super::facade_harness_trait::{FacadeContractHarness, TestAccount};
use crate::helpers::{VARIABLE_SET_CANONICAL_SELECTOR, sorted_handle_names};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A predefined account with the admin property, outside [`TestAccount`] so
/// the contract suite's fixture accounts stay non-admin
const ADMIN_ACCOUNT_NAME: &str = "admin";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct LocalFacadeHarnessOpts {
    pub outbox_provider: OutboxProvider,
}

impl Default for LocalFacadeHarnessOpts {
    fn default() -> Self {
        Self {
            outbox_provider: OutboxProvider::Immediate {
                force_immediate: true,
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct LocalFacadeHarness {
    base_catalog: dill::Catalog,
    alice_account_id: odf::AccountID,
    bob_account_id: odf::AccountID,
}

impl LocalFacadeHarness {
    pub async fn new() -> Self {
        Self::new_with_opts(LocalFacadeHarnessOpts::default()).await
    }

    pub async fn new_with_opts(opts: LocalFacadeHarnessOpts) -> Self {
        let subjects: Vec<(TestAccount, CurrentAccountSubject)> = TestAccount::iter()
            .map(|a| {
                let subject = CurrentAccountSubject::new_test_with(&a);
                (a, subject)
            })
            .collect();

        let alice_account_id = subjects
            .iter()
            .find(|(a, _)| *a == TestAccount::Alice)
            .unwrap()
            .1
            .account_id()
            .clone();
        let bob_account_id = subjects
            .iter()
            .find(|(a, _)| *a == TestAccount::Bob)
            .unwrap()
            .1
            .account_id()
            .clone();

        let is_dispatching = matches!(opts.outbox_provider, OutboxProvider::Dispatching);
        let base_catalog = Self::build_base_catalog(&subjects, opts.outbox_provider).await;

        if is_dispatching {
            base_catalog
                .get_one::<dyn OutboxAgent>()
                .unwrap()
                .run_initialization()
                .await
                .unwrap();
        }

        Self {
            base_catalog,
            alice_account_id,
            bob_account_id,
        }
    }

    async fn build_base_catalog(
        subjects: &[(TestAccount, CurrentAccountSubject)],
        outbox_provider: OutboxProvider,
    ) -> dill::Catalog {
        let mut predefined_accounts_config = PredefinedAccountsConfig::new();
        for (_, subject) in subjects {
            let CurrentAccountSubject::Logged(logged) = subject else {
                unreachable!();
            };
            predefined_accounts_config
                .predefined
                .push(AccountConfig::test_config_from_subject(logged.clone()));
        }
        let CurrentAccountSubject::Logged(admin) = Self::admin_subject() else {
            unreachable!();
        };
        predefined_accounts_config.predefined.push(AccountConfig {
            properties: vec![AccountPropertyName::IsAdmin],
            ..AccountConfig::test_config_from_subject(admin)
        });

        let mut b = CatalogBuilder::new();

        // Time source
        SystemTimeSourceProvider::default().embed_into_catalog(&mut b);

        // Database noop
        NoOpDatabasePlugin::init_database_components(&mut b);

        // Outbox
        let needs_bridge = matches!(outbox_provider, OutboxProvider::Dispatching);
        outbox_provider.embed_into_catalog(&mut b);
        // The controller hint hub needs it with any outbox
        b.add::<WakeupListenerMetrics>();
        if needs_bridge {
            b.add::<kamu_messaging_outbox_inmem::InMemoryOutboxMessageBridge>();
            b.add::<kamu_wakeup_listener_inmem::InMemoryWakeupHub>();
        } else {
            // The controller runtime needs it; the dispatching outbox brings
            // its own
            b.add_value(WakeupListenerConfig::local_default());
        }

        // Resource controllers
        b.add::<ResourceControllerRuntimeImpl>()
            .add_value(ResourceControllerRuntimeConfig::default());

        // Accounts
        b.add::<InMemoryAccountRepository>()
            .add::<InMemoryAccountQuotaEventStore>()
            .add::<InMemoryDidSecretKeyRepository>()
            .add::<LoginPasswordAuthProvider>()
            .add::<PredefinedAccountsRegistrator>()
            .add::<AccountServiceImpl>()
            .add::<AccountQuotaServiceImpl>()
            .add::<CreateAccountUseCaseImpl>()
            .add::<UpdateAccountUseCaseImpl>()
            .add_value(DidSecretEncryptionConfig::sample())
            .add_value(predefined_accounts_config);

        // ReBac
        b.add::<InMemoryRebacRepository>()
            .add::<RebacServiceImpl>()
            .add::<SetDatasetRebacPropertiesUseCaseImpl>()
            .add::<DeleteDatasetRebacPropertiesUseCaseImpl>()
            .add_value(DefaultAccountProperties::default())
            .add_value(DefaultDatasetProperties::default());

        // Resources
        kamu_resources_inmem::register_dependencies(&mut b);

        // Configuration
        b.add::<InMemoryVariableSetProjectionRepository>()
            .add::<InMemorySecretSetProjectionRepository>()
            .add_value(SecretsEncryptionConfig::sample());

        kamu_resources_services::register_dependencies(&mut b);
        kamu_configuration_services::register_dependencies(&mut b);

        register_message_dispatcher::<ResourceLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
        );

        let catalog = b.build();

        // Run startup jobs to register predefined accounts
        init_on_startup::run_startup_jobs_ex(
            &catalog,
            init_on_startup::RunStartupJobsOptions {
                job_selector: Some(init_on_startup::JobSelector::AllOf(HashSet::from([
                    JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
                ]))),
            },
        )
        .await
        .unwrap();

        catalog
    }

    pub fn base_catalog(&self) -> &dill::Catalog {
        &self.base_catalog
    }

    /// See [`FacadeContractHarness::settle`]
    pub async fn settle(&self) {
        BackgroundWorkFinalizer
            .finalize(&self.base_catalog)
            .await
            .unwrap();
    }

    /// A facade acting as the admin account
    pub fn facade_for_admin(&self) -> Arc<dyn ResourceFacade> {
        let subject = Self::admin_subject();
        let catalog = dill::CatalogBuilder::new_chained(&self.base_catalog)
            .add_current_account_subject(subject)
            .build();
        Self::facade_in(&catalog)
    }

    /// A facade of background work: the caller is `account`, or anonymous
    /// when `None`, while permissions are checked for [`Principal::Node`]
    pub fn facade_for_node(&self, account: Option<TestAccount>) -> Arc<dyn ResourceFacade> {
        let subject = match account {
            Some(account) => CurrentAccountSubject::new_test_with(&account),
            None => {
                CurrentAccountSubject::anonymous(AnonymousAccountReason::NoAuthenticationProvided)
            }
        };
        let catalog = dill::CatalogBuilder::new_chained(&self.base_catalog)
            .add_value(subject)
            .add_value(Principal::Node)
            .build();
        Self::facade_in(&catalog)
    }

    /// Sorted names of the variable sets `facade` finds in `account`, or in
    /// the caller's own account when `None`
    pub async fn search_variable_set_names(
        &self,
        facade: Arc<dyn ResourceFacade>,
        account: Option<TestAccount>,
    ) -> Result<Vec<String>, ListResourcesError> {
        let response = facade
            .search_handles(SearchResourcesRequest {
                visibility: ResourceVisibility::Active,
                selectors: vec![ResourceSelector {
                    account: account.map(|account| ResourceOwnerRef {
                        id: None,
                        did: None,
                        name: Some(self.account_name(account)),
                    }),
                    ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
                }],
                pagination: PaginationOpts::from_max_results(1000),
            })
            .await?;
        Ok(sorted_handle_names(response.items))
    }

    fn admin_subject() -> CurrentAccountSubject {
        CurrentAccountSubject::new_test_with(&ADMIN_ACCOUNT_NAME)
    }

    fn facade_in(catalog: &dill::Catalog) -> Arc<dyn ResourceFacade> {
        let mut b = dill::CatalogBuilder::new_chained(catalog);
        kamu_resources_facade::register_dependencies(&mut b);
        b.build().get_one::<dyn ResourceFacade>().unwrap()
    }

    fn catalog_for_account(&self, account: TestAccount) -> dill::Catalog {
        let account_id = self.account_id(account);
        let subject = CurrentAccountSubject::logged(
            kamu_accounts::Account::seed_resource_id_from_name(account.name()),
            account_id,
            odf::AccountName::new_unchecked(account.name()),
        );

        dill::CatalogBuilder::new_chained(&self.base_catalog)
            .add_current_account_subject(subject)
            .build()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FacadeContractHarness for LocalFacadeHarness {
    fn facade_for(&self, account: TestAccount) -> Arc<dyn ResourceFacade> {
        Self::facade_in(&self.catalog_for_account(account))
    }

    fn account_id(&self, account: TestAccount) -> odf::AccountID {
        match account {
            TestAccount::Alice => self.alice_account_id.clone(),
            TestAccount::Bob => self.bob_account_id.clone(),
        }
    }

    fn account_name(&self, account: TestAccount) -> odf::AccountName {
        odf::AccountName::new_unchecked(account.name())
    }

    async fn settle(&self) {
        LocalFacadeHarness::settle(self).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
