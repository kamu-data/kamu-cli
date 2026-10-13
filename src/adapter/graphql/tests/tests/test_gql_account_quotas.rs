// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use bon::bon;
use indoc::indoc;
use kamu_accounts::*;
use kamu_adapter_graphql::data_loader::account_entity_data_loader;
use kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl;
use kamu_datasets_inmem::InMemoryDatasetStatisticsRepository;
use kamu_datasets_services::{
    AccountQuotaCheckerStorageImpl,
    DatasetStatisticsServiceImpl,
    QuotaDefaultsConfig,
};
use messaging_outbox::{ConsumerFilter, Outbox, OutboxImmediateImpl};
use pretty_assertions::assert_eq;
use serde_json::json;
use time_source::SystemTimeSourceDefault;

use crate::utils::{PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct GraphQLAccountQuotasHarness {
    schema: kamu_adapter_graphql::Schema,
    catalog_authorized: dill::Catalog,
}

#[bon]
impl GraphQLAccountQuotasHarness {
    #[builder]
    pub async fn new(
        #[builder(default = PredefinedAccountOpts::default())]
        predefined_account_opts: PredefinedAccountOpts,
    ) -> Self {
        let mut b = dill::CatalogBuilder::new();
        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        let base_catalog = b.build();

        let catalog = dill::CatalogBuilder::new_chained(&base_catalog)
            .add_value(kamu_core::TenancyConfig::MultiTenant)
            .add::<kamu_accounts_inmem::InMemoryAccessTokenRepository>()
            .add::<kamu_accounts_inmem::InMemoryDidSecretKeyRepository>()
            .add::<kamu_accounts_inmem::InMemoryOAuthDeviceCodeRepository>()
            .add::<kamu_accounts_inmem::InMemoryAccountQuotaEventStore>()
            .add::<kamu_accounts_services::AccountQuotaServiceImpl>()
            .add::<AccountQuotaCheckerStorageImpl>()
            .add::<InMemoryDatasetStatisticsRepository>()
            .add::<DatasetStatisticsServiceImpl>()
            .add::<kamu_accounts_services::AccessTokenServiceImpl>()
            .add::<kamu_accounts_services::AuthenticationServiceImpl>()
            .add::<kamu_accounts_services::CreateAccountUseCaseImpl>()
            .add::<kamu_accounts_services::ModifyAccountPasswordUseCaseImpl>()
            .add::<kamu_accounts_services::DeleteAccountUseCaseImpl>()
            .add::<kamu_accounts_services::UpdateAccountUseCaseImpl>()
            .add::<kamu_accounts_services::OAuthDeviceCodeGeneratorDefault>()
            .add::<kamu_accounts_services::OAuthDeviceCodeServiceImpl>()
            .add::<kamu_accounts_services::utils::AccountAuthorizationHelperImpl>()
            .add::<RebacDatasetRegistryFacadeImpl>()
            .add::<SystemTimeSourceDefault>()
            .add_value(JwtAuthenticationConfig::default())
            .add_value(QuotaDefaultsConfig::default())
            .add_value(AuthConfig::sample())
            .add_builder(OutboxImmediateImpl::builder(ConsumerFilter::AllConsumers))
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .build();

        let (_, catalog_authorized) =
            authentication_catalogs(&catalog, predefined_account_opts).await;

        Self {
            schema: kamu_adapter_graphql::schema_quiet(),
            catalog_authorized,
        }
    }

    pub async fn execute_authorized_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        self.schema
            .execute(
                query
                    .into()
                    .data(account_entity_data_loader(&self.catalog_authorized))
                    .data(self.catalog_authorized.clone()),
            )
            .await
    }

    async fn create_account(&self, account_name: &str) {
        let res = self
            .execute_authorized_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    mutation ($accountName: AccountName!) {
                      accounts {
                        createAccount(accountName: $accountName) {
                          __typename
                        }
                      }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_value(value!({
                    "accountName": account_name,
                }))),
            )
            .await;

        assert_eq!(
            res.data,
            value!({
                "accounts": {
                    "createAccount": {
                        "__typename": "CreateAccountSuccess"
                    }
                }
            }),
            "{res:?}"
        );
    }

    async fn set_storage_quota(&self, account_name: &str, limit_bytes: u64) {
        let res = self
            .execute_authorized_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    mutation ($accountName: AccountName!, $limitBytes: Int!) {
                      accounts {
                        byName(accountName: $accountName) {
                          quotas {
                            setAccountQuotas(quotas: { storage: { limitTotalBytes: $limitBytes } }) {
                              isSuccess
                            }
                          }
                        }
                      }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_value(value!({
                    "accountName": account_name,
                    "limitBytes": limit_bytes,
                }))),
            )
            .await;

        assert_eq!(
            res.data,
            value!({
                "accounts": {
                    "byName": {
                        "quotas": {
                            "setAccountQuotas": {
                                "isSuccess": true
                            }
                        }
                    }
                }
            }),
            "{res:?}"
        );
    }

    /// Returns `limitTotalBytes` of the account, `null` meaning unlimited.
    async fn get_storage_limit(&self, account_name: &str) -> serde_json::Value {
        let res = self
            .execute_authorized_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    query ($accountName: AccountName!) {
                      accounts {
                        byName(name: $accountName) {
                          quotas {
                            user {
                              storage {
                                limitTotalBytes
                              }
                            }
                          }
                        }
                      }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_value(value!({
                    "accountName": account_name,
                }))),
            )
            .await;

        assert!(res.is_ok(), "{res:?}");
        res.data.into_json().unwrap()["accounts"]["byName"]["quotas"]["user"]["storage"]
            ["limitTotalBytes"]
            .clone()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_set_and_get_account_quota() {
    let harness = GraphQLAccountQuotasHarness::builder()
        .predefined_account_opts(PredefinedAccountOpts {
            is_admin: true,
            ..Default::default()
        })
        .build()
        .await;

    harness.create_account("foo").await;
    harness.set_storage_quota("foo", 12345).await;

    assert_eq!(harness.get_storage_limit("foo").await, json!(12345));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_account_quota_default() {
    let harness = GraphQLAccountQuotasHarness::builder().build().await;

    assert_eq!(
        harness.get_storage_limit(DEFAULT_ACCOUNT_NAME_STR).await,
        json!(1_000_000_000u64)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_account_quota_is_unlimited() {
    let harness = GraphQLAccountQuotasHarness::builder()
        .predefined_account_opts(PredefinedAccountOpts {
            is_admin: true,
            ..Default::default()
        })
        .build()
        .await;

    assert_eq!(
        harness.get_storage_limit(DEFAULT_ACCOUNT_NAME_STR).await,
        json!(null)
    );

    // A quota stored for an admin account does not limit it either
    harness
        .set_storage_quota(DEFAULT_ACCOUNT_NAME_STR, 12345)
        .await;

    assert_eq!(
        harness.get_storage_limit(DEFAULT_ACCOUNT_NAME_STR).await,
        json!(null)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
