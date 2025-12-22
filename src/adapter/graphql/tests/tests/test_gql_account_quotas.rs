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
use dill::Component;
use indoc::indoc;
use kamu_accounts::*;
use kamu_adapter_graphql::data_loader::account_entity_data_loader;
use kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl;
use messaging_outbox::{ConsumerFilter, Outbox, OutboxImmediateImpl};
use pretty_assertions::assert_eq;
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
            .add_value(AuthConfig::sample())
            .add_builder(
                OutboxImmediateImpl::builder().with_consumer_filter(ConsumerFilter::AllConsumers),
            )
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

    // Set quota for default user
    let set_res = harness
        .execute_authorized_query(async_graphql::Request::new(indoc!(
            r#"
            mutation {
              accounts {
                me {
                  quotas {
                    setAccountQuotas(quotas: { storage: { limitTotalBytes: 12345 } }) {
                      success
                    }
                  }
                }
              }
            }
            "#
        )))
        .await;

    assert!(set_res.is_ok(), "{set_res:?}");
    assert_eq!(
        set_res.data,
        value!({
            "accounts": {
                "me": {
                    "quotas": {
                        "setAccountQuotas": {
                            "success": true
                        }
                    }
                }
            }
        })
    );

    // Read quota back (limit only for now)
    let get_res = harness
        .execute_authorized_query(async_graphql::Request::new(indoc!(
            r#"
            query {
              accounts {
                me {
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
        )))
        .await;

    assert!(get_res.is_ok(), "{get_res:?}");
    assert_eq!(
        get_res.data,
        value!({
            "accounts": {
                "me": {
                    "quotas": {
                        "user": {
                            "storage": {
                                "limitTotalBytes": 12345
                            }
                        }
                    }
                }
            }
        })
    );
}
